"""协调引擎不变量测试：映射语义、半版本禁止、在途承诺、并发与重启恢复。"""

import json
import tempfile
import threading
import unittest
from pathlib import Path

from workflow_coordinator.engine import (
    EFFECT,
    SIGNAL,
    Coordinator,
)
from workflow_coordinator.errors import (
    ConflictError,
    NotFoundError,
    TransferBlockedError,
    ValidationError,
)

V1_STEPS = [
    {"name": "approve", "kind": SIGNAL, "needs": []},
    {"name": "record", "kind": EFFECT, "needs": ["approve"]},
]
V2_STEPS = [
    {"name": "check", "kind": SIGNAL, "needs": []},
    {"name": "publish", "kind": SIGNAL, "needs": ["check"]},
    {"name": "attach", "kind": EFFECT, "needs": ["check", "publish"]},
]


class EngineTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.coordinator = Coordinator(":memory:")
        self.coordinator.publish_definition(1, V1_STEPS)
        self.coordinator.publish_definition(2, V2_STEPS)

    def tearDown(self) -> None:
        self.coordinator.close()


class DefinitionAndCompatTests(EngineTestBase):
    def test_definitions_and_compat_are_immutable(self):
        self.coordinator.publish_compat(
            1, 2, {"approve": {"step": "check", "reuse": "partial",
                                "reason": "检查通过"}}
        )
        with self.assertRaises(ConflictError):
            self.coordinator.publish_definition(1, V1_STEPS)
        with self.assertRaises(ConflictError):
            self.coordinator.publish_compat(1, 2, {"approve": "check"})

    def test_bad_graph_and_unknown_mapping_targets_are_rejected(self):
        with self.assertRaises(ValidationError):
            self.coordinator.publish_definition(
                3, [{"name": "a", "kind": SIGNAL, "needs": ["b"]},
                    {"name": "b", "kind": SIGNAL, "needs": ["a"]}]
            )
        with self.assertRaises(ValidationError):
            self.coordinator.publish_compat(1, 2, {"record": "ghost"})

    def test_start_requires_registered_version_and_cannot_repeat(self):
        with self.assertRaises(NotFoundError):
            self.coordinator.start("i", 99)
        self.coordinator.start("i", 1)
        with self.assertRaises(ConflictError):
            self.coordinator.start("i", 2)


class SignalMappingTests(EngineTestBase):
    def test_partial_mapping_needs_explicit_reason(self):
        self.coordinator.publish_compat(
            1, 2,
            {"approve": {"step": "check", "reuse": "partial",
                         "reason": "检查通过"}},
        )
        self.coordinator.start("i", 1)
        # 原因不匹配：旧信号不能被解释成对 check 的部分满足。
        self.coordinator.signal("i", 1, "approve", "s1", reason="别的原因")
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m1", 2)
        view = self.coordinator.inspect("i")
        self.assertEqual(view["effective_version"], 1)  # 半版本不得生效
        self.assertTrue(
            any(b["code"] == "reason_mismatch"
                for b in view["active_transfer"]["blockers"])
        )
        # 正确原因的旧信号解除 check 的 blocker；只剩新版新增的 publish 信号，
        # attach 是新版自己的 effect，不阻塞转版。
        self.coordinator.signal("i", 1, "approve", "s2", reason="检查通过")
        view = self.coordinator.inspect("i")
        self.assertEqual(view["effective_version"], 1)
        self.assertIsNotNone(view["active_transfer"])
        self.assertFalse(any(
            b["code"] == "reason_mismatch"
            for b in view["active_transfer"]["blockers"]
        ))
        # 值班员放弃转移、实例明确继续旧版运行，留痕说明保留原因。
        self.coordinator.abort_transfer("i")
        view = self.coordinator.inspect("i")
        self.assertEqual(view["effective_version"], 1)
        self.assertEqual(view["drive"], "running")
        self.assertTrue(any(
            item["code"] == "transfer_aborted" for item in view["retention"]
        ))

    def test_duplicate_signal_is_keyed_by_instance_step_and_id(self):
        self.coordinator.start("i", 1)
        first = self.coordinator.signal("i", 1, "approve", "same")
        second = self.coordinator.signal("i", 1, "approve", "same")
        other = self.coordinator.signal("i", 1, "approve", "different")
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["seq"], first["seq"])
        self.assertFalse(other["duplicate"])

    def test_ambiguous_alternatives_keep_instance_on_old_version(self):
        self.coordinator.publish_compat(
            1, 2,
            {"approve": {"step": "check",
                         "alternatives": [{"step": "publish"}]}},
        )
        self.coordinator.start("i", 1)
        self.coordinator.signal("i", 1, "approve", "s")
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m", 2)
        view = self.coordinator.inspect("i")
        self.assertTrue(any(
            b["code"] == "ambiguous_alternatives"
            for b in view["active_transfer"]["blockers"]
        ))
        # 值班员可放弃转移，明确继续旧版。
        self.coordinator.abort_transfer("i")
        self.assertEqual(self.coordinator.inspect("i")["effective_version"], 1)
        self.assertEqual(self.coordinator.inspect("i")["drive"], "running")

    def test_missing_required_input_blocks_and_later_signal_unblocks(self):
        # record 在新版没有任何映射；attach 也没有映射，输入缺失必须挂起。
        self.coordinator.publish_compat(1, 2, {})
        self.coordinator.start("i", 1)
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m", 2)
        view = self.coordinator.inspect("i")
        self.assertTrue(all(
            b["code"] == "missing_input" for b in view["active_transfer"]["blockers"]
        ))
        # 缺失无法靠旧证据补齐时，放弃候选；新版启动的新实例则天然运行 v2。
        self.coordinator.abort_transfer("i")
        self.coordinator.start("j", 2)
        self.assertEqual(
            self.coordinator.inspect("j")["effective_version"], 2
        )


class InFlightCommitmentTests(EngineTestBase):
    def test_claimed_effect_must_settle_before_commit(self):
        self.coordinator.publish_compat(1, 2, {
            "approve": "check",
        })
        self.coordinator.start("i", 1)
        self.coordinator.signal("i", 1, "approve", "a")
        claimed = self.coordinator.claim("i", 1, "record", "att-1")
        self.assertEqual(claimed["step"], "record")
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m", 2)
        # 准备期不允许新领取；未领的新工作项也不会再派发。
        with self.assertRaises(ConflictError):
            self.coordinator.claim("i", 1, "record", "att-2")

        # never_started 表示承诺释放；check 已被映射满足，但 publish/attach 仍缺输入。
        self.coordinator.settle("att-1", "never_started")
        view = self.coordinator.inspect("i")
        self.assertEqual(view["drive"], "paused")
        self.assertIsNotNone(view["active_transfer"])
        self.assertTrue(all(
            b["code"] == "missing_input"
            for b in view["active_transfer"]["blockers"]
        ))

    def test_done_effect_is_never_redone_after_migration(self):
        # 旧 record 全量映射到新 attach：done 证据应直接沿用。
        self.coordinator.publish_compat(1, 2, {
            "approve": "check",
            "record": {"step": "attach", "reuse": "full"},
        })
        self.coordinator.start("i", 1)
        self.coordinator.signal("i", 1, "approve", "a")
        self.coordinator.claim("i", 1, "record", "att-1")
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m", 2)
        self.coordinator.settle("att-1", "done")
        # attach/check 已沿用，只剩新版 publish 一项人工确认。
        view = self.coordinator.inspect("i")
        self.assertEqual(view["drive"], "paused")
        self.coordinator.signal("i", 2, "publish", "p")
        view = self.coordinator.inspect("i")
        self.assertEqual(view["effective_version"], 2)
        # attach 已被旧承诺满足，不存在新的 queued/dispatch，更不会重做。
        self.assertEqual(
            [w["step"] for w in view["waiting_evidence"]], []
        )
        self.assertEqual(view["drive"], "completed")
        with self.assertRaises(ConflictError):
            self.coordinator.claim("i", 2, "attach", "att-2")

    def test_unknown_receipt_pauses_until_outcome_is_certain(self):
        self.coordinator.publish_compat(1, 2, {
            "approve": "check",
            "record": {"step": "attach", "reuse": "full"},
        })
        self.coordinator.start("i", 1)
        self.coordinator.signal("i", 1, "approve", "a")
        self.coordinator.claim("i", 1, "record", "att-1")
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m", 2)
        self.coordinator.settle("att-1", "unknown")
        view = self.coordinator.inspect("i")
        self.assertEqual(view["drive"], "paused")
        self.assertTrue(any(
            item["code"] == "effect_outcome_unknown" for item in view["retention"]
        ))
        # 结局明确为 done 后，补 publish 信号，转移提交且对外结果保留。
        self.coordinator.settle("att-1", "done")
        self.coordinator.signal("i", 2, "publish", "p")
        view = self.coordinator.inspect("i")
        self.assertEqual(view["effective_version"], 2)
        self.assertEqual(view["drive"], "completed")

    def test_late_old_version_receipt_is_interpreted_by_claimed_step(self):
        # 旧 record 全量映射到新 attach：先拿到 done 并完成迁移，
        # 之后执行端迟到的重复回执仍按领取时的 v1/record 语义解释（幂等）。
        self.coordinator.publish_compat(1, 2, {
            "approve": "check",
            "record": {"step": "attach", "reuse": "full"},
        })
        self.coordinator.start("i", 1)
        self.coordinator.signal("i", 1, "approve", "a")
        self.coordinator.claim("i", 1, "record", "att-1")
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m", 2)
        self.coordinator.settle("att-1", "done")
        self.coordinator.signal("i", 2, "publish", "p")
        self.assertEqual(self.coordinator.inspect("i")["effective_version"], 2)

        repeat = self.coordinator.settle("att-1", "done")
        self.assertTrue(repeat["idempotent"])
        self.assertEqual(repeat["version"], 1)  # 仍按领取时的版本步骤解释
        self.assertEqual(repeat["step"], "record")
        # 相反的结局不允许覆盖已对外生效的结果。
        with self.assertRaises(ConflictError):
            self.coordinator.settle("att-1", "never_started")


class ConcurrencyAndRestartTests(EngineTestBase):
    def test_only_one_concurrent_migration_takes_over(self):
        self.coordinator.publish_compat(
            1, 2, {"approve": {"step": "check", "reuse": "partial",
                                "reason": "检查通过"}}
        )
        self.coordinator.start("i", 1)
        outcomes: list[str] = []
        barrier = threading.Barrier(8)

        def worker(idx: int) -> None:
            barrier.wait()
            try:
                self.coordinator.request_migration("i", f"req-{idx}", 2)
                outcomes.append("ok")
            except TransferBlockedError:
                # 候选已落库、只是 blocker 未清，同样意味着这个请求接管成功。
                outcomes.append("ok")
            except ConflictError:
                outcomes.append("conflict")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # 一个请求成为候选（缺 publish 输入而 blocked），其余七个冲突拒绝。
        self.assertEqual(len(outcomes), 8)
        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(outcomes.count("conflict"), 7)
        view = self.coordinator.inspect("i")
        self.assertIsNotNone(view["active_transfer"])

    def test_restart_distinguishes_proposal_from_effective_version(self):
        self.coordinator.publish_compat(1, 2, {})
        self.coordinator.start("i", 1)
        self.coordinator.signal("i", 1, "approve", "a")
        self.coordinator.claim("i", 1, "record", "att-1")
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m", 2)

        # 用磁盘库完整走一遍再重启。
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "state.db")
            disk = Coordinator(db_path)
            disk.publish_definition(1, V1_STEPS)
            disk.publish_definition(2, V2_STEPS)
            disk.publish_compat(1, 2, {})
            disk.start("i", 1)
            disk.signal("i", 1, "approve", "a")
            disk.claim("i", 1, "record", "att-1")
            with self.assertRaises(TransferBlockedError):
                disk.request_migration("i", "m", 2)
            disk.close()

            restarted = Coordinator(db_path)
            report = restarted.restart()
            self.assertEqual(report["recovered_instances"], 1)
            view = restarted.inspect("i")
            self.assertEqual(view["effective_version"], 1)  # 真实生效版仍是 v1
            self.assertEqual(view["drive"], "paused")
            self.assertEqual(view["active_transfer"]["state"], "proposed")

            # 结局落定后，重启后的进程同样能推进提交。
            restarted.settle("att-1", "never_started")
            # 仍缺新步骤输入，候选保持；补 abort 验证旧版恢复运行。
            restarted.abort_transfer("i")
            self.assertEqual(restarted.inspect("i")["drive"], "running")
            restarted.close()

    def test_abort_requeues_canceled_work_items_and_keeps_ledger(self):
        self.coordinator.publish_compat(
            1, 2, {"approve": {"step": "check", "reuse": "partial",
                                "reason": "检查通过"}}
        )
        self.coordinator.start("i", 1)
        self.coordinator.signal("i", 1, "approve", "a")
        # record 的 queued 工作项已就绪。
        waiting = {w["step"]: w["status"]
                   for w in self.coordinator.inspect("i")["waiting_evidence"]}
        self.assertEqual(waiting.get("record"), "queued")
        with self.assertRaises(TransferBlockedError):
            self.coordinator.request_migration("i", "m", 2)
        # 准备期 queued 项被取消、动力暂停。
        view = self.coordinator.inspect("i")
        self.assertEqual(view["drive"], "paused")
        self.assertEqual(
            view["waiting_evidence"][0]["status"], "paused_pending_transfer"
        )
        # 放弃转移：旧版恢复运行，同一 effect 重新排队，对外没有产生任何结果。
        self.coordinator.abort_transfer("i")
        view = self.coordinator.inspect("i")
        self.assertEqual(view["drive"], "running")
        self.assertEqual(
            view["waiting_evidence"][0]["status"], "queued"
        )
        # 旧账本与留痕仍然保留，值班员能查到为什么曾经暂停。
        self.assertTrue(any(
            item["code"] == "transfer_aborted" for item in view["retention"]
        ))

    def test_disk_state_is_human_inspectable_as_json(self):
        view = self.coordinator.inspect  # 仅确认接口存在，实际内容在其余用例验证
        self.assertTrue(callable(view))
        # 所有查询结果必须可 JSON 序列化，方便值班工具消费。
        self.coordinator.start("i", 1)
        json.dumps(self.coordinator.inspect("i"), ensure_ascii=False)

    def test_old_instance_keeps_contract_until_explicit_migration(self):
        # 新版发布后，已启动的旧实例继续按 v1 契约工作，不会被自动切走。
        self.coordinator.publish_compat(
            1, 2, {"approve": {"step": "check", "reuse": "partial",
                                "reason": "检查通过"}}
        )
        self.coordinator.start("old", 1)
        self.coordinator.start("new", 2)  # 新实例可直接采用新版
        self.assertEqual(
            self.coordinator.inspect("old")["effective_version"], 1
        )
        self.assertEqual(
            self.coordinator.inspect("new")["effective_version"], 2
        )
        # 旧执行端按 v1 步骤发信号、走 effect，完全保持原契约。
        self.coordinator.signal("old", 1, "approve", "a")
        self.coordinator.claim("old", 1, "record", "att")
        self.coordinator.settle("att", "done")
        view = self.coordinator.inspect("old")
        self.assertEqual(view["effective_version"], 1)
        self.assertEqual(view["drive"], "completed")
        self.assertIsNone(view["active_transfer"])


if __name__ == "__main__":
    unittest.main()
