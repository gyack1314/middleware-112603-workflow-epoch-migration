"""演练 JSONL 的端到端重放测试，对齐仓库自带的两个场景。"""

import unittest
from pathlib import Path

from workflow_coordinator.engine import Coordinator
from workflow_coordinator.replay import MESSAGE_TYPES, replay_file

EXAMPLES = Path(__file__).with_name("examples")


class ReplayScenarioTests(unittest.TestCase):
    def test_split_approval_single_old_signal_cannot_finish_two_steps(self):
        coordinator = Coordinator(":memory:")
        results = replay_file(coordinator, EXAMPLES / "split-approval.jsonl")
        # 新版信号在候选期间预入账本，所有消息都被接受；迁移一度被 blocker 挂起。
        self.assertTrue(all(r["accepted"] for r in results))
        self.assertTrue(any(
            r["kind"] == "migrate" and r["blocked"] for r in results
        ))

        view = next(
            r["record"] for r in results if r["kind"] == "inspect"
        )
        # inspect 发生在新版 publish 信号之前：生效版仍是 v1，候选转移挂起。
        self.assertEqual(view["effective_version"], 1)
        self.assertEqual(view["startup_version"], 1)
        self.assertIsNotNone(view["active_transfer"])
        self.assertEqual(
            [b["step"] for b in view["active_transfer"]["blockers"]], ["publish"]
        )

        # 重放结束后：旧的一步确认只满足 check，publish 由新信号补齐后才提交。
        final = coordinator.inspect("x")
        self.assertEqual(final["effective_version"], 2)
        self.assertEqual(final["drive"], "completed")
        # 重复旧信号识别为同一信号，没有产生第二份证据。
        duplicate = next(
            r["record"] for r in results
            if r["kind"] == "signal" and r["record"].get("duplicate")
        )
        self.assertEqual(duplicate["signal_id"], "old")
        # 迁移账本里保留了部分沿用的说明。
        codes = [item["code"] for item in final["retention"]]
        self.assertIn("evidence_reused", codes)
        self.assertIn("transfer_committed", codes)

    def test_in_flight_effect_waits_for_outcome_and_survives_restart(self):
        coordinator = Coordinator(":memory:")
        results = replay_file(
            coordinator, EXAMPLES / "in-flight-effect-at-migration.jsonl"
        )
        by_kind = {}
        for item in results:
            by_kind.setdefault(item["kind"], []).append(item)

        # 两个并发迁移请求，只有一个接管：第一个登记为候选（被在途 effect 阻塞），
        # 第二个直接冲突拒绝。
        migrations = by_kind["migrate"]
        self.assertTrue(migrations[0]["accepted"])
        self.assertTrue(migrations[0]["blocked"])
        self.assertFalse(migrations[1]["accepted"])
        self.assertEqual(migrations[1]["error"]["type"], "ConflictError")

        # unknown 回执后（第 6 行）到重启前，转移仍是候选、实例暂停在旧版。
        # 第 7 行 restart 不改变“候选 vs 生效版本”的区分；第 8 行 done 才允许提交。
        restart = next(r["record"] for r in results if r["kind"] == "restart")
        recovered = next(i for i in restart["instances"] if i["instance"] == "x")
        self.assertEqual(recovered["effective_version"], 1)
        self.assertEqual(recovered["drive"], "paused")
        self.assertEqual(
            recovered["active_transfer"]["state"], "proposed"
        )
        self.assertTrue(
            any(b["code"] == "in_flight_commitment"
                for b in recovered["active_transfer"]["blockers"])
        )

        # done 到达：外部结果保留，随后转移才原子提交；新版无映射的步骤重新派发。
        view = by_kind["inspect"][0]["record"]
        self.assertEqual(view["effective_version"], 2)
        self.assertIsNone(view["active_transfer"])
        self.assertTrue(
            any(item["code"] == "external_effect_done" for item in view["retention"])
        )
        waiting = {w["step"]: w for w in view["waiting_evidence"]}
        self.assertEqual(waiting["attach"]["status"], "queued")

    def test_message_registry_stays_in_sync(self):
        import json

        known = set(json.loads((EXAMPLES / "message_types.json").read_text("utf-8")))
        self.assertEqual(known, set(MESSAGE_TYPES))


if __name__ == "__main__":
    unittest.main()
