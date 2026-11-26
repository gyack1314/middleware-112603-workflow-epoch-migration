"""协调引擎：版本登记、实例账本、领取/回执与两阶段转版。

关键不变量：
* 流程定义与 compat 一旦入库不可变；
* 信号按 实例/版本/步骤/信号 id 去重，原因显式随信号登记；
* attempt 永久绑定领取时的版本与步骤，回执始终按当时语义解释；
* 转版只有 proposed 与 committed 两种存在方式，commit 在单个事务内完成，
  不存在“半个版本已生效”；blocker 未清时实例继续留在旧版并暂停动力。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any

from .errors import (
    ConflictError,
    NotFoundError,
    TransferBlockedError,
    ValidationError,
)
from .store import Store

SIGNAL = "signal"
EFFECT = "effect"
VALID_STEP_KINDS = (SIGNAL, EFFECT)
VALID_SETTLE_STATES = ("done", "never_started", "unknown")
VALID_REUSE = ("full", "partial", "none")


def _topo_order(steps: list[dict[str, Any]]) -> list[str]:
    """按 needs 做拓扑排序，同时校验重名、悬空依赖与环。"""
    by_name: dict[str, dict[str, Any]] = {}
    for step in steps:
        if step["name"] in by_name:
            raise ValidationError(f"步骤名重复：{step['name']}")
        by_name[step["name"]] = step
    for step in steps:
        for need in step["needs"]:
            if need not in by_name:
                raise ValidationError(
                    f"步骤 {step['name']} 依赖不存在的步骤 {need}"
                )
    ordered: list[str] = []
    visited: dict[str, int] = {}

    def visit(name: str, stack: list[str]) -> None:
        mark = visited.get(name, 0)
        if mark == 1:
            raise ValidationError(f"步骤依赖存在环：{' -> '.join(stack + [name])}")
        if mark == 2:
            return
        visited[name] = 1
        for need in by_name[name]["needs"]:
            visit(need, stack + [name])
        visited[name] = 2
        ordered.append(name)

    for step in steps:
        visit(step["name"], [])
    return ordered


class Coordinator:
    """线程安全的工作流协调器，状态全部由 :class:`Store` 持久化。"""

    def __init__(self, store: str | Store = ":memory:") -> None:
        self.store = store if isinstance(store, Store) else Store(store)
        self._lock = threading.RLock()

    def close(self) -> None:
        self.store.close()

    # ------------------------------------------------------------------ #
    # 流程定义与兼容映射
    # ------------------------------------------------------------------ #
    def publish_definition(self, version: int, steps: list[dict[str, Any]]) -> dict:
        """登记一个不可变流程版本。"""
        if type(version) is not int or version < 1:
            raise ValidationError("version 必须是正整数")
        if not isinstance(steps, list) or not steps:
            raise ValidationError("steps 必须是非空列表")
        clean: list[dict[str, Any]] = []
        for raw in steps:
            if not isinstance(raw, dict) or set(raw) < {"name", "kind", "needs"}:
                raise ValidationError("每个步骤需要 name、kind、needs 字段")
            name, kind, needs = raw["name"], raw["kind"], raw["needs"]
            if not isinstance(name, str) or not name:
                raise ValidationError("步骤 name 必须是非空字符串")
            if kind not in VALID_STEP_KINDS:
                raise ValidationError(f"步骤 {name} 的 kind 只能是 signal/effect")
            if not isinstance(needs, list) or not all(isinstance(n, str) for n in needs):
                raise ValidationError(f"步骤 {name} 的 needs 必须是字符串列表")
            clean.append({"name": name, "kind": kind, "needs": list(needs)})
        _topo_order(clean)  # 校验依赖图
        with self._lock:
            try:
                self.store.begin()
                if self._fetch_definition(version) is not None:
                    raise ConflictError(f"版本 {version} 的定义已存在且不可变")
                seq = self.store.next_seq()
                self.store.conn.execute(
                    "INSERT INTO definitions(version, steps_json, seq) VALUES(?,?,?)",
                    (version, json.dumps(clean, ensure_ascii=False), seq),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        return {"ok": True, "version": version, "steps": clean, "seq": seq}

    def publish_compat(
        self, from_version: int, to_version: int, mapping: dict[str, Any]
    ) -> dict:
        """登记两版之间的步骤映射，不可变。

        简写 ``"旧步骤": "新步骤"`` 与扩展形式都支持。扩展形式::

            {"step": "新步骤", "reason": "检查通过", "reuse": "partial",
             "alternatives": ["另一步", {"step": "再一步"}]}
        """
        if not isinstance(mapping, dict):
            raise ValidationError("compat 的 steps 映射必须是对象")
        with self._lock:
            from_def = self._require_definition(from_version)
            to_def = self._require_definition(to_version)
            if from_version == to_version:
                raise ValidationError("compat 的 from 与 to 不能相同")
            normalized = self._normalize_mapping(mapping, from_def, to_def)
            try:
                self.store.begin()
                exists = self.store.conn.execute(
                    "SELECT 1 FROM compat WHERE from_version=? AND to_version=?",
                    (from_version, to_version),
                ).fetchone()
                if exists:
                    raise ConflictError(
                        f"{from_version}->{to_version} 的 compat 已存在且不可变"
                    )
                seq = self.store.next_seq()
                self.store.conn.execute(
                    "INSERT INTO compat(from_version, to_version, mapping_json, seq)"
                    " VALUES(?,?,?,?)",
                    (
                        from_version,
                        to_version,
                        json.dumps(normalized, ensure_ascii=False),
                        seq,
                    ),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
        return {
            "ok": True,
            "from_version": from_version,
            "to_version": to_version,
            "mapping": normalized,
            "seq": seq,
        }

    def _normalize_mapping(
        self, mapping: dict[str, Any], from_def: dict, to_def: dict
    ) -> dict[str, dict[str, Any]]:
        to_names = {s["name"] for s in to_def["steps"]}
        from_names = {s["name"] for s in from_def["steps"]}
        result: dict[str, dict[str, Any]] = {}

        def as_target(value: Any) -> dict[str, Any]:
            if isinstance(value, str):
                target = {"step": value, "reuse": "full"}
            elif isinstance(value, dict) and isinstance(value.get("step"), str):
                reuse = value.get("reuse", "full")
                if reuse not in VALID_REUSE:
                    raise ValidationError(f"未知 reuse：{reuse}")
                target = {
                    "step": value["step"],
                    "reuse": reuse,
                }
                if "reason" in value:
                    if not isinstance(value["reason"], str):
                        raise ValidationError("映射 reason 必须是字符串")
                    target["reason"] = value["reason"]
                if "alternatives" in value:
                    alts = value["alternatives"]
                    if not isinstance(alts, list) or not alts:
                        raise ValidationError("alternatives 必须是非空列表")
                    target["alternatives"] = [
                        a["step"] if isinstance(a, dict) else a for a in alts
                    ]
            else:
                raise ValidationError("映射值必须是步骤名字符串或含 step 的对象")
            if target["step"] not in to_names:
                raise ValidationError(f"映射目标步骤不存在：{target['step']}")
            for alt in target.get("alternatives", []):
                if alt not in to_names:
                    raise ValidationError(f"替代目标步骤不存在：{alt}")
            return target

        for from_step, value in mapping.items():
            if from_step not in from_names:
                raise ValidationError(f"映射源步骤不存在：{from_step}")
            result[from_step] = as_target(value)
        return result

    # ------------------------------------------------------------------ #
    # 实例生命周期
    # ------------------------------------------------------------------ #
    def start(self, instance: str, version: int) -> dict:
        """按明确指定的版本打开实例，创建该版全部步骤的账本行。"""
        if not isinstance(instance, str) or not instance:
            raise ValidationError("instance 必须是非空字符串")
        definition = self._require_definition(version)
        with self._lock:
            row = self._fetch_instance(instance)
            if row is not None:
                raise ConflictError(
                    f"实例 {instance} 已在版本 {row['startup_version']} 启动"
                )
            try:
                self.store.begin()
                seq = self.store.next_seq()
                self.store.conn.execute(
                    "INSERT INTO instances(instance, startup_version,"
                    " effective_version, drive, created_seq) VALUES(?,?,?,?,?)",
                    (instance, version, version, "running", seq),
                )
                for step in definition["steps"]:
                    self.store.conn.execute(
                        "INSERT INTO instance_steps(instance, version, step, kind,"
                        " needs_json, state) VALUES(?,?,?,?,?, 'waiting')",
                        (
                            instance,
                            version,
                            step["name"],
                            step["kind"],
                            json.dumps(step["needs"], ensure_ascii=False),
                        ),
                    )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
            self._reconcile(instance)
        return {
            "ok": True,
            "instance": instance,
            "startup_version": version,
            "effective_version": version,
            "seq": seq,
        }

    # ------------------------------------------------------------------ #
    # 人工信号
    # ------------------------------------------------------------------ #
    def signal(
        self,
        instance: str,
        version: int,
        step: str,
        signal_id: str,
        reason: str | None = None,
    ) -> dict:
        """登记人工信号；同一 实例/版本/步骤/id 重复提交只识别为一次。"""
        if not signal_id or not isinstance(signal_id, str):
            raise ValidationError("信号 id 必须是非空字符串")
        with self._lock:
            inst = self._require_instance(instance)
            proposal = self._active_proposal(instance)
            candidate_version = (
                proposal["to_version"] if proposal is not None else None
            )
            # 候选转移期间，目标版本的步骤已在计划中“预开账”，
            # 新版信号可以先登记，作为解除 blocker 的证据；提交前生效版仍是旧版。
            step_row = self._fetch_step(instance, version, step)
            if step_row is None and version == candidate_version:
                target_def = self._require_definition(version)
                if not any(s["name"] == step for s in target_def["steps"]):
                    raise NotFoundError(
                        f"目标版本 {version} 没有步骤 {step}"
                    )
            elif step_row is None:
                raise NotFoundError(
                    f"实例 {instance} 的版本 {version} 没有步骤 {step}"
                )
            if step_row is not None and step_row["kind"] != SIGNAL:
                raise ValidationError(f"步骤 {step} 不是人工信号步骤")
            try:
                self.store.begin()
                seq = self.store.next_seq()
                self.store.conn.execute(
                    "INSERT INTO signals(instance, version, step, signal_id, reason,"
                    " seq) VALUES(?,?,?,?,?,?)",
                    (instance, version, step, signal_id, reason, seq),
                )
            except sqlite3.IntegrityError:
                self.store.rollback()
                dup = self.store.conn.execute(
                    "SELECT seq FROM signals WHERE instance=? AND version=? AND step=?"
                    " AND signal_id=?",
                    (instance, version, step, signal_id),
                ).fetchone()
                return {
                    "ok": True,
                    "duplicate": True,
                    "instance": instance,
                    "version": version,
                    "step": step,
                    "signal_id": signal_id,
                    "seq": dup["seq"],
                }
            try:
                applied = False
                if step_row is None:
                    # 候选转移的目标版本步骤：信号先落在 signals 账本，
                    # 提交时由转版计划据此把步骤置为 satisfied。
                    applied = False
                elif version == inst["effective_version"] and step_row["state"] == "waiting":
                    self.store.conn.execute(
                        "UPDATE instance_steps SET state='satisfied',"
                        " satisfied_by=? WHERE instance=? AND version=? AND step=?",
                        (f"signal:{signal_id}", instance, version, step),
                    )
                    applied = True
                elif version != inst["effective_version"]:
                    # 旧版信号到达新实例：进账本并留痕，是否被映射采用由转版计划判断。
                    self._add_retention(
                        instance,
                        "legacy_signal",
                        f"旧版本 {version} 步骤 {step} 的信号 {signal_id} 已入账本，"
                        "按原版本语义解释",
                        {"version": version, "step": step, "signal_id": signal_id},
                        seq=seq,
                    )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
            proposal = self._active_proposal(instance)
            if proposal is not None:
                self._evaluate_proposal(proposal)
            else:
                self._reconcile(instance)
        return {
            "ok": True,
            "duplicate": False,
            "applied": applied,
            "instance": instance,
            "version": version,
            "step": step,
            "signal_id": signal_id,
            "seq": seq,
        }

    # ------------------------------------------------------------------ #
    # 执行端领取与回执
    # ------------------------------------------------------------------ #
    def claim(self, instance: str, version: int, step: str, attempt: str) -> dict:
        """执行端按自己采用的版本领取 effect；attempt 与领取时的步骤永久绑定。"""
        with self._lock:
            inst = self._require_instance(instance)
            if self._active_proposal(instance) is not None:
                raise ConflictError(
                    f"实例 {instance} 正在准备转版，动力已暂停，不接受新的领取"
                )
            if version != inst["effective_version"]:
                raise ConflictError(
                    f"实例当前生效版本是 {inst['effective_version']}，"
                    f"不能按版本 {version} 领取"
                )
            step_row = self._fetch_step(instance, version, step)
            if step_row is None:
                raise NotFoundError(f"版本 {version} 没有步骤 {step}")
            if step_row["kind"] != EFFECT:
                raise ValidationError(f"步骤 {step} 不是 effect，不能领取")
            if step_row["state"] != "waiting":
                raise ConflictError(f"步骤 {step} 已完结，无需领取")
            dispatch = self.store.conn.execute(
                "SELECT * FROM dispatches WHERE instance=? AND version=? AND step=?"
                " AND state='queued'",
                (instance, version, step),
            ).fetchone()
            if dispatch is None:
                raise ConflictError(f"步骤 {step} 的依赖尚未齐备或工作项已取消")
            try:
                self.store.begin()
                seq = self.store.next_seq()
                self.store.conn.execute(
                    "INSERT INTO attempts(attempt, instance, version, step, state,"
                    " claimed_seq) VALUES(?,?,?,?, 'claimed', ?)",
                    (attempt, instance, version, step, seq),
                )
                self.store.conn.execute(
                    "UPDATE dispatches SET state='claimed', attempt=? WHERE id=?",
                    (attempt, dispatch["id"]),
                )
                self.store.commit()
            except sqlite3.IntegrityError:
                self.store.rollback()
                raise ConflictError(f"attempt {attempt} 已存在，标识必须全局唯一")
            except Exception:
                self.store.rollback()
                raise
        return {
            "ok": True,
            "attempt": attempt,
            "instance": instance,
            "version": version,
            "step": step,
            "seq": seq,
        }

    def settle(self, attempt: str, state: str) -> dict:
        """执行端回执：done / never_started / unknown，按领取时的版本步骤解释。"""
        if state not in VALID_SETTLE_STATES:
            raise ValidationError(f"回执状态只能是 {VALID_SETTLE_STATES} 之一")
        with self._lock:
            row = self.store.conn.execute(
                "SELECT * FROM attempts WHERE attempt=?", (attempt,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"attempt {attempt} 不存在")
            instance = row["instance"]
            if row["state"] != "claimed" and row["state"] != "unknown":
                if row["state"] == state:
                    return {
                        "ok": True,
                        "idempotent": True,
                        "attempt": attempt,
                        "state": state,
                        "version": row["version"],
                        "step": row["step"],
                    }
                raise ConflictError(
                    f"attempt {attempt} 已回执为 {row['state']}，"
                    f"不能改为 {state}；对外结果保留，不允许重做"
                )
            try:
                self.store.begin()
                seq = self.store.next_seq()
                self.store.conn.execute(
                    "UPDATE attempts SET state=?, settled_seq=? WHERE attempt=?",
                    (state, seq, attempt),
                )
                if state == "done":
                    self.store.conn.execute(
                        "UPDATE dispatches SET state='done' WHERE attempt=?",
                        (attempt,),
                    )
                    self.store.conn.execute(
                        "UPDATE instance_steps SET state='satisfied',"
                        " satisfied_by=? WHERE instance=? AND version=? AND step=?",
                        (
                            f"attempt:{attempt}",
                            instance,
                            row["version"],
                            row["step"],
                        ),
                    )
                    self._add_retention(
                        instance,
                        "external_effect_done",
                        f"步骤 {row['step']}（版本 {row['version']}）已对外生效，"
                        "结果保留，迁移不会重做",
                        {"attempt": attempt, "version": row["version"],
                         "step": row["step"]},
                        seq=seq,
                    )
                elif state == "never_started":
                    self.store.conn.execute(
                        "UPDATE dispatches SET state='canceled' WHERE attempt=?",
                        (attempt,),
                    )
                    self._add_retention(
                        instance,
                        "effect_released",
                        f"步骤 {row['step']} 的执行端确认从未开始，承诺已释放",
                        {"attempt": attempt, "step": row["step"]},
                        seq=seq,
                    )
                else:  # unknown：动力暂停，结局确定前不推进
                    self._add_retention(
                        instance,
                        "effect_outcome_unknown",
                        f"步骤 {row['step']} 的外部结局未知，暂停动力等待明确回执",
                        {"attempt": attempt, "step": row["step"]},
                        seq=seq,
                    )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
            proposal = self._active_proposal(instance)
            if proposal is not None:
                self._evaluate_proposal(proposal)
            else:
                self._reconcile(instance)
        return {
            "ok": True,
            "idempotent": False,
            "attempt": attempt,
            "state": state,
            "version": row["version"],
            "step": row["step"],
            "seq": seq,
        }

    # ------------------------------------------------------------------ #
    # 两阶段转版
    # ------------------------------------------------------------------ #
    def request_migration(self, instance: str, request_id: str, to_version: int) -> dict:
        """提出转版：立即返回候选计划；有 blocker 时保持 proposed 并暂停动力。"""
        with self._lock:
            inst = self._require_instance(instance)
            self._require_definition(to_version)
            active = self._active_proposal(instance)
            if active is not None:
                if active["request_id"] == request_id and active["to_version"] == to_version:
                    return {
                        "ok": True,
                        "idempotent": True,
                        "transfer": self._transfer_view(active),
                    }
                raise ConflictError(
                    f"实例已有未决转移 {active['request_id']}，"
                    "两个迁移请求只能有一个接管"
                )
            if to_version == inst["effective_version"]:
                raise ConflictError(
                    f"实例已运行在版本 {to_version}，无需转版"
                )
            compat = self._fetch_compat(inst["effective_version"], to_version)
            if compat is None:
                raise NotFoundError(
                    f"缺少 {inst['effective_version']}->{to_version} 的 compat 声明，"
                    "实例继续保留在旧版本"
                )
            plan, blockers = self._build_plan(instance, inst["effective_version"],
                                              to_version, compat)
            try:
                self.store.begin()
                seq = self.store.next_seq()
                self.store.conn.execute(
                    "INSERT INTO transfers(request_id, instance, from_version,"
                    " to_version, state, plan_json, blockers_json, created_seq)"
                    " VALUES(?,?,?,?, 'proposed', ?, ?, ?)",
                    (
                        request_id,
                        instance,
                        inst["effective_version"],
                        to_version,
                        json.dumps(plan, ensure_ascii=False),
                        json.dumps(blockers, ensure_ascii=False),
                        seq,
                    ),
                )
                # 准备期暂停动力；尚未被执行端取得的 queued 工作项直接取消。
                self.store.conn.execute(
                    "UPDATE instances SET drive='paused' WHERE instance=?",
                    (instance,),
                )
                canceled = self.store.conn.execute(
                    "UPDATE dispatches SET state='canceled' WHERE instance=?"
                    " AND version=? AND state='queued'",
                    (instance, inst["effective_version"]),
                ).rowcount
                self._add_retention(
                    instance,
                    "transfer_proposed",
                    f"转移 {request_id} 进入候选：{inst['effective_version']} -> "
                    f"{to_version}，动力暂停，取消 {canceled} 个未领取工作项",
                    {"request_id": request_id, "to_version": to_version,
                     "canceled_queued": canceled},
                    seq=seq,
                )
                self.store.commit()
            except sqlite3.IntegrityError:
                # 并发下部分唯一索引兜底：只有一个 proposed 能落库。
                self.store.rollback()
                winner = self._active_proposal(instance)
                raise ConflictError(
                    "并发转移冲突，接管者为 "
                    f"{winner['request_id'] if winner else '另一个请求'}"
                )
            except Exception:
                self.store.rollback()
                raise
            if not blockers:
                return self._commit_transfer(instance, request_id)
        raise TransferBlockedError(
            f"转移 {request_id} 已登记为候选，但存在未决 blocker，实例保留在版本 "
            f"{inst['effective_version']}"
        )

    def abort_transfer(self, instance: str, request_id: str | None = None) -> dict:
        """放弃候选转移，实例恢复旧版运行；已对外生效的结果一律保留。"""
        with self._lock:
            proposal = self._active_proposal(instance)
            if proposal is None:
                raise NotFoundError(f"实例 {instance} 没有未决转移")
            if request_id is not None and proposal["request_id"] != request_id:
                raise ConflictError(
                    f"未决转移是 {proposal['request_id']}，与请求的 {request_id} 不一致"
                )
            try:
                self.store.begin()
                seq = self.store.next_seq()
                self.store.conn.execute(
                    "UPDATE transfers SET state='aborted', decided_seq=?"
                    " WHERE request_id=?",
                    (seq, proposal["request_id"]),
                )
                self.store.conn.execute(
                    "UPDATE instances SET drive='running' WHERE instance=?",
                    (instance,),
                )
                self._add_retention(
                    instance,
                    "transfer_aborted",
                    f"转移 {proposal['request_id']} 已放弃，继续运行版本 "
                    f"{proposal['from_version']}",
                    {"request_id": proposal["request_id"]},
                    seq=seq,
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
            self._reconcile(instance)
        return {"ok": True, "instance": instance, "aborted": proposal["request_id"],
                "seq": seq}

    def _build_plan(
        self, instance: str, from_version: int, to_version: int, compat: dict
    ) -> tuple[list[dict], list[dict]]:
        """根据当前证据与 compat 映射推演新版每个步骤能否被满足。"""
        mapping = compat["mapping"]
        # 新步骤名 -> 可满足它的旧来源列表。
        targets: dict[str, list[str]] = {}
        # 旧来源 -> 它声明过的全部可选目标；同一来源的多个目标即“替代有歧义”。
        ambiguous_sources: dict[str, list[str]] = {}
        for from_step, rule in mapping.items():
            choices = [rule["step"], *rule.get("alternatives", [])]
            if len(set(choices)) > 1:
                ambiguous_sources[from_step] = choices
            for target in choices:
                if from_step not in targets.setdefault(target, []):
                    targets[target].append(from_step)

        to_def = self._require_definition(to_version)
        old_steps = {
            r["step"]: r
            for r in self.store.conn.execute(
                "SELECT * FROM instance_steps WHERE instance=? AND version=?",
                (instance, from_version),
            ).fetchall()
        }
        plan: list[dict] = []
        blockers: list[dict] = []

        for step in to_def["steps"]:
            name = step["name"]
            entry: dict[str, Any] = {
                "step": name,
                "kind": step["kind"],
                "status": "waiting",
                "sources": targets.get(name, []),
            }
            sources = targets.get(name, [])
            ambiguous_here = [
                source for source in sources if source in ambiguous_sources
            ]
            if ambiguous_here:
                # 同一旧信号可被解释到多个新步骤，替代映射有歧义，不许自动挑选。
                entry["status"] = "blocked"
                for source in ambiguous_here:
                    blockers.append({
                        "code": "ambiguous_alternatives",
                        "step": name,
                        "message": f"旧步骤 {source} 声明了多个可选目标"
                                   f"{ambiguous_sources[source]}，映射有歧义，"
                                   f"不能自动选择是否用于 {name}",
                    })
                plan.append(entry)
                continue
            if len(sources) > 1:
                entry["status"] = "blocked"
                blockers.append({
                    "code": "ambiguous_alternatives",
                    "step": name,
                    "message": f"新步骤 {name} 有多个可替代旧来源 {sources}，"
                               "映射有歧义，不能自动选择",
                })
                plan.append(entry)
                continue
            if sources:
                from_step = sources[0]
                rule = mapping[from_step]
                old = old_steps.get(from_step)
                entry["from_step"] = from_step
                entry["reuse"] = rule["reuse"]
                if old is None:
                    blockers.append({
                        "code": "missing_input",
                        "step": name,
                        "message": f"映射来源 {from_step} 在实例账本中不存在",
                    })
                    entry["status"] = "blocked"
                elif rule["reuse"] == "none":
                    entry["status"] = "waiting"
                    blockers.append({
                        "code": "mapping_declared_none",
                        "step": name,
                        "message": f"映射声明旧 {from_step} 的证据不可用于新 {name}，"
                                   "必须重新取得",
                    })
                elif old["state"] != "satisfied":
                    # 旧步骤本身还没拿到证据（含 effect 在途）。
                    attempt = self._latest_attempt(instance, from_version, from_step)
                    if attempt is not None and attempt["state"] in ("claimed", "unknown"):
                        blockers.append({
                            "code": "in_flight_commitment",
                            "step": name,
                            "message": f"旧步骤 {from_step} 已被领取"
                                       f"（attempt {attempt['attempt']}，"
                                       f"状态 {attempt['state']}），须等它释放或完结",
                            "detail": {"attempt": attempt["attempt"],
                                       "state": attempt["state"]},
                        })
                    else:
                        blockers.append({
                            "code": "missing_input",
                            "step": name,
                            "message": f"旧步骤 {from_step} 尚未取得证据，"
                                       f"新步骤 {name} 的必要输入缺失",
                        })
                    entry["status"] = "blocked"
                elif rule["reuse"] == "partial":
                    # 拆分场景：单一旧信号只能满足被声明的那一项。
                    if "reason" not in rule:
                        blockers.append({
                            "code": "partial_without_reason",
                            "step": name,
                            "message": f"到 {name} 的部分映射未声明 reason，"
                                       "无法核对旧信号语义",
                        })
                        entry["status"] = "blocked"
                    elif not self._signal_with_reason_exists(
                        instance, from_version, from_step, rule["reason"]
                    ):
                        blockers.append({
                            "code": "reason_mismatch",
                            "step": name,
                            "message": f"旧 {from_step} 没有原因为“{rule['reason']}”"
                                       f"的信号，不能部分满足 {name}",
                        })
                        entry["status"] = "blocked"
                    else:
                        entry["status"] = "satisfied"
                        entry["satisfied_by"] = f"mapping:{from_step}"
                else:  # full
                    entry["status"] = "satisfied"
                    entry["satisfied_by"] = f"mapping:{from_step}"
            else:
                # 无旧版映射的新步骤：
                # * signal 步骤必须显式取得人工信号，缺失即 blocker，不能借旧信号完成；
                # * effect 步骤是新版本自己的新工作，不阻塞转版，提交后正常派发。
                if step["kind"] == SIGNAL:
                    if self.store.conn.execute(
                        "SELECT 1 FROM signals WHERE instance=? AND version=? AND step=?"
                        " LIMIT 1",
                        (instance, to_version, name),
                    ).fetchone():
                        entry["status"] = "satisfied"
                        entry["satisfied_by"] = "signal:pending-version"
                    else:
                        entry["status"] = "blocked"
                        blockers.append({
                            "code": "missing_input",
                            "step": name,
                            "message": f"新人工步骤 {name} 没有兼容映射，"
                                       "必须在新版本取得显式信号",
                        })
                else:
                    entry["status"] = "waiting"
                    entry["note"] = "新版本新增 effect，提交后派发，不阻塞转版"
            plan.append(entry)

        # 仍在途（claimed/unknown）且映射无法沿用承诺的 effect 才构成硬 blocker；
        # done 已在上面按映射沿用，never_started 已释放，都不再阻塞。
        for attempt in self.store.conn.execute(
            "SELECT * FROM attempts WHERE instance=? AND version=? AND state IN"
            " ('claimed','unknown')",
            (instance, from_version),
        ).fetchall():
            rule = mapping.get(attempt["step"])
            reusable = (
                rule is not None
                and rule["reuse"] == "full"
                and rule["step"] in {s["name"] for s in to_def["steps"]}
            )
            if not reusable:
                blockers.append({
                    "code": "in_flight_commitment",
                    "step": attempt["step"],
                    "message": f"旧 effect {attempt['step']}（attempt "
                               f"{attempt['attempt']}）已被领取且无法在新版本沿用"
                               "同一承诺，须等执行端释放或确认结局",
                    "detail": {"attempt": attempt["attempt"],
                               "state": attempt["state"]},
                })
        return plan, blockers

    def _evaluate_proposal(self, proposal: sqlite3.Row) -> None:
        """新证据到达后重估候选转移：blocker 清空则提交，否则更新候选计划。"""
        with self._lock:
            if proposal["state"] != "proposed":
                return
            instance = proposal["instance"]
            compat = self._fetch_compat(
                proposal["from_version"], proposal["to_version"]
            )
            plan, blockers = self._build_plan(
                instance,
                proposal["from_version"],
                proposal["to_version"],
                compat,
            )
            if blockers:
                self.store.begin()
                try:
                    self.store.conn.execute(
                        "UPDATE transfers SET plan_json=?, blockers_json=?"
                        " WHERE request_id=?",
                        (json.dumps(plan, ensure_ascii=False),
                         json.dumps(blockers, ensure_ascii=False),
                         proposal["request_id"]),
                    )
                    self.store.commit()
                except Exception:
                    self.store.rollback()
                    raise
                return
            self._commit_transfer(instance, proposal["request_id"])

    def _commit_transfer(self, instance: str, request_id: str) -> dict:
        """原子提交：在一个事务里把实例切到新版，绝不留下半生效状态。"""
        with self._lock:
            proposal = self.store.conn.execute(
                "SELECT * FROM transfers WHERE request_id=?", (request_id,)
            ).fetchone()
            if proposal is None or proposal["state"] != "proposed":
                raise ConflictError(f"转移 {request_id} 不是未决候选")
            inst = self._require_instance(instance)
            to_def = self._require_definition(proposal["to_version"])
            compat = self._fetch_compat(
                proposal["from_version"], proposal["to_version"]
            )
            plan, blockers = self._build_plan(
                instance,
                proposal["from_version"],
                proposal["to_version"],
                compat,
            )
            if blockers:
                raise ConflictError("转移仍有 blocker，不能提交")
            try:
                self.store.begin()
                seq = self.store.next_seq()
                for entry in plan:
                    satisfied_by = entry.get("satisfied_by")
                    if satisfied_by == "signal:pending-version":
                        signal_row = self.store.conn.execute(
                            "SELECT signal_id FROM signals WHERE instance=?"
                            " AND version=? AND step=? ORDER BY seq LIMIT 1",
                            (instance, proposal["to_version"], entry["step"]),
                        ).fetchone()
                        satisfied_by = f"signal:{signal_row['signal_id']}"
                    self.store.conn.execute(
                        "INSERT INTO instance_steps(instance, version, step, kind,"
                        " needs_json, state, satisfied_by) VALUES(?,?,?,?,?,?,?)",
                        (
                            instance,
                            proposal["to_version"],
                            entry["step"],
                            entry["kind"],
                            json.dumps(
                                next(
                                    s["needs"] for s in to_def["steps"]
                                    if s["name"] == entry["step"]
                                ),
                                ensure_ascii=False,
                            ),
                            "satisfied" if entry["status"] == "satisfied" else "waiting",
                            satisfied_by,
                        ),
                    )
                    if entry["status"] == "satisfied" and entry.get("from_step"):
                        self._add_retention(
                            instance,
                            "evidence_reused",
                            f"旧步骤 {entry['from_step']} 的证据按映射沿用到新步骤 "
                            f"{entry['step']}（{entry['reuse']}）",
                            {"from_step": entry["from_step"],
                             "to_step": entry["step"], "reuse": entry["reuse"]},
                            seq=seq,
                        )
                self.store.conn.execute(
                    "UPDATE transfers SET state='committed', decided_seq=?,"
                    " plan_json=?, blockers_json=? WHERE request_id=?",
                    (seq, json.dumps(plan, ensure_ascii=False), "[]", request_id),
                )
                self.store.conn.execute(
                    "UPDATE instances SET effective_version=?, drive='running'"
                    " WHERE instance=?",
                    (proposal["to_version"], instance),
                )
                self._add_retention(
                    instance,
                    "transfer_committed",
                    f"实例原子切换到版本 {proposal['to_version']}，"
                    "旧版本账本与已生效结果保留",
                    {"request_id": request_id,
                     "from_version": proposal["from_version"],
                     "to_version": proposal["to_version"]},
                    seq=seq,
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
            self._reconcile(instance)
        view = self._transfer_view(
            self.store.conn.execute(
                "SELECT * FROM transfers WHERE request_id=?", (request_id,)
            ).fetchone()
        )
        return {"ok": True, "committed": True, "transfer": view, "seq": seq}

    # ------------------------------------------------------------------ #
    # 重启与值班查询
    # ------------------------------------------------------------------ #
    def restart(self) -> dict:
        """协调进程重启：状态全部从盘上恢复，候选转移与生效版本仍可区分。"""
        with self._lock:
            instances = self.store.conn.execute(
                "SELECT instance FROM instances"
            ).fetchall()
            recovered = []
            for row in instances:
                instance = row["instance"]
                inst = self._require_instance(instance)
                proposal = self._active_proposal(instance)
                if proposal is not None:
                    # 候选转移跨重启仍暂停动力；重估一次以反映盘上最新证据。
                    self._evaluate_proposal(proposal)
                    inst = self._require_instance(instance)
                if inst["drive"] == "running":
                    self._reconcile(instance)
                recovered.append(self.inspect(instance))
        return {"ok": True, "recovered_instances": len(instances),
                "instances": recovered}

    def inspect(self, instance: str) -> dict:
        """值班视图：在等什么证据、运行在哪版、旧版为何保留，一次查清。"""
        with self._lock:
            inst = self._require_instance(instance)
            effective = inst["effective_version"]
            steps = self.store.conn.execute(
                "SELECT * FROM instance_steps WHERE instance=? AND version=?"
                " ORDER BY rowid",
                (instance, effective),
            ).fetchall()
            definition = self._fetch_definition(effective)
            order = _topo_order(definition["steps"])
            proposal = self._active_proposal(instance)
            waiting_evidence: list[dict] = []
            for name in order:
                step = next(s for s in steps if s["step"] == name)
                if step["state"] == "satisfied":
                    continue
                needs = json.loads(step["needs_json"])
                blocked_deps = [
                    n for n in needs
                    if next(s for s in steps if s["step"] == n)["state"] != "satisfied"
                ]
                item: dict[str, Any] = {
                    "step": name,
                    "kind": step["kind"],
                    "waiting_for": "signal" if step["kind"] == SIGNAL else "effect",
                }
                if blocked_deps:
                    item["status"] = "blocked_by_dependencies"
                    item["blocked_dependencies"] = blocked_deps
                elif step["kind"] == EFFECT:
                    dispatch = self.store.conn.execute(
                        "SELECT * FROM dispatches WHERE instance=? AND version=?"
                        " AND step=? ORDER BY id DESC LIMIT 1",
                        (instance, effective, name),
                    ).fetchone()
                    if dispatch is None:
                        item["status"] = "not_dispatched"
                    elif dispatch["state"] == "canceled" and proposal is not None:
                        item["status"] = "paused_pending_transfer"
                    else:
                        item["status"] = dispatch["state"]
                    if dispatch is not None and dispatch["attempt"]:
                        item["attempt"] = dispatch["attempt"]
                else:
                    item["status"] = "awaiting_human_signal"
                waiting_evidence.append(item)
            retention = [
                {
                    "seq": r["seq"],
                    "code": r["code"],
                    "message": r["message"],
                    "detail": json.loads(r["detail_json"]),
                }
                for r in self.store.conn.execute(
                    "SELECT * FROM retention WHERE instance=? ORDER BY seq",
                    (instance,),
                ).fetchall()
            ]
            versions = [
                r["version"]
                for r in self.store.conn.execute(
                    "SELECT DISTINCT version FROM instance_steps WHERE instance=?"
                    " ORDER BY version",
                    (instance,),
                ).fetchall()
            ]
            return {
                "instance": instance,
                "startup_version": inst["startup_version"],
                "effective_version": effective,
                "drive": inst["drive"],
                "versions_on_ledger": versions,
                "waiting_evidence": waiting_evidence,
                "active_transfer": (
                    self._transfer_view(proposal) if proposal is not None else None
                ),
                "retention": retention,
            }

    # ------------------------------------------------------------------ #
    # 内部辅助
    # ------------------------------------------------------------------ #
    def _reconcile(self, instance: str) -> None:
        """推进动力：依赖齐备的 effect 生成 queued 工作项；全部满足则收尾。

        仅在实例 running 时派发，proposed 转移期间动力保持暂停。
        """
        inst = self._require_instance(instance)
        if inst["drive"] != "running":
            return
        version = inst["effective_version"]
        definition = self._fetch_definition(version)
        rows = {
            r["step"]: r
            for r in self.store.conn.execute(
                "SELECT * FROM instance_steps WHERE instance=? AND version=?",
                (instance, version),
            ).fetchall()
        }
        self.store.begin()
        try:
            for name in _topo_order(definition["steps"]):
                row = rows[name]
                if row["state"] == "satisfied":
                    continue
                needs = json.loads(row["needs_json"])
                if any(rows[n]["state"] != "satisfied" for n in needs):
                    continue
                if row["kind"] == EFFECT:
                    # canceled 是转版准备期撤销的未领取项，abort 后允许重新排队；
                    # done 永不重做，claimed 仍在途。
                    exists = self.store.conn.execute(
                        "SELECT 1 FROM dispatches WHERE instance=? AND version=?"
                        " AND step=? AND state IN ('queued','claimed','done')",
                        (instance, version, name),
                    ).fetchone()
                    if not exists:
                        seq = self.store.next_seq()
                        self.store.conn.execute(
                            "INSERT INTO dispatches(instance, version, step, state,"
                            " seq) VALUES(?,?,?, 'queued', ?)",
                            (instance, version, name, seq),
                        )
            if all(r["state"] == "satisfied" for r in rows.values()):
                self.store.conn.execute(
                    "UPDATE instances SET drive='completed' WHERE instance=?",
                    (instance,),
                )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise

    def _add_retention(
        self,
        instance: str,
        code: str,
        message: str,
        detail: dict[str, Any] | None = None,
        seq: int | None = None,
    ) -> None:
        """调用方需已持有事务；seq 可复用当前事务序号。"""
        if seq is None:
            seq = self.store.next_seq()
        self.store.conn.execute(
            "INSERT INTO retention(instance, seq, code, message, detail_json)"
            " VALUES(?,?,?,?,?)",
            (instance, seq, code, message,
             json.dumps(detail or {}, ensure_ascii=False)),
        )

    def _signal_with_reason_exists(
        self, instance: str, version: int, step: str, reason: str
    ) -> bool:
        return self.store.conn.execute(
            "SELECT 1 FROM signals WHERE instance=? AND version=? AND step=?"
            " AND reason=? LIMIT 1",
            (instance, version, step, reason),
        ).fetchone() is not None

    def _latest_attempt(self, instance: str, version: int, step: str):
        return self.store.conn.execute(
            "SELECT * FROM attempts WHERE instance=? AND version=? AND step=?"
            " ORDER BY claimed_seq DESC LIMIT 1",
            (instance, version, step),
        ).fetchone()

    def _fetch_definition(self, version: int):
        row = self.store.conn.execute(
            "SELECT * FROM definitions WHERE version=?", (version,)
        ).fetchone()
        if row is None:
            return None
        return {"version": version, "steps": json.loads(row["steps_json"])}

    def _require_definition(self, version: int) -> dict:
        definition = self._fetch_definition(version)
        if definition is None:
            raise NotFoundError(f"流程版本 {version} 尚未登记")
        return definition

    def _fetch_compat(self, from_version: int, to_version: int):
        row = self.store.conn.execute(
            "SELECT * FROM compat WHERE from_version=? AND to_version=?",
            (from_version, to_version),
        ).fetchone()
        if row is None:
            return None
        return {"from_version": from_version, "to_version": to_version,
                "mapping": json.loads(row["mapping_json"])}

    def _fetch_instance(self, instance: str):
        return self.store.conn.execute(
            "SELECT * FROM instances WHERE instance=?", (instance,)
        ).fetchone()

    def _require_instance(self, instance: str):
        row = self._fetch_instance(instance)
        if row is None:
            raise NotFoundError(f"实例 {instance} 不存在")
        return row

    def _fetch_step(self, instance: str, version: int, step: str):
        return self.store.conn.execute(
            "SELECT * FROM instance_steps WHERE instance=? AND version=? AND step=?",
            (instance, version, step),
        ).fetchone()

    def _active_proposal(self, instance: str):
        return self.store.conn.execute(
            "SELECT * FROM transfers WHERE instance=? AND state='proposed'",
            (instance,),
        ).fetchone()

    def _transfer_view(self, row: sqlite3.Row) -> dict:
        return {
            "request_id": row["request_id"],
            "instance": row["instance"],
            "from_version": row["from_version"],
            "to_version": row["to_version"],
            "state": row["state"],
            "plan": json.loads(row["plan_json"]),
            "blockers": json.loads(row["blockers_json"]),
            "created_seq": row["created_seq"],
            "decided_seq": row["decided_seq"],
        }
