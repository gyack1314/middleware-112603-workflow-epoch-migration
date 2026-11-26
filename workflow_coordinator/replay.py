"""把演练 JSONL 的九类消息按到达顺序重放到协调引擎。

每条消息独立给出结果；协调层的可预期错误记为 rejected 而不中断后续消息，
这样值班演练可以完整看到“保留旧版/待人工”等分支。字段层面的坏消息仍按
scenario_reader 的惯例抛出 ValueError。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scenario_reader import iter_messages

from .engine import Coordinator
from .errors import CoordinatorError, TransferBlockedError

MESSAGE_TYPES = [
    "claim",
    "compat",
    "definition",
    "inspect",
    "migrate",
    "restart",
    "settle",
    "signal",
    "start",
]


def _require(record: dict, key: str, kind: str) -> Any:
    if key not in record:
        raise ValueError(f"{kind} 消息缺少字段 {key}")
    return record[key]


def replay_file(
    coordinator: Coordinator, path: str | Path
) -> list[dict[str, Any]]:
    """重放单个 JSONL 文件，返回逐条结果，顺序与文件行序一致。"""
    results: list[dict[str, Any]] = []
    for line, message in enumerate(
        iter_messages(Path(path), set(MESSAGE_TYPES)), start=1
    ):
        kind, record, at = message["kind"], message["record"], message["at"]
        outcome = {"line": line, "at": at, "kind": kind}
        try:
            outcome["record"] = _dispatch(coordinator, kind, record)
            outcome["accepted"] = True
            outcome["blocked"] = False
        except TransferBlockedError as error:
            # 候选转移已成功登记，只是 blocker 未清：属于正常的“待人工/留旧版”分支。
            outcome["accepted"] = True
            outcome["blocked"] = True
            outcome["record"] = {
                "blocked": True,
                "error": {"type": type(error).__name__, "message": str(error)},
            }
        except CoordinatorError as error:
            outcome["accepted"] = False
            outcome["blocked"] = False
            outcome["error"] = {"type": type(error).__name__, "message": str(error)}
        results.append(outcome)
    return results


def _dispatch(coordinator: Coordinator, kind: str, record: dict) -> dict:
    if kind == "definition":
        return coordinator.publish_definition(
            _require(record, "version", kind),
            _require(record, "steps", kind),
        )
    if kind == "compat":
        return coordinator.publish_compat(
            _require(record, "from", kind),
            _require(record, "to", kind),
            _require(record, "steps", kind),
        )
    if kind == "start":
        return coordinator.start(
            _require(record, "instance", kind),
            _require(record, "version", kind),
        )
    if kind == "signal":
        return coordinator.signal(
            _require(record, "instance", kind),
            _require(record, "version", kind),
            _require(record, "step", kind),
            _require(record, "id", kind),
            record.get("reason"),
        )
    if kind == "claim":
        return coordinator.claim(
            _require(record, "instance", kind),
            _require(record, "version", kind),
            _require(record, "step", kind),
            _require(record, "attempt", kind),
        )
    if kind == "settle":
        return coordinator.settle(
            _require(record, "attempt", kind),
            _require(record, "state", kind),
        )
    if kind == "migrate":
        return coordinator.request_migration(
            _require(record, "instance", kind),
            _require(record, "request", kind),
            _require(record, "to", kind),
        )
    if kind == "restart":
        return coordinator.restart()
    if kind == "inspect":
        return coordinator.inspect(_require(record, "instance", kind))
    raise ValueError(f"未登记的消息名称：{kind}")


def render_summary(results: list[dict[str, Any]]) -> str:
    """给值班员的紧凑文本摘要，重放 CLI 与演示脚本共用。"""
    lines = []
    for item in results:
        head = f"[{item['at']:>3}] {item['kind']:<10}"
        if not item["accepted"]:
            lines.append(f"{head} REJECTED {item['error']['type']}："
                         f"{item['error']['message']}")
            continue
        record = item["record"]
        if item["kind"] == "inspect":
            lines.append(f"{head} 实例 {record['instance']}："
                         f"启动版 {record['startup_version']}，"
                         f"生效版 {record['effective_version']}，"
                         f"动力 {record['drive']}，"
                         f"等待 {len(record['waiting_evidence'])} 项证据")
            for wait in record["waiting_evidence"]:
                lines.append(
                    f"        - 等 {wait['waiting_for']}：步骤 {wait['step']}"
                    f"（{wait['status']}）"
                )
            if record["active_transfer"]:
                t = record["active_transfer"]
                lines.append(f"        未决转移 {t['request_id']} -> "
                             f"v{t['to_version']}，blocker "
                             f"{len(t['blockers'])} 个")
        elif item["kind"] == "restart":
            lines.append(f"{head} 恢复实例 {record['recovered_instances']} 个")
        elif item["kind"] == "migrate":
            if item.get("blocked"):
                lines.append(f"{head} 候选已挂起（待人工/留旧版）："
                             f"{record['error']['message']}")
            else:
                t = record["transfer"]
                lines.append(f"{head} 转移 {t['request_id']} 已 {t['state']} -> "
                             f"v{t['to_version']}")
        elif item["kind"] == "signal":
            tag = "重复" if record.get("duplicate") else (
                "已采用" if record.get("applied") else "仅入账")
            lines.append(f"{head} {tag}：{record['step']}#{record['signal_id']}")
        elif item["kind"] == "settle":
            lines.append(f"{head} attempt {record['attempt']} -> "
                         f"{record['state']}")
        else:
            lines.append(f"{head} {json.dumps(record, ensure_ascii=False)}")
    return "\n".join(lines)
