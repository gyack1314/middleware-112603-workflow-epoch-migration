"""命令行入口：重放一个演练目录或单个 JSONL 文件。

    python3 -m workflow_coordinator.run_replay examples/split-approval.jsonl \
        --db /tmp/demo.db
"""

from __future__ import annotations

import argparse
from pathlib import Path

from .engine import Coordinator
from .replay import render_summary, replay_file


def main() -> None:
    parser = argparse.ArgumentParser(description="工作流协调服务演练重放器")
    parser.add_argument("path", type=Path, help="JSONL 文件或包含 JSONL 的目录")
    parser.add_argument(
        "--db", default=":memory:",
        help="SQLite 路径前缀；目录模式下每个演练文件各开一个独立库，"
             "例如 --db /tmp/demo 生成 /tmp/demo-<文件名>.db",
    )
    args = parser.parse_args()

    paths = (
        sorted(args.path.glob("*.jsonl")) if args.path.is_dir() else [args.path]
    )
    if not paths:
        raise SystemExit("没有找到可重放的 JSONL 文件")
    for path in paths:
        # 每个演练是独立的参与方记录集合，协调器与账本按文件隔离。
        if args.db == ":memory:" or args.path.is_dir():
            db_path = (
                ":memory:"
                if args.db == ":memory:"
                else f"{args.db}-{path.stem}.db"
            )
        else:
            db_path = args.db
        coordinator = Coordinator(db_path)
        print(f"===== {path.name} =====")
        results = replay_file(coordinator, path)
        print(render_summary(results))
        coordinator.close()


if __name__ == "__main__":
    main()
