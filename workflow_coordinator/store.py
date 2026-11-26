"""SQLite 持久层：所有协调状态都落盘，重启后可区分候选转移与生效版本。

只使用标准库。连接以序列化方式使用，配合引擎层的线程锁即可支撑并发请求；
单实例活跃转移另由部分唯一索引在数据库层面兜底。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 流程版本不可变：同一 version 只能提交一次定义。
CREATE TABLE IF NOT EXISTS definitions (
    version INTEGER PRIMARY KEY,
    steps_json TEXT NOT NULL,
    seq INTEGER NOT NULL
);

-- compat 声明同样不可变，from/to 唯一。
CREATE TABLE IF NOT EXISTS compat (
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    mapping_json TEXT NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY (from_version, to_version)
);

CREATE TABLE IF NOT EXISTS instances (
    instance TEXT PRIMARY KEY,
    startup_version INTEGER NOT NULL,
    effective_version INTEGER NOT NULL,
    drive TEXT NOT NULL,              -- running | paused | completed
    created_seq INTEGER NOT NULL
);

-- 实例经历过的每个版本各有一组步骤行，转版后旧行作为证据账本保留。
CREATE TABLE IF NOT EXISTS instance_steps (
    instance TEXT NOT NULL,
    version INTEGER NOT NULL,
    step TEXT NOT NULL,
    kind TEXT NOT NULL,
    needs_json TEXT NOT NULL,
    state TEXT NOT NULL,             -- waiting | satisfied
    satisfied_by TEXT,
    PRIMARY KEY (instance, version, step)
);

-- 人工信号去重：实例 + 版本 + 步骤 + 明示原因/信号 id。
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance TEXT NOT NULL,
    version INTEGER NOT NULL,
    step TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    reason TEXT,
    seq INTEGER NOT NULL,
    UNIQUE (instance, version, step, signal_id)
);

CREATE TABLE IF NOT EXISTS attempts (
    attempt TEXT PRIMARY KEY,
    instance TEXT NOT NULL,
    version INTEGER NOT NULL,
    step TEXT NOT NULL,
    state TEXT NOT NULL,             -- claimed | done | never_started | unknown
    claimed_seq INTEGER NOT NULL,
    settled_seq INTEGER
);

-- 每个待执行 effect 的工作项：queued 可在转版准备时取消，claimed 必须等结局。
CREATE TABLE IF NOT EXISTS dispatches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance TEXT NOT NULL,
    version INTEGER NOT NULL,
    step TEXT NOT NULL,
    state TEXT NOT NULL,             -- queued | claimed | done | canceled
    attempt TEXT,
    seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transfers (
    request_id TEXT PRIMARY KEY,
    instance TEXT NOT NULL,
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    state TEXT NOT NULL,             -- proposed | committed | rejected | aborted
    plan_json TEXT NOT NULL,
    blockers_json TEXT NOT NULL,
    created_seq INTEGER NOT NULL,
    decided_seq INTEGER
);

-- 同一实例同时只允许一个未决（proposed）转移，两个并发请求只有一个能接管。
CREATE UNIQUE INDEX IF NOT EXISTS one_active_transfer
    ON transfers(instance) WHERE state = 'proposed';

-- 旧版本被保留下来的原因、转移过程中的每个结论都进留痕表，供值班查询。
CREATE TABLE IF NOT EXISTS retention (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance TEXT NOT NULL,
    seq INTEGER NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}'
);
"""


class Store:
    """薄封装：集中管理连接、单调序号与行字典化。"""

    def __init__(self, path: str | Path = ":memory:"):
        # check_same_thread=False 后由引擎锁保证线程安全。
        self._conn = sqlite3.connect(
            str(path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    # -- 单调事件序号，重启后继续递增，所有状态变更都带序号，便于审计 ----------------
    def next_seq(self) -> int:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES('seq', "
            "COALESCE((SELECT value FROM meta WHERE key='seq'), '0')) "
            "ON CONFLICT(key) DO UPDATE SET value = value + 1"
        )
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key='seq'"
        ).fetchone()
        return int(row["value"])

    # -- 小工具 -------------------------------------------------------------------
    def begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self._conn.execute("COMMIT")

    def rollback(self) -> None:
        self._conn.execute("ROLLBACK")
