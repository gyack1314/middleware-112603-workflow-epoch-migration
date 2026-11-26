# 作业工作流版本的在线接力与人工信号归属

流程版本内容不可变。map 中每个旧步骤只映射到给出的新版步骤，kind 为 signal 或 effect。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **definition**　提交 version 和 steps，每步包含 name、kind、needs。
- **compat**　声明 from/to 的 steps 字段映射。
- **start**　用 version 打开 instance。
- **signal**　按 instance/version/step 接收信号及 id（可带 reason）。
- **claim**　执行端以 attempt 领取 effect。
- **migrate**　尝试迁移 instance，为 request 指定 to。
- **settle**　执行端回报 attempt 的 done、never_started 或 unknown。
- **restart**　服务重启。
- **inspect**　查询实例。

## 示例

- split-approval.jsonl：一步人工确认拆成检查/发布两步，旧信号只能满足其中一项。
- in-flight-effect-at-migration.jsonl：转版时 effect 已被领取，须等结局明确。
- reason-gated-partial-mapping.jsonl：扩展 compat（reason/reuse）驱动的部分映射。

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 协调服务（workflow_coordinator）

`workflow_coordinator/` 在消息契约之上实现可持久化的版本协调服务，全部只用标准库，
执行均为本地合成。

### 核心不变量

- **版本与映射不可变**：`definition`、`compat` 重复提交会被拒；compat 支持简写
  （`"旧步骤": "新步骤"`）与扩展形式 `{step, reuse, reason, alternatives}`，
  `reuse` 取 `full`/`partial`/`none`。
- **实例启动即定版**：`start` 必须显式给出版本；新版发布不影响已启动的旧实例，
  旧实例继续按原契约领取与回执，直到显式转版。
- **信号去重**：按 实例/版本/步骤/信号 id 识别，重复提交返回首次序号；原因 `reason`
  随信号显式登记，部分映射必须核对原因。
- **两阶段转版，无半版本**：`migrate` 先落 `proposed` 候选（动力暂停、未领取的
  queued 工作项取消），blocker 全部解除后才在单事务内 `committed`。blocker 包括
  必要输入缺失（`missing_input`）、替代映射歧义（`ambiguous_alternatives`）、
  在途承诺未决（`in_flight_commitment`）、部分映射原因不符（`reason_mismatch`）。
  挂起时实例继续保留在旧版，值班员可 `abort` 明确留旧。
- **单一旧信号不能完成两项确认**：拆分场景下 `partial` 映射只满足被声明的那一步，
  新版另一步必须收到新信号；候选期间新版信号可预入账本用于解除 blocker。
- **在途 effect 的承诺语义**：attempt 永久绑定领取时的版本与步骤，迟到回执仍按当时
  语义解释。`done` 只记录不重做；`never_started` 释放承诺；`unknown` 暂停动力直到
  结局明确。已对外生效的结果在迁移后保留，不会重新派发。
- **并发与重启**：同一实例同时只允许一个 proposed 转移（数据库部分唯一索引兜底），
  两个迁移请求只有一个接管；状态全部落 SQLite（WAL），重启后仍能区分候选转移与
  当前真实生效版本。

### 模块

- `engine.py`：协调引擎（定义/映射、实例账本、信号、领取回执、两阶段转版、inspect）。
- `store.py`：SQLite 持久层与单调事件序号。
- `replay.py`：把九类 JSONL 消息按序重放到引擎，结构化返回每条结果。
- `http_server.py`：标准库 `http.server` 适配的项目接口（见下表）。
- `run_replay.py`：演练重放 CLI。

### 运行

    # 重放单个或整目录演练（目录模式下每个文件各开独立内存库/磁盘库）
    python3 -m workflow_coordinator.run_replay examples
    python3 -m workflow_coordinator.run_replay examples/split-approval.jsonl \
        --db /tmp/demo.db

    # HTTP 项目接口，供不同版本的执行端领取/回执
    python3 -m workflow_coordinator.http_server --db /tmp/wf.db --port 8080

| 方法与路径 | 对应消息 | 说明 |
| --- | --- | --- |
| POST /definitions | definition | 登记不可变流程版本 |
| POST /compat | compat | 登记两版步骤映射 |
| POST /instances | start | 按指定版本启动实例 |
| POST /signals | signal | 提交人工信号（含 reason） |
| POST /claims | claim | 执行端领取 effect |
| POST /receipts | settle | 执行端回执 done/never_started/unknown |
| POST /transfers | migrate | 请求转版；挂起返回 423，冲突返回 409 |
| POST /transfers/abort | — | 放弃候选转移，恢复旧版运行 |
| POST /restart | restart | 从盘上恢复并区分候选/生效版本 |
| GET /instances?instance= | inspect | 等待证据、生效版本、保留原因一次查清 |

`inspect` 返回 `startup_version`（启动版本）、`effective_version`（当前生效版本）、
`drive`（running/paused/completed）、`waiting_evidence`（在等哪类证据）、
`active_transfer`（候选转移及 blocker）与 `retention`（旧版本被保留的全部留痕原因）。
