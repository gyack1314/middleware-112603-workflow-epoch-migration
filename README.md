# 作业工作流版本的在线接力与人工信号归属

流程版本内容不可变。map 中每个旧步骤只映射到给出的新版步骤，kind 为 signal 或 effect。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **definition**　提交 version 和 steps，每步包含 name、kind、needs。
- **compat**　声明 from/to 的 steps 字段映射。
- **start**　用 version 打开 instance。
- **signal**　按 instance/version/step 接收信号及 id。
- **claim**　执行端以 attempt 领取 effect。
- **migrate**　尝试迁移 instance，为 request 指定 to。
- **settle**　执行端回报 attempt 的 done、never_started 或 unknown。
- **restart**　服务重启。
- **inspect**　查询实例。

## 示例

- split-approval.jsonl
- in-flight-effect-at-migration.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
