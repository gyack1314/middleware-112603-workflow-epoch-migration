"""协调服务的领域异常。"""


class CoordinatorError(Exception):
    """所有可预期的协调失败都继承它，重放层据此逐条记录而不中断流。"""


class ValidationError(CoordinatorError):
    """流程定义、映射或消息字段不满足契约。"""


class NotFoundError(CoordinatorError):
    """引用的版本、实例或 attempt 不存在。"""


class ConflictError(CoordinatorError):
    """请求与当前状态冲突，例如重复提交、不可变内容被改写。"""


class TransferBlockedError(CoordinatorError):
    """转版无法立即生效，候选转移已登记并附带 blocker 原因。"""
