"""版本化工作流协调服务。

对外只暴露协调器与领域异常，存储、重放与 HTTP 适配分别在子模块中。
"""

from .engine import Coordinator
from .errors import (
    ConflictError,
    CoordinatorError,
    NotFoundError,
    TransferBlockedError,
    ValidationError,
)

__all__ = [
    "Coordinator",
    "CoordinatorError",
    "ConflictError",
    "NotFoundError",
    "ValidationError",
    "TransferBlockedError",
]
