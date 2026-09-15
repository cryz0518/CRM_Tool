"""外部任务失败分类与安全错误摘要工具。"""

from __future__ import annotations

from enum import StrEnum


class TaskFailureCategory(StrEnum):
    """描述任务失败是否可能因外部暂态问题恢复。"""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"


def classify_task_failure(error: BaseException) -> TaskFailureCategory:
    """按确定性异常类型分类任务失败，不从异常文本猜测业务事实。

    参数：error 为外部适配器或任务执行抛出的异常。
    返回值：transient、permanent 或 unknown 失败类别。
    异常：无；未知异常保守归入 unknown。
    副作用：无。
    """
    # 参数、权限和业务校验失败即使重复执行也不会自行恢复。
    if isinstance(error, (ValueError, PermissionError, LookupError)):
        return TaskFailureCategory.PERMANENT
    # 网络、连接、进程层失败具备重试价值；既有适配器以 RuntimeError 表示可恢复边界。
    # PermissionError 必须先于 OSError 判断，否则会被错误地归类为暂态失败。
    if isinstance(error, (TimeoutError, ConnectionError, OSError, RuntimeError)):
        return TaskFailureCategory.TRANSIENT
    # 未知异常保留给人工审查，避免错误的自动重试放大副作用。
    return TaskFailureCategory.UNKNOWN


def safe_failure_summary(error: BaseException) -> str:
    """返回不包含异常正文的有限失败摘要，供数据库和日志使用。

    参数：error 为已捕获的任务异常。
    返回值：异常类型名，长度受限且不包含 payload、凭据或响应正文。
    异常：无。
    副作用：无。
    """
    return type(error).__name__[:128]
