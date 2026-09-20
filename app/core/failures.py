"""外部任务失败分类与安全错误摘要工具。"""

from __future__ import annotations

import re
from enum import StrEnum


class TaskFailureCategory(StrEnum):
    """描述任务失败是否可能因外部暂态问题恢复。"""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"


class RetryableTaskFailure(RuntimeError):
    """标记已经由适配器确认属于可安全重试的暂态故障。"""


class PermanentTaskFailure(RuntimeError):
    """标记已经由适配器或业务层确认不可自动恢复的故障。"""


def classify_task_failure(error: BaseException) -> TaskFailureCategory:
    """按确定性异常类型分类任务失败，不从异常文本猜测业务事实。

    参数：error 为外部适配器或任务执行抛出的异常。
    返回值：transient、permanent 或 unknown 失败类别。
    异常：无；未知异常保守归入 unknown。
    副作用：无。
    """
    # 适配器和业务层的显式标记优先于 Python 通用异常基类。
    if isinstance(error, PermanentTaskFailure):
        return TaskFailureCategory.PERMANENT
    if isinstance(error, RetryableTaskFailure):
        return TaskFailureCategory.TRANSIENT
    # 参数、权限和业务校验失败即使重复执行也不会自行恢复。
    if isinstance(error, (ValueError, PermissionError, LookupError)):
        return TaskFailureCategory.PERMANENT
    # 只有明确的网络、连接和进程层异常才具备安全重试价值。
    # PermissionError 必须先于 OSError 判断，否则会被错误地归类为暂态失败。
    if isinstance(error, (TimeoutError, ConnectionError, OSError)):
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


def safe_audit_text(value: str, *, max_length: int = 512) -> str:
    """规范化管理原因，移除控制字符并遮蔽联系方式和凭据。"""

    normalized = " ".join(value.split())[:max_length]
    normalized = re.sub(
        r"(?i)(?:password|token|secret|密码|口令)\s*[:：]?\s*\S+",
        "[已遮蔽]",
        normalized,
    )
    normalized = re.sub(r"[^\s@]+@[^\s@]+", "[已遮蔽邮箱]", normalized)
    normalized = re.sub(r"(?<!\d)\+?[0-9][0-9 -]{6,22}[0-9](?!\d)", "[已遮蔽电话]", normalized)
    return normalized


def validate_request_id(value: str) -> str:
    """校验并返回可安全写入日志和审计的幂等请求标识。"""

    normalized = value.strip()
    if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", normalized) is None:
        raise ValueError("管理写操作必须填写合法 request_id")
    return normalized
