"""标准库 JSON 结构化日志能力。"""

from __future__ import annotations

import json
import logging
import re
import sys
import traceback as traceback_module
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from types import TracebackType
from typing import Any

LOG_CONTEXT: ContextVar[dict[str, str]] = ContextVar("log_context", default={})
CONTEXT_FIELDS = ("request_id", "message_id", "lead_id", "record_id", "wecom_user_id")
STRUCTURED_EXTRA_FIELDS = (
    "path",
    "method",
    "status_code",
    "target",
    "smart_table_adapter",
    "readiness_status",
    "readiness_issue_count",
    "duration_ms",
    "error_type",
    "error_traceback",
    "ai_trace_id",
    "ai_status",
    "ai_call_count",
    "ai_input_tokens",
    "ai_output_tokens",
    # 企业微信接入状态字段经过类型限制，不包含 SDK 原始帧、正文或凭据。
    "accepted",
    "attempt",
    "duplicate",
    "has_reason",
)
_LOG_EMAIL_PATTERN = re.compile(r"(?P<local>[^\s@]+)@(?P<domain>[^\s@]+)")
_LOG_PHONE_PATTERN = re.compile(r"(?<!\d)(?P<number>\+?[0-9][0-9 -]{6,22}[0-9])(?!\d)")
_LOG_CREDENTIAL_PATTERN = re.compile(
    r"(?i)(?:密码|口令|验证码|secret|credential|cookie|password|token|api[_-]?key)"
    r"\s*[:=：]?\s*[^\s；;，,]+"
)
_TRACEBACK_FRAME_PATTERN = re.compile(r'^\s*File ".+", line \d+, in .+$')


class ContextFilter(logging.Filter):
    """将当前请求上下文附加到每一条日志记录。"""

    def filter(self, record: logging.LogRecord) -> bool:
        """补齐约定链路字段，使日志消费者无需处理缺失键。"""
        context = LOG_CONTEXT.get()
        for field in CONTEXT_FIELDS:
            # HTTP/Worker 上下文优先；没有上下文时保留领域服务通过 extra
            # 提供的 request_id/lead_id/record_id，避免管理操作丢失链路字段。
            if field in context:
                setattr(record, field, context[field])
            elif not hasattr(record, field):
                setattr(record, field, None)
        return True


class JsonFormatter(logging.Formatter):
    """将日志记录序列化为 stdout/stderr 可直接采集的 JSON。"""

    def __init__(self, environment: str = "development", service: str = "app") -> None:
        """保存每条日志共用的运行环境与服务名称。

        参数：environment 为部署环境，service 为当前进程服务名称。
        返回值：无。
        异常：无。
        副作用：后续格式化日志会包含稳定的环境和服务字段。
        """
        super().__init__()
        self._environment = environment
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        """保留安全的日志元数据与链路字段，避免自动输出敏感配置。"""
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "environment": self._environment,
            "service": self._service,
            "logger": record.name,
            "message": _redact_log_text(record.getMessage()),
        }
        for field in CONTEXT_FIELDS:
            payload[field] = getattr(record, field, None)
        # 仅白名单化输出业务观测字段，避免将外部响应或敏感 Payload 自动写入日志。
        for field in STRUCTURED_EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                # readiness 传入的 traceback 可能包含源码行，统一裁剪为安全的栈帧摘要。
                payload[field] = (
                    _safe_traceback_text(str(value))
                    if field == "error_traceback"
                    else value
                )
        if record.exc_info:
            # 保留文件、行号和异常类型，移除异常消息与源码行，防止 traceback 携带正文或凭据。
            payload["exception"] = _safe_exception_traceback(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def _redact_log_text(value: str) -> str:
    """脱敏普通日志消息中的邮箱、电话号码和凭据片段。

    参数：value 为待写入日志的文本。
    返回值：隐藏邮箱、电话和凭据值后的文本。
    异常：无。
    副作用：无，不修改传入字符串。
    """
    redacted = _LOG_CREDENTIAL_PATTERN.sub("[已遮蔽凭据]", value)
    redacted = _LOG_EMAIL_PATTERN.sub("[已遮蔽邮箱]", redacted)
    return _LOG_PHONE_PATTERN.sub("[已遮蔽电话]", redacted)


def _safe_exception_traceback(
    exc_info: tuple[
        type[BaseException] | None,
        BaseException | None,
        TracebackType | None,
    ],
) -> str:
    """保留异常调用栈结构但不输出异常正文、源码行或局部变量。

    参数：exc_info 为 logging 捕获的异常类型、实例和 traceback 三元组。
    返回值：仅含安全栈帧和异常类型的文本摘要。
    异常：无；异常链循环会被安全终止。
    副作用：无，不读取或修改异常对象。
    """
    exception_type, exception, traceback_obj = exc_info
    lines = ["Traceback (most recent call last):"]
    if traceback_obj is not None:
        for frame in traceback_module.extract_tb(traceback_obj):
            # 文件路径和函数名也经过同一规则处理，避免异常栈元数据携带凭据片段。
            lines.append(
                _redact_log_text(
                    f'  File "{frame.filename}", line {frame.lineno}, in {frame.name}'
                )
            )

    current: BaseException | None = exception
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        lines.append(f"{type(current).__name__}: [异常详情已隐藏]")
        if current.__cause__ is not None:
            current = current.__cause__
        elif not current.__suppress_context__:
            current = current.__context__
        else:
            current = None
    if exception is None:
        type_name = exception_type.__name__ if exception_type is not None else "UnknownException"
        lines.append(f"{type_name}: [异常详情已隐藏]")
    return "\n".join(lines)


def _safe_traceback_text(value: str) -> str:
    """将外部传入的 traceback 文本裁剪为不含源码行的安全栈帧摘要。

    参数：value 为 readiness 或其他结构化日志字段中的 traceback 文本。
    返回值：仅保留文件、行号和函数名的 traceback 摘要；没有栈帧时返回固定提示。
    异常：无。
    副作用：无，不将原始 traceback 写入日志。
    """
    # format_tb 每个栈帧后会附带一行源码；这里只保留可定位问题的栈帧结构。
    frames = [
        _redact_log_text(line)
        for line in value.splitlines()
        if _TRACEBACK_FRAME_PATTERN.match(line)
    ]
    if not frames:
        return "Traceback [调用栈不可用]"
    return "\n".join(["Traceback (most recent call last):", *frames])


def bind_log_context(**values: str | None) -> Token[dict[str, str]]:
    """将非空链路字段绑定到当前异步上下文并返回复位令牌。"""
    context = LOG_CONTEXT.get().copy()
    context.update({key: value for key, value in values.items() if value is not None})
    return LOG_CONTEXT.set(context)


def reset_log_context(token: Token[dict[str, str]]) -> None:
    """在请求完成后恢复此前日志上下文，防止请求间串联。"""
    LOG_CONTEXT.reset(token)


def configure_logging(level: str, environment: str = "development", service: str = "app") -> None:
    """配置进程根日志为包含环境和服务字段的 JSON Handler。

    参数：level 为日志等级，environment 为部署环境，service 为进程服务名称。
    返回值：无。
    异常：无。
    副作用：首次调用替换根日志 Handler，重复调用复用既有 Handler。
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())

    # 重复导入应用时复用已配置 Handler，避免日志被重复输出。
    if any(getattr(handler, "_crm_json_handler", False) for handler in root_logger.handlers):
        return

    handler = logging.StreamHandler(sys.stdout)
    handler._crm_json_handler = True  # type: ignore[attr-defined]
    handler.addFilter(ContextFilter())
    handler.setFormatter(JsonFormatter(environment=environment, service=service))
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
