"""标准库 JSON 结构化日志能力。"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from typing import Any

LOG_CONTEXT: ContextVar[dict[str, str]] = ContextVar("log_context", default={})
CONTEXT_FIELDS = ("request_id", "message_id", "lead_id", "record_id", "wecom_user_id")


class ContextFilter(logging.Filter):
    """将当前请求上下文附加到每一条日志记录。"""

    def filter(self, record: logging.LogRecord) -> bool:
        """补齐约定链路字段，使日志消费者无需处理缺失键。"""
        context = LOG_CONTEXT.get()
        for field in CONTEXT_FIELDS:
            setattr(record, field, context.get(field))
        return True


class JsonFormatter(logging.Formatter):
    """将日志记录序列化为 stdout/stderr 可直接采集的 JSON。"""

    def format(self, record: logging.LogRecord) -> str:
        """保留安全的日志元数据与链路字段，避免自动输出敏感配置。"""
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in CONTEXT_FIELDS:
            payload[field] = getattr(record, field, None)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def bind_log_context(**values: str | None) -> Token[dict[str, str]]:
    """将非空链路字段绑定到当前异步上下文并返回复位令牌。"""
    context = LOG_CONTEXT.get().copy()
    context.update({key: value for key, value in values.items() if value is not None})
    return LOG_CONTEXT.set(context)


def reset_log_context(token: Token[dict[str, str]]) -> None:
    """在请求完成后恢复此前日志上下文，防止请求间串联。"""
    LOG_CONTEXT.reset(token)


def configure_logging(level: str) -> None:
    """配置进程根日志为单一 JSON stdout Handler，且允许重复调用。"""
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())

    # 重复导入应用时复用已配置 Handler，避免日志被重复输出。
    if any(getattr(handler, "_crm_json_handler", False) for handler in root_logger.handlers):
        return

    handler = logging.StreamHandler(sys.stdout)
    handler._crm_json_handler = True  # type: ignore[attr-defined]
    handler.addFilter(ContextFilter())
    handler.setFormatter(JsonFormatter())
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
