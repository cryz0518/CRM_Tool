"""AI Gateway 的脱敏执行元数据持久化边界。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from sqlalchemy.orm import Session, sessionmaker

from app.console.models import AIExecutionRecord


@dataclass(frozen=True)
class AIExecutionRecorderEvent:
    """承载一次 AI 调用的非敏感运行元数据。"""

    trace_id: str
    operation: str
    provider: str
    model: str | None
    status: str
    message_id: str | None = None
    lead_id: str | None = None
    call_count: int | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    duration_ms: int | None = None
    error_type: str | None = None
    error_summary: str | None = None
    created_at: datetime | None = None
    completed_at: datetime | None = None


class AIExecutionRecorder(Protocol):
    """定义 AI Gateway 可选的执行元数据记录接口。"""

    def record(self, event: AIExecutionRecorderEvent) -> None:
        """持久化一次不含模型内容的 AI 执行记录。"""


class DatabaseAIExecutionRecorder:
    """使用独立短事务将 AI 执行元数据保存到 PostgreSQL 或测试数据库。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """注入数据库会话工厂。

        参数：session_factory 为短事务会话工厂。
        返回值：无。
        异常：无。
        副作用：不建立连接，直到 record 被调用。
        """
        self._session_factory = session_factory

    def record(self, event: AIExecutionRecorderEvent) -> None:
        """写入一条 AI 执行元数据，不接受原始 Prompt 或响应字段。

        参数：event 为已由 Gateway 过滤后的运行元数据。
        返回值：无。
        异常：数据库异常向调用方传播，由 Gateway 决定是否降级记录。
        副作用：新增一条 AI execution record。
        """
        values = {
            key: value
            for key, value in {
                "trace_id": event.trace_id,
                "operation": event.operation,
                "provider": event.provider,
                "model": event.model,
                "status": event.status,
                "message_id": event.message_id,
                "lead_id": event.lead_id,
                "call_count": event.call_count,
                "input_tokens": event.input_tokens,
                "output_tokens": event.output_tokens,
                "duration_ms": event.duration_ms,
                "error_type": event.error_type,
                "error_summary": event.error_summary,
                "created_at": event.created_at,
                "completed_at": event.completed_at,
            }.items()
            if value is not None
        }
        with self._session_factory.begin() as session:
            session.add(AIExecutionRecord(**values))
