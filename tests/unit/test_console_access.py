"""T15 AI execution 与 Break-glass 访问 Seam 测试。"""

from __future__ import annotations

import json
from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ai.gateway import AIGateway
from app.ai.models import LLMResponse
from app.ai.persistence import AIExecutionRecorderEvent, DatabaseAIExecutionRecorder
from app.ai.provider import MockLLMProvider
from app.console.auth import AdminPrincipal, DevelopmentAdminIdentityProvider
from app.console.break_glass import (
    BreakGlassAccessError,
    BreakGlassAccessRequest,
    BreakGlassAccessService,
)
from app.console.models import AIExecutionRecord, BreakGlassAccessAudit
from app.media.storage import FakeStorageProvider
from app.messaging.models import Base, IncomingMessage, MessageAttachment, SalesAuthorization


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供隔离的 T15 SQLite 会话工厂。"""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    try:
        yield sessionmaker(engine)
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_ai_gateway_persists_execution_metadata_without_model_content(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 AI execution 只保存调用元数据，不保存 Prompt 或模型响应。"""
    analysis = json.dumps(
        {
            "intent": "NEW_LEAD",
            "customer_reference": {},
            "crm_fields": {},
            "enrichment": {},
            "confidence_by_field": {},
            "conflicts": [],
            "warnings": [],
        }
    )
    gateway = AIGateway(
        MockLLMProvider([LLMResponse(analysis, input_tokens=3, output_tokens=5)]),
        execution_recorder=DatabaseAIExecutionRecorder(session_factory),
    )

    gateway.extract_fields("原始客户 Prompt alice@example.com")

    with session_factory() as session:
        record = session.scalar(select(AIExecutionRecord))
    assert record is not None
    assert record.status == "succeeded"
    assert record.input_tokens == 3
    assert record.output_tokens == 5
    assert record.duration_ms is not None
    assert not hasattr(record, "prompt")
    assert not hasattr(record, "response")


def test_break_glass_requires_reason_audits_before_returning_raw_message(
    session_factory: sessionmaker[Session],
) -> None:
    """验证原始消息访问必须有原因，并在返回内容前先提交授权审计。"""
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True))
        session.add(
            SalesAuthorization(
                wecom_user_id="admin-1", is_authorized=True, is_active=True, is_administrator=True
            )
        )
        session.add(
            IncomingMessage(
                message_id="message-1",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={"secret": "customer-content"},
                normalized_text="客户原文",
            )
        )

    service = BreakGlassAccessService(session_factory)
    request = BreakGlassAccessRequest(
        principal=AdminPrincipal("admin-1", frozenset({"administrator"}), "test"),
        object_type="message",
        object_id="message-1",
        access_type="view_raw_message",
        reason="排查 T14 失败消息",
        request_id="request-1",
    )

    result = service.access(request)
    assert result.payload == {"secret": "customer-content"}

    with session_factory() as session:
        audits = session.scalars(
            select(BreakGlassAccessAudit).order_by(BreakGlassAccessAudit.created_at)
        ).all()
    assert [audit.phase for audit in audits] == ["granted", "completed"]
    assert audits[0].reason == "排查 T14 失败消息"

    service.access(request)
    with session_factory() as session:
        assert session.query(BreakGlassAccessAudit).count() == 4

    with pytest.raises(BreakGlassAccessError, match="必须填写原因"):
        service.access(
            request.__class__(
                principal=request.principal,
                object_type=request.object_type,
                object_id=request.object_id,
                access_type=request.access_type,
                reason=" ",
                request_id="request-2",
            )
        )


def test_break_glass_audit_failure_blocks_raw_access() -> None:
    """验证审计事务失败时不读取原始对象。"""

    class FailingSessionFactory:
        """模拟不可写审计数据库。"""

        def begin(self) -> object:
            """让审计事务在任何读取前失败。"""
            raise RuntimeError("audit database unavailable")

    service = BreakGlassAccessService(
        FailingSessionFactory(),
        capability_authorizer=DevelopmentAdminIdentityProvider(
            token=None, subject="admin-1", roles=frozenset({"administrator"})
        ),
    )  # type: ignore[arg-type]
    request = BreakGlassAccessRequest(
        principal=AdminPrincipal("admin-1", frozenset({"administrator"}), "test"),
        object_type="message",
        object_id="message-1",
        access_type="view_raw_message",
        reason="安全排查",
        request_id="request-3",
    )

    with pytest.raises(BreakGlassAccessError, match="审计失败"):
        service.access(request)


def test_database_ai_recorder_accepts_only_metadata_event(
    session_factory: sessionmaker[Session],
) -> None:
    """验证数据库 AI recorder 的公共输入不包含原始请求和响应字段。"""
    recorder = DatabaseAIExecutionRecorder(session_factory)
    recorder.record(
        AIExecutionRecorderEvent(
            trace_id="trace-1",
            operation="extract_fields",
            provider="MockLLMProvider",
            model=None,
            status="failed",
            error_type="AIGatewayError",
            error_summary="结构校验失败",
        )
    )

    with session_factory() as session:
        record = session.scalar(select(AIExecutionRecord))
    assert record is not None
    assert record.trace_id == "trace-1"
    assert record.error_summary == "结构校验失败"


def test_break_glass_attachment_returns_signed_url_without_bytes(
    session_factory: sessionmaker[Session],
    tmp_path,
) -> None:
    """验证附件 Break-glass 只返回签名地址，不把二进制装入服务响应。"""

    class FakeSignedURLProvider:
        """记录签名地址请求的测试存储适配器。"""

        def __init__(self) -> None:
            """初始化签名请求记录。"""
            self.calls: list[tuple[str, int, bool]] = []

        def create_signed_url(
            self,
            storage_key: str,
            *,
            expires_in_seconds: int,
            download: bool,
        ) -> str:
            """返回固定短期地址并记录下载意图。"""
            self.calls.append((storage_key, expires_in_seconds, download))
            return "https://storage.test/signed/opaque"

    storage = FakeStorageProvider(tmp_path)
    stored = storage.put(b"safe", suffix=".png")
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="admin-1", is_authorized=True, is_active=True, is_administrator=True
            )
        )
        session.add(
            IncomingMessage(
                message_id="message-attachment",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
                normalized_text="附件消息",
            )
        )
        session.add(
            MessageAttachment(
                id="attachment-1",
                message_id="message-attachment",
                media_kind="image",
                storage_key=stored.storage_key,
                detected_mime_type="image/png",
                scan_status="clean",
            )
        )

    provider = FakeSignedURLProvider()
    service = BreakGlassAccessService(
        session_factory, signed_url_provider=provider, storage_provider=storage
    )
    result = service.access(
        BreakGlassAccessRequest(
            principal=AdminPrincipal("admin-1", frozenset({"administrator"}), "test"),
            object_type="attachment",
            object_id="attachment-1",
            access_type="preview_attachment",
            reason="核对名片附件",
            request_id="request-attachment",
        )
    )

    assert result.signed_url == "https://storage.test/signed/opaque"
    assert not hasattr(result, "content")
    assert provider.calls == [(stored.storage_key, 300, False)]
