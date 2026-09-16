"""T15 ConsoleQueryService 和 ConflictProjection 的公开 Seam 测试。"""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.console.dto import ConsoleHealthStatusDTO
from app.console.models import AIExecutionRecord, BreakGlassAccessAudit
from app.console.queries import ConsoleQueryService
from app.leads.models import (
    CrmSyncRecord,
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    MessageRetryAttempt,
)
from app.messaging.models import (
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    OutboxEvent,
    SalesAuthorization,
)


class FakeHealthProvider:
    """为 Query Service 提供确定性的依赖健康状态。"""

    def snapshot(self) -> list[ConsoleHealthStatusDTO]:
        """返回全部 Console 依赖的模拟健康结果。"""
        now = datetime.now(UTC)
        return [
            ConsoleHealthStatusDTO(name="postgres", status="ok", checked_at=now),
            ConsoleHealthStatusDTO(name="redis", status="ok", checked_at=now),
            ConsoleHealthStatusDTO(
                name="worker", status="unknown", detail="未接入心跳", checked_at=now
            ),
            ConsoleHealthStatusDTO(name="wecom", status="ok", checked_at=now),
        ]


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供包含 T15 新模型的隔离数据库。"""
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


@pytest.fixture
def seeded_session_factory(session_factory: sessionmaker[Session]) -> sessionmaker[Session]:
    """写入覆盖 T14 失败、公司冲突、人工修改和敏感字段的测试事实。"""
    timestamp = datetime(2026, 9, 16, tzinfo=UTC)
    with session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-1",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={"raw": "do-not-return"},
                normalized_text="联系人王验收，电话 13812345678，邮箱 alice@example.com",
                received_at=timestamp,
            )
        )
        session.add(
            Lead(
                id="lead-1",
                source_message_id="message-1",
                source_segment_index=0,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                lifecycle_state="company_identity_change_pending_review",
                field_values={
                    "线索名称": "星海验收科技",
                    "联系人": "王验收",
                    "手机": "13812345678",
                    "邮箱": "alice@example.com",
                },
                enrichment_values={"客户需求/痛点": "需要原始需求隐藏"},
                standard_company_name="星海验收科技",
                company_region="domestic",
                company_verification_status="verification_conflict",
                company_confirmed_by_user=False,
                updated_at=timestamp,
            )
        )
        session.add(
            LeadFieldProvenance(
                lead_id="lead-1",
                source_message_id="message-1",
                field_name="联系人",
                value="王验收",
                last_ai_synced_value="李旧值",
                is_user_modified=True,
                is_user_confirmed=True,
                created_at=timestamp,
            )
        )
        session.add(
            OutboxEvent(
                message_id="message-1",
                sales_user_id="sales-1",
                sequence=1,
                status="failed_pending_review",
                attempts=2,
                failure_category="permanent",
                failure_summary="smart_table_permission_denied",
                created_at=timestamp,
            )
        )
        session.add(
            MessageRetryAttempt(
                message_id="message-1",
                segment_index=0,
                lead_id="lead-1",
                operator_user_id="sales-1",
                attempt_number=1,
                status="failed_pending_review",
                failure_category="unknown",
                error_summary="retry_failed alice@example.com",
                created_at=timestamp,
                completed_at=timestamp,
            )
        )
        session.add(
            CrmSyncRecord(
                lead_id="lead-1",
                operation="update",
                smart_table_record_id="record-1",
                idempotency_key="crm-update-1",
                canonical_payload={"线索名称": "星海验收科技"},
                snapshot_hash="hash-1",
                request_message_id="message-1",
                submitting_sales_user_id="sales-1",
                submitting_crm_user_id="crm-1",
                status="unknown",
                attempts=3,
                failure_category="unknown",
                failure_summary="crm_timeout",
                created_at=timestamp,
            )
        )
        session.add(
            LeadMessageResolution(
                message_id="message-1",
                segment_index=0,
                lead_id=None,
                status="unassigned",
                created_at=timestamp,
            )
        )
        session.add(
            AIExecutionRecord(
                trace_id="trace-1",
                message_id="message-1",
                lead_id="lead-1",
                operation="extract_fields",
                provider="MockLLMProvider",
                status="succeeded",
                created_at=timestamp,
            )
        )
        session.add(
            BusinessAuditEvent(
                message_id="message-1",
                sales_user_id="sales-1",
                event_type="discard_request_not_effective",
                details={"raw": "do-not-return"},
                created_at=timestamp,
            )
        )
    return session_factory


def test_query_service_returns_masked_dtos_without_raw_message_payload(
    seeded_session_factory: sessionmaker[Session],
) -> None:
    """验证消息和线索查询只返回脱敏 DTO，不泄露原始内容。"""
    service = ConsoleQueryService(seeded_session_factory, FakeHealthProvider())

    messages = service.list_messages(limit=10)
    leads = service.list_leads(limit=10)

    assert len(messages.items) == 1
    message = messages.items[0]
    assert message.text_summary is not None
    assert "13812345678" not in message.text_summary
    assert "alice@example.com" not in message.text_summary
    assert "raw_payload" not in message.model_dump()
    assert message.has_raw_payload is True

    lead = leads.items[0]
    assert lead.masked_field_values["手机"] == "138****5678"
    assert lead.masked_field_values["邮箱"] == "a***@example.com"
    assert "enrichment_values" not in lead.model_dump()


def test_conflict_projection_covers_identity_verification_user_edit_and_t14_failure(
    seeded_session_factory: sessionmaker[Session],
) -> None:
    """验证 T14 失败、废弃竞态和公司/人工冲突都进入 Console 投影。"""
    service = ConsoleQueryService(seeded_session_factory, FakeHealthProvider())

    conflicts = service.list_conflicts(limit=100)
    kinds = {item.conflict_kind for item in conflicts.items}

    assert "company_identity" in kinds
    assert "company_verification" in kinds
    assert "user_modified_field" in kinds
    assert "task_failure" in kinds
    assert sum(item.conflict_kind == "task_failure" for item in conflicts.items) >= 3
    assert "discard_request_not_effective" in kinds
    assert all(item.detail != "do-not-return" for item in conflicts.items)


def test_query_service_limits_pages_and_exposes_ai_metadata_only(
    seeded_session_factory: sessionmaker[Session],
) -> None:
    """验证查询有数量上限，并且 AI 页面不出现 Prompt/response 字段。"""
    service = ConsoleQueryService(seeded_session_factory, FakeHealthProvider())

    tasks = service.list_tasks(limit=1)
    ai_records = service.list_ai_executions(limit=10)

    assert len(tasks.items) == 1
    assert all("alice@example.com" not in (item.error_summary or "") for item in tasks.items)
    assert len(ai_records.items) == 1
    assert "prompt" not in ai_records.items[0].model_dump()
    assert "response" not in ai_records.items[0].model_dump()


def test_audit_and_config_queries_keep_security_facts_and_mapping_status(
    session_factory: sessionmaker[Session],
) -> None:
    """验证审计页保留 Break-glass 安全事实，配置页展示授权映射风险。"""
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-1",
                is_authorized=True,
                is_active=True,
                crm_user_id=None,
            )
        )
        session.add(
            BreakGlassAccessAudit(
                access_id="access-1",
                request_id="request-1",
                operator_subject="admin-1",
                operator_role="administrator",
                auth_source="development_token",
                object_type="message",
                object_id="message-1",
                access_type="view_raw_message",
                reason="核对异常消息 alice@example.com",
                phase="completed",
                outcome="succeeded",
                data_returned=True,
                request_context={"route": "/api/console/break-glass/access", "secret": "隐藏"},
            )
        )

    service = ConsoleQueryService(session_factory, FakeHealthProvider())
    audits = service.list_audits(limit=10)
    config = service.list_config()

    audit = next(item for item in audits.items if item.audit_kind == "break_glass")
    assert audit.operator_role == "administrator"
    assert audit.access_type == "view_raw_message"
    assert audit.reason == "核对异常消息 a***@example.com"
    assert audit.data_returned is True
    assert audit.request_context == {"route": "/api/console/break-glass/access"}
    mapping = next(item for item in config.items if item.source == "sales_authorization")
    assert mapping.status == "not_ready"
    assert "CRM 映射缺失 1 条" in mapping.issues[0]
