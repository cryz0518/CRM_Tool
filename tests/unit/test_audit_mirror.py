"""业务审计事实到 Smart Table 管理员镜像子表的测试。"""

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.messaging.models import AuditMirrorOutbox, Base, BusinessAuditEvent, utc_now
from app.smart_table.audit import SmartTableAuditSink, build_mock_audit_schema
from app.smart_table.mock import MockSmartTableAdapter
from workers import tasks


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """创建包含审计镜像模型的隔离内存数据库。"""
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


def _persist_event(factory: sessionmaker[Session]) -> int:
    """持久化一条含安全字段和应被丢弃字段的审计事件，并返回镜像任务 ID。"""
    with factory.begin() as session:
        session.add(
            BusinessAuditEvent(
                message_id="message-1",
                sales_user_id="sales-1",
                event_type="crm_submit_result",
                details={
                    "lead_id": "lead-1",
                    "smart_table_record_id": "record-1",
                    "operation": "create",
                    "status": "succeeded",
                    "missing_fields": ["业务线"],
                    "phone": "13800000000",
                    "email": "customer@example.com",
                    "raw_response": "do-not-mirror",
                },
            )
        )
    with factory() as session:
        outbox = session.scalar(select(AuditMirrorOutbox))
        assert outbox is not None
        return outbox.id


def test_business_audit_event_creates_one_unique_mirror_job(
    session_factory: sessionmaker[Session],
) -> None:
    """验证审计事实提交后自动生成稳定唯一的镜像任务。"""
    outbox_id = _persist_event(session_factory)
    with session_factory() as session:
        rows = session.scalars(select(AuditMirrorOutbox)).all()
        assert len(rows) == 1
        assert rows[0].id == outbox_id
        assert rows[0].mirror_key == "audit:1"
        assert rows[0].status == "pending"


def test_audit_mirror_retry_is_idempotent_and_drops_sensitive_details(
    session_factory: sessionmaker[Session],
) -> None:
    """验证镜像重试不重复写行，且不写入手机号、邮箱或原始外部响应。"""
    outbox_id = _persist_event(session_factory)
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)
    with session_factory() as session:
        event = session.scalar(select(BusinessAuditEvent))
        assert event is not None
        first = SmartTableAuditSink(adapter).mirror(event)
    with session_factory.begin() as update_session:
        outbox = update_session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        outbox.status = "retrying"
    with session_factory() as session:
        event = session.scalar(select(BusinessAuditEvent))
        assert event is not None
        second = SmartTableAuditSink(adapter).mirror(event)

    assert first.record_id == second.record_id
    assert len(adapter.get_records()) == 1
    fields = adapter.get_records()[0].fields
    assert fields["lead_id"] == "lead-1"
    assert fields["missing_fields"] == "业务线"
    assert "phone" not in fields
    assert "email" not in fields
    assert "raw_response" not in fields


def test_audit_mirror_failure_only_retries_mirror_job(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 Smart Table 镜像失败只更新镜像 Outbox，不回滚业务审计事实。"""
    outbox_id = _persist_event(session_factory)

    class FailingAdapter:
        """只用于证明外部审计子表失败不会改变业务审计状态。"""

        def get_schema(self):
            """模拟审计子表外部调用失败。"""
            raise RuntimeError("external body must not be persisted")

    engine = session_factory.kw["bind"]
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(tasks, "_session_factory", lambda: (engine, session_factory))
    monkeypatch.setattr(tasks, "get_smart_table_audit_adapter", FailingAdapter)

    assert tasks.consume_audit_mirror_outbox.run(outbox_id) == "retrying"
    with session_factory() as session:
        assert session.scalar(select(BusinessAuditEvent)) is not None
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "retrying"
        assert outbox.claim_token is None
        assert outbox.failure_category == "retryable"
        assert outbox.failure_code == "RuntimeError"


def test_stale_audit_mirror_worker_cannot_finalize_new_claim(
    session_factory: sessionmaker[Session],
) -> None:
    """验证旧 Worker 的 claim token 不能覆盖过期接管后的新 Worker 状态。"""
    outbox_id = _persist_event(session_factory)

    token_a = tasks._claim_audit_mirror_outbox(session_factory, outbox_id)
    assert token_a is not None
    with session_factory.begin() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        outbox.processing_started_at = utc_now() - timedelta(days=1)

    token_b = tasks._claim_audit_mirror_outbox(session_factory, outbox_id)
    assert token_b is not None
    assert token_b != token_a

    tasks._finish_audit_mirror(
        session_factory,
        outbox_id,
        token_a,
        succeeded=False,
        error=RuntimeError("stale worker"),
    )
    with session_factory() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "processing"
        assert outbox.claim_token == token_b

    tasks._finish_audit_mirror(session_factory, outbox_id, token_a, succeeded=True)
    with session_factory() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "processing"
        assert outbox.claim_token == token_b

    tasks._finish_audit_mirror(session_factory, outbox_id, token_b, succeeded=True)
    with session_factory() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "succeeded"
        assert outbox.claim_token is None
