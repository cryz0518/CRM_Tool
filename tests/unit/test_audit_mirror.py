"""业务审计事实到 Smart Table 管理员镜像子表的测试。"""

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.messaging.models import (
    AuditMirrorOutbox,
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    SalesAuthorization,
    utc_now,
)
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


def _persist_event_with_message(
    factory: sessionmaker[Session],
    *,
    normalized_text: str | None,
    scrubbed_at=None,
) -> int:
    """持久化带标准化销售文本的消息和审计事件，并返回镜像任务 ID。"""
    with factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-1",
                is_authorized=False,
                is_active=True,
            )
        )
        session.add(
            IncomingMessage(
                message_id="message-1",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={
                    "body": {
                        "text": {
                            "content": normalized_text,
                            "phone": "13800000000",
                            "email": "customer@example.com",
                        }
                    }
                },
                normalized_text=normalized_text,
                scrubbed_at=scrubbed_at,
            )
        )
        session.add(
            BusinessAuditEvent(
                message_id="message-1",
                sales_user_id="sales-1",
                event_type="crm_submit_result",
                details={"status": "succeeded"},
            )
        )
    with factory() as session:
        outbox = session.scalar(select(AuditMirrorOutbox))
        assert outbox is not None
        return outbox.id


def _run_worker_with_adapter(
    factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    outbox_id: int,
    adapter: MockSmartTableAdapter,
) -> str:
    """用测试会话和 Mock adapter 执行一次审计镜像 Worker。"""
    engine = factory.kw["bind"]
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(tasks, "_session_factory", lambda: (engine, factory))
    monkeypatch.setattr(tasks, "get_smart_table_audit_adapter", lambda: adapter)
    return tasks.consume_audit_mirror_outbox.run(outbox_id)


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


def test_audit_mirror_uses_normalized_text_and_excludes_raw_payload(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证镜像只写标准化销售文本，不写 WeCom raw payload。"""
    message_text = "青岛远达物流的吴总，仓储中心想做AGV联动"
    outbox_id = _persist_event_with_message(
        session_factory,
        normalized_text=message_text,
    )
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"

    fields = adapter.get_records()[0].fields
    assert fields["销售原始消息"] == message_text
    assert "raw_payload" not in fields
    assert "13800000000" not in fields.values()
    assert "customer@example.com" not in fields.values()


@pytest.mark.parametrize(
    ("normalized_text", "scrubbed_at"),
    [
        (None, None),
        ("仍在对象中的文本", utc_now()),
    ],
)
def test_audit_mirror_omits_missing_or_scrubbed_original_message(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    normalized_text: str | None,
    scrubbed_at,
) -> None:
    """验证缺失文本或已 scrub 消息不会进入管理员审计镜像。"""
    outbox_id = _persist_event_with_message(
        session_factory,
        normalized_text=normalized_text,
        scrubbed_at=scrubbed_at,
    )
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert "销售原始消息" not in adapter.get_records()[0].fields


def test_audit_mirror_truncates_original_message_to_2000_chars(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证销售原始消息单独限制为最多 2000 个字符。"""
    message_text = "甲" * 2005
    outbox_id = _persist_event_with_message(
        session_factory,
        normalized_text=message_text,
    )
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    mirrored_message = adapter.get_records()[0].fields["销售原始消息"]
    assert isinstance(mirrored_message, str)
    assert mirrored_message == message_text[:2000]
    assert len(mirrored_message) <= 2000


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
