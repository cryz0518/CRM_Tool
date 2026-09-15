"""T14 失败消息补充重试与受控废弃测试。"""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ai.gateway import BusinessValidationError, FailedStructuredOutputError
from app.core.failures import TaskFailureCategory, classify_task_failure
from app.leads.discard import LeadDiscardService, LeadDiscardStatus
from app.leads.models import (
    CrmCompanyIdentity,
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    MessageRetryAttempt,
)
from app.leads.service import (
    FirstTextLeadWorkspaceService,
    ProtectedSupplementStatus,
)
from app.messaging.models import (
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    OutboxEvent,
    SalesAuthorization,
)
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableRecord
from app.smart_table.registry import build_required_smart_table_schema
from app.smart_table.wecom_cli import (
    WecomCliProtocolError,
    WecomCliTransportError,
)


def test_failure_classification_keeps_permission_failures_permanent() -> None:
    """验证 PermissionError 不因继承 OSError 被误判为可重试暂态失败。"""
    assert classify_task_failure(PermissionError("denied")) is TaskFailureCategory.PERMANENT


def test_failure_classification_uses_explicit_adapter_and_gateway_categories() -> None:
    """验证真实适配器与 AI 网关异常不会依赖 RuntimeError 基类猜测重试策略。"""
    assert (
        classify_task_failure(WecomCliProtocolError("business response"))
        is TaskFailureCategory.PERMANENT
    )
    assert (
        classify_task_failure(WecomCliTransportError("network unavailable"))
        is TaskFailureCategory.TRANSIENT
    )
    assert (
        classify_task_failure(BusinessValidationError("invalid enum"))
        is TaskFailureCategory.PERMANENT
    )
    assert (
        classify_task_failure(FailedStructuredOutputError("raw", "repaired", "invalid"))
        is TaskFailureCategory.PERMANENT
    )
    assert classify_task_failure(RuntimeError("unclassified")) is TaskFailureCategory.UNKNOWN


def test_permanent_message_failure_skips_automatic_retry(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证智能表格权限失败只执行一次并直接进入人工处理状态。"""
    event_id = persist_messages(session_factory, ["客户：权限失败客户"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    def fail_permission(*args: object, **kwargs: object) -> object:
        """模拟智能表格返回不可重试的权限错误。"""
        raise PermissionError("smart table permission denied")

    monkeypatch.setattr(adapter, "create_record", fail_permission)
    FirstTextLeadWorkspaceService(session_factory, adapter).consume(event_id)

    with session_factory() as session:
        event = session.get(OutboxEvent, event_id)
    assert event is not None
    assert event.status == "failed_pending_review"
    assert event.attempts == 1
    assert event.failure_category == "permanent"


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供 T14 单元测试使用的隔离事务数据库。

    参数：无。
    返回值：绑定 SQLite 内存数据库的 SQLAlchemy 会话工厂。
    异常：数据库建表失败时由 pytest 直接报告。
    副作用：测试结束后删除本例创建的临时表和连接。
    """
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


def persist_messages(
    session_factory: sessionmaker[Session], texts: list[str]
) -> list[int]:
    """持久化同一销售按 sequence 排列的消息和 Outbox 事件。

    参数：session_factory 为测试事务工厂；texts 为按接收顺序排列的消息文本。
    返回值：按输入顺序返回 Outbox 事件标识。
    异常：数据库约束失败时由 SQLAlchemy 抛出。
    副作用：新增销售授权、原始消息和待消费事件。
    """
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True))
        event_ids: list[int] = []
        for sequence, text in enumerate(texts, start=1):
            message_id = f"message-{sequence}"
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id="sales-1",
                    sequence=sequence,
                    raw_payload={"text": text},
                    normalized_text=text,
                )
            )
            event = OutboxEvent(
                message_id=message_id,
                sales_user_id="sales-1",
                sequence=sequence,
            )
            session.add(event)
            session.flush()
            event_ids.append(event.id)
    return event_ids


def append_messages(
    session_factory: sessionmaker[Session], texts: list[str]
) -> list[int]:
    """在已创建的首条消息之后追加连续 sequence，避免首条成功消费自动递归处理后续消息。"""
    with session_factory.begin() as session:
        last_sequence = (
            session.scalar(
                select(IncomingMessage.sequence)
                .order_by(IncomingMessage.sequence.desc())
                .limit(1)
            )
            or 0
        )
        event_ids: list[int] = []
        for sequence, text in enumerate(texts, start=last_sequence + 1):
            message_id = f"message-{sequence}"
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id="sales-1",
                    sequence=sequence,
                    raw_payload={"text": text},
                    normalized_text=text,
                )
            )
            event = OutboxEvent(message_id=message_id, sales_user_id="sales-1", sequence=sequence)
            session.add(event)
            session.flush()
            event_ids.append(event.id)
    return event_ids


def test_failed_message_classification_keeps_sequence_checkpoint_and_processes_next(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证消息失败分类已保存且不阻塞同销售下一条消息。

    参数：session_factory 提供隔离数据库；monkeypatch 注入两次表格失败。
    返回值：无。
    异常：失败状态、顺序检查点或后续消息状态错误时由 pytest 报告。
    副作用：消费两条消息并创建后一条消息对应的线索。
    """
    first_event_id, second_event_id = persist_messages(
        session_factory, ["客户：失败客户", "客户：后续客户"]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original_create = adapter.create_record
    failures_remaining = 2

    def fail_first_message_then_create(*args: object, **kwargs: object) -> object:
        """仅让前两次表格创建失败，模拟一次耗尽自动重试的消息任务。"""
        nonlocal failures_remaining
        if failures_remaining:
            failures_remaining -= 1
            raise ConnectionError("smart table unavailable")
        return original_create(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(adapter, "create_record", fail_first_message_then_create)
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    service.consume(first_event_id)
    service.consume(first_event_id)

    with session_factory() as session:
        first_event = session.get(OutboxEvent, first_event_id)
        second_event = session.get(OutboxEvent, second_event_id)
        leads = session.scalars(select(Lead)).all()

    assert first_event is not None
    assert first_event.status == "failed_pending_review"
    assert first_event.attempts == 2
    assert first_event.failure_category == "transient"
    assert first_event.failure_summary == "ConnectionError"
    assert second_event is not None and second_event.sequence == 2
    assert second_event.status == "succeeded"
    assert len(leads) == 2
    assert any(lead.field_values.get("线索名称") == "后续客户" for lead in leads)


def test_retry_failed_message_is_protected_supplement_not_history_replay(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证失败消息重试只补充当前 Lead，不回放其后的两条消息。"""
    event_ids = persist_messages(session_factory, ["客户：客户甲；工艺：装配"])
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original_update = adapter.update_record
    update_calls: list[dict[str, object]] = []
    failures_remaining = 0

    def fail_update_twice(*args: object, **kwargs: object) -> object:
        """只让失败消息的两次自动处理失败，后续消息保持可处理。"""
        nonlocal failures_remaining
        update_calls.append(dict(args[1]))
        if failures_remaining:
            failures_remaining -= 1
            raise ConnectionError("temporary smart table outage")
        return original_update(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(adapter, "update_record", fail_update_twice)
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    service.consume(event_ids[0])
    event_ids.extend(
        append_messages(
            session_factory,
            [
                "客户：客户甲；联系人：失败联系人",
                "客户：客户甲；手机：13800138000",
                "客户：客户甲；需求：码垛",
            ],
        )
    )
    failures_remaining = 2
    service.consume(event_ids[1])
    service.consume(event_ids[1])

    with session_factory() as session:
        event_before_retry = session.get(OutboxEvent, event_ids[1])
    assert event_before_retry is not None, "failed message event missing"
    assert event_before_retry.status == "failed_pending_review", event_before_retry.status
    calls_before_retry = len(update_calls)
    result = service.retry_failed_message("message-2")
    repeated = service.retry_failed_message("message-2")

    assert result.status is ProtectedSupplementStatus.SUCCEEDED
    assert repeated.attempt_id == result.attempt_id
    assert result.updated_fields == ("联系人",)
    with session_factory() as session:
        lead = session.scalar(select(Lead).where(Lead.source_message_id == "message-1"))
        failed_event = session.get(OutboxEvent, event_ids[1])
        later_events = session.scalars(
            select(OutboxEvent).where(OutboxEvent.sequence.in_((3, 4)))
        ).all()
        attempts = session.scalars(select(MessageRetryAttempt)).all()
    assert lead is not None
    assert lead.field_values["手机"] == "13800138000"
    assert lead.field_values["工艺"] == "码垛"
    assert lead.field_values["联系人"] == "失败联系人"
    assert failed_event is not None and failed_event.status == "failed_pending_review"
    assert all(event.status == "succeeded" for event in later_events)
    assert len(attempts) == 1 and attempts[0].status == "succeeded"
    # 第二次人工点击只命中同一 attempt；retry 不会再次调用 N+1/N+2 的历史消费路径。
    assert len(update_calls) == calls_before_retry + 1
    assert update_calls[-1] == {"联系人": "失败联系人"}


def test_retry_failed_message_keeps_sales_edit_protected(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证失败后销售修改字段时，受保护重试不会覆盖人工确认值。"""
    event_ids = persist_messages(session_factory, ["客户：客户乙；工艺：装配"])
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original_update = adapter.update_record
    failures_remaining = 0

    def fail_update_twice(*args: object, **kwargs: object) -> object:
        """让目标消息耗尽自动重试。"""
        nonlocal failures_remaining
        if failures_remaining:
            failures_remaining -= 1
            raise ConnectionError("temporary smart table outage")
        return original_update(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(adapter, "update_record", fail_update_twice)
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    service.consume(event_ids[0])
    event_ids.extend(append_messages(session_factory, ["客户：客户乙；联系人：失败联系人"]))
    failures_remaining = 2
    service.consume(event_ids[1])
    service.consume(event_ids[1])

    with session_factory() as session:
        event_before_retry = session.get(OutboxEvent, event_ids[1])
    assert event_before_retry is not None, "failed message event missing"
    assert event_before_retry.status == "failed_pending_review", event_before_retry.status
    with session_factory() as session:
        lead = session.scalar(select(Lead).where(Lead.source_message_id == "message-1"))
        assert lead is not None and lead.smart_table_record_id is not None
        record_id = lead.smart_table_record_id
    original_update(record_id, {"联系人": "销售确认"})

    result = service.retry_failed_message("message-2")

    assert result.status is ProtectedSupplementStatus.SUCCEEDED
    assert "联系人" in result.protected_fields
    assert "联系人" not in result.updated_fields
    record = adapter.get_record(record_id)
    assert record is not None and record.fields["联系人"] == "销售确认"


def test_retry_failed_message_targets_the_failed_segment_only(
    session_factory: sessionmaker[Session],
) -> None:
    """验证多客户消息重试指定失败分段时不会把字段串到其他客户。"""
    persist_messages(
        session_factory,
        ["客户：客户甲；联系人：甲；客户：客户乙；联系人：乙失败补充"],
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record_a = adapter.create_record(
        {"负责人": "sales-1", "创建人": "sales-1", "线索名称": "客户甲"},
        actor=SmartTableActor.ROBOT,
    )
    record_b = adapter.create_record(
        {"负责人": "sales-1", "创建人": "sales-1", "线索名称": "客户乙"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        lead_a = Lead(
            source_message_id="message-1",
            source_segment_index=0,
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record_a.record_id,
            lifecycle_state="pending_create",
            field_values={"线索名称": "客户甲"},
        )
        lead_b = Lead(
            source_message_id="message-1",
            source_segment_index=1,
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record_b.record_id,
            lifecycle_state="pending_create",
            field_values={"线索名称": "客户乙"},
        )
        session.add_all((lead_a, lead_b))
        session.flush()
        session.add_all(
            (
                LeadFieldProvenance(
                    lead_id=lead_a.id,
                    source_message_id="message-1",
                    field_name="线索名称",
                    value="客户甲",
                    last_ai_synced_value="客户甲",
                ),
                LeadFieldProvenance(
                    lead_id=lead_b.id,
                    source_message_id="message-1",
                    field_name="线索名称",
                    value="客户乙",
                    last_ai_synced_value="客户乙",
                ),
                LeadMessageResolution(
                    message_id="message-1", segment_index=0, lead_id=lead_a.id, status="assigned"
                ),
                LeadMessageResolution(
                    message_id="message-1", segment_index=1, lead_id=lead_b.id, status="processing"
                ),
            )
        )
        event = session.scalar(select(OutboxEvent).where(OutboxEvent.message_id == "message-1"))
        assert event is not None
        event.status = "failed_pending_review"

    result = FirstTextLeadWorkspaceService(session_factory, adapter).retry_failed_message(
        "message-1", segment_index=1
    )

    assert result.status is ProtectedSupplementStatus.SUCCEEDED
    assert result.lead_id is not None
    assert adapter.get_record(record_a.record_id).fields == {  # type: ignore[union-attr]
        "负责人": "sales-1",
        "创建人": "sales-1",
        "线索名称": "客户甲",
    }
    assert adapter.get_record(record_b.record_id).fields["联系人"] == "乙失败补充"  # type: ignore[union-attr]
    with session_factory() as session:
        lead_a_after = session.scalar(
            select(Lead).where(Lead.source_segment_index == 0)
        )
    assert lead_a_after is not None
    assert "联系人" not in lead_a_after.field_values


def test_retry_does_not_replace_current_nonempty_ai_value(
    session_factory: sessionmaker[Session],
) -> None:
    """验证当前非空且已人工确认的字段不会被旧失败候选覆盖或重新要求确认。"""
    event_id = persist_messages(session_factory, ["客户：当前状态客户；联系人：失败候选"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {
            "负责人": "sales-1",
            "创建人": "sales-1",
            "线索名称": "当前状态客户",
            "联系人": "后续消息联系人",
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        lead = Lead(
            source_message_id="message-1",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            field_values={"线索名称": "当前状态客户", "联系人": "后续消息联系人"},
        )
        session.add(lead)
        session.flush()
        session.add(
            LeadFieldProvenance(
                lead_id=lead.id,
                source_message_id="message-next",
                field_name="联系人",
                value="后续消息联系人",
                last_ai_synced_value="后续消息联系人",
                is_user_confirmed=True,
            )
        )
        event = session.get(OutboxEvent, event_id)
        assert event is not None
        event.status = "failed_pending_review"

    result = FirstTextLeadWorkspaceService(session_factory, adapter).retry_failed_message(
        "message-1"
    )

    assert result.status is ProtectedSupplementStatus.SUCCEEDED
    assert "联系人" in result.protected_fields
    assert "联系人" not in result.updated_fields
    current_record = adapter.get_record(record.record_id)
    assert current_record is not None
    assert current_record.fields["联系人"] == "后续消息联系人"
    assert "联系人" not in current_record.fields.get("AI待确认", [])
    with session_factory() as session:
        lead = session.scalar(select(Lead).where(Lead.source_message_id == "message-1"))
        provenance = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead.id,
                LeadFieldProvenance.field_name == "联系人",
            )
        ) if lead is not None else None
    assert lead is not None and lead.field_values["联系人"] == "后续消息联系人"
    assert provenance is not None and provenance.is_user_confirmed is True


def test_retry_failed_message_recovers_expired_attempt_without_new_attempt(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 retry worker 崩溃后租约过期可复用原 logical attempt 并保持幂等。"""
    event_id = persist_messages(session_factory, ["客户：租约恢复客户；联系人：恢复联系人"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {"负责人": "sales-1", "创建人": "sales-1", "线索名称": "租约恢复客户"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        lead = Lead(
            source_message_id="message-1",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            field_values={"线索名称": "租约恢复客户"},
        )
        session.add(lead)
        session.flush()
        session.add(
            LeadMessageResolution(
                message_id="message-1", segment_index=0, lead_id=lead.id, status="processing"
            )
        )
        event = session.get(OutboxEvent, event_id)
        assert event is not None
        event.status = "failed_pending_review"
        session.add(
            MessageRetryAttempt(
                message_id="message-1",
                segment_index=0,
                lead_id=lead.id,
                operator_user_id="sales-1",
                attempt_number=1,
                status="processing",
                processing_started_at=datetime.now(UTC) - timedelta(minutes=10),
                processing_lease_expires_at=datetime.now(UTC) - timedelta(minutes=5),
                updated_fields=[],
                protected_fields=[],
            )
        )

    result = FirstTextLeadWorkspaceService(session_factory, adapter).retry_failed_message(
        "message-1"
    )

    assert result.status is ProtectedSupplementStatus.SUCCEEDED
    assert result.attempt_id is not None
    with session_factory() as session:
        attempts = session.scalars(select(MessageRetryAttempt)).all()
    assert len(attempts) == 1
    assert attempts[0].id == result.attempt_id
    assert attempts[0].status == "succeeded"


def test_retry_unexpired_attempt_cannot_be_claimed_by_second_executor(
    session_factory: sessionmaker[Session],
) -> None:
    """验证仍在 processing 租约内的 retry 不会被第二执行者接管或重写表格。"""
    event_id = persist_messages(session_factory, ["客户：租约未过期客户；联系人：原联系人"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {"负责人": "sales-1", "创建人": "sales-1", "线索名称": "租约未过期客户"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        lead = Lead(
            source_message_id="message-1",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            field_values={"线索名称": "租约未过期客户"},
        )
        session.add(lead)
        session.flush()
        session.add(
            LeadMessageResolution(
                message_id="message-1", segment_index=0, lead_id=lead.id, status="processing"
            )
        )
        event = session.get(OutboxEvent, event_id)
        assert event is not None
        event.status = "failed_pending_review"
        now = datetime.now(UTC)
        session.add(
            MessageRetryAttempt(
                message_id="message-1",
                segment_index=0,
                lead_id=lead.id,
                operator_user_id="sales-1",
                attempt_number=1,
                status="processing",
                processing_started_at=now,
                processing_lease_expires_at=now + timedelta(minutes=5),
                updated_fields=[],
                protected_fields=[],
            )
        )

    result = FirstTextLeadWorkspaceService(session_factory, adapter).retry_failed_message(
        "message-1"
    )

    assert result.status is ProtectedSupplementStatus.PROCESSING
    assert result.attempt_id is not None
    assert adapter.get_records()[0].fields == record.fields
    with session_factory() as session:
        attempts = session.scalars(select(MessageRetryAttempt)).all()
    assert len(attempts) == 1 and attempts[0].status == "processing"


def test_retry_cannot_patch_discarded_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 Lead 生命周期护栏优先于失败消息补充，已废弃线索不再写表格。"""
    event_id = persist_messages(session_factory, ["客户：已废弃客户；联系人：旧联系人"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    service.consume(event_id)
    with session_factory.begin() as session:
        lead = session.scalar(select(Lead).where(Lead.source_message_id == "message-1"))
        event = session.get(OutboxEvent, event_id)
        assert lead is not None and event is not None
        lead.lifecycle_state = "discarded"
        event.status = "failed_pending_review"
        record_id = lead.smart_table_record_id
    assert record_id is not None
    record_before = adapter.get_record(record_id)
    assert record_before is not None

    result = service.retry_failed_message("message-1")

    assert result.status is ProtectedSupplementStatus.NO_TARGET
    record = adapter.get_record(record_id)
    assert record is not None and record.fields == record_before.fields
    assert adapter.delete_calls == 0
    with session_factory() as session:
        lead = session.scalar(select(Lead).where(Lead.source_message_id == "message-1"))
    assert lead is not None and lead.lifecycle_state == "discarded"


def test_retry_lifecycle_fence_observes_discard_before_smart_table_patch(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 retry 已 claim 后若 discard 先提交，最终 fence 会阻止 Smart Table patch。"""
    event_id = persist_messages(session_factory, ["客户：竞态废弃客户；联系人：失败联系人"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    service.consume(event_id)
    with session_factory.begin() as session:
        lead = session.scalar(select(Lead).where(Lead.source_message_id == "message-1"))
        event = session.get(OutboxEvent, event_id)
        assert lead is not None and event is not None and lead.smart_table_record_id is not None
        lead_id = lead.id
        record_id = lead.smart_table_record_id
        event.status = "failed_pending_review"

    second_read_started, release_second_read = Event(), Event()

    class PausingAdapter(MockSmartTableAdapter):
        """在 retry 最后一次表格读取后暂停，留出 discard 已提交窗口。"""

        def __init__(self) -> None:
            """复制当前记录并初始化一次性读取屏障。"""
            super().__init__(schema=build_required_smart_table_schema())
            record = adapter.get_record(record_id)
            assert record is not None
            self._records[record.record_id] = record
            self._next_record_number = 2
            self._get_count = 0

        def get_record(self, requested_record_id: str) -> SmartTableRecord | None:
            """第二次读取完成前暂停，使 discard 能先提交生命周期变更。"""
            self._get_count += 1
            if self._get_count == 2:
                second_read_started.set()
                assert release_second_read.wait(timeout=5)
            return super().get_record(requested_record_id)

    pausing_adapter = PausingAdapter()
    retry_service = FirstTextLeadWorkspaceService(session_factory, pausing_adapter)
    record_before = pausing_adapter.get_record(record_id)
    assert record_before is not None

    def retry() -> object:
        """在后台执行受保护补充重试。"""
        return retry_service.retry_failed_message("message-1")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(retry)
        assert second_read_started.wait(timeout=5)
        discard = LeadDiscardService(session_factory).discard(
            lead_id, "sales-1", "retry 之前先废弃"
        )
        assert discard.status is LeadDiscardStatus.DISCARDED
        release_second_read.set()
        result = future.result(timeout=5)

    assert result.status is ProtectedSupplementStatus.FAILED_PENDING_REVIEW
    assert pausing_adapter.delete_calls == 0
    record_after = pausing_adapter.get_record(record_id)
    assert record_after is not None and record_after.fields == record_before.fields
    with session_factory() as session:
        attempt = session.scalar(
            select(MessageRetryAttempt).where(MessageRetryAttempt.message_id == "message-1")
        )
        audits = session.scalars(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.event_type == "lead_message_protected_retry_failed"
            )
        ).all()
    assert attempt is not None and attempt.status == "failed_pending_review"
    assert attempt.failure_category == "permanent"
    assert audits


def test_transferred_owner_can_retry_using_current_lead_owner(
    session_factory: sessionmaker[Session],
) -> None:
    """验证合法转交后的当前负责人可按当前 Lead 事实执行受保护补充。"""
    event_id = persist_messages(session_factory, ["客户：转交后重试客户"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    service.consume(event_id)
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-2", is_authorized=True))
        lead = session.scalar(select(Lead).where(Lead.source_message_id == "message-1"))
        event = session.get(OutboxEvent, event_id)
        assert lead is not None and event is not None and lead.smart_table_record_id is not None
        lead.smart_table_owner_user_id = "sales-2"
        event.status = "failed_pending_review"
        record_id = lead.smart_table_record_id

    result = service.retry_failed_message("message-1", operator_user_id="sales-2")

    assert result.status is ProtectedSupplementStatus.SUCCEEDED
    record = adapter.get_record(record_id)
    assert record is not None and record.fields["线索名称"] == "转交后重试客户"


def test_pending_create_discard_is_logical_and_idempotent(
    session_factory: sessionmaker[Session],
) -> None:
    """验证未启动 CRM create 的线索可逻辑废弃且保留表格与审计事实。"""
    event_id = persist_messages(session_factory, ["客户：待废弃客户"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    record = adapter.create_record(
        {"负责人": "sales-1", "创建人": "sales-1", "线索名称": "待废弃客户"},
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        lead = Lead(
            source_message_id="message-1",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            field_values={"线索名称": "待废弃客户", "线索来源": "展会"},
            standard_company_name="待废弃客户",
        )
        session.add(lead)
        session.flush()
        lead_id = lead.id
        session.add(
            CrmCompanyIdentity(
                standard_company_name="待废弃客户",
                state="reserving",
                creating_lead_id=lead.id,
            )
        )

    service = LeadDiscardService(session_factory)
    result = service.discard(lead_id, "sales-1", "客户明确放弃项目")
    repeated = service.discard(lead_id, "sales-1", "重复点击")

    assert result.status is LeadDiscardStatus.DISCARDED
    assert repeated.status is LeadDiscardStatus.ALREADY_DISCARDED
    with session_factory() as session:
        lead = session.get(Lead, lead_id)
        audits = session.scalars(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == "message-1",
                BusinessAuditEvent.event_type == "lead_discarded",
            )
        ).all()
        source = session.get(IncomingMessage, "message-1")
        event = session.get(OutboxEvent, event_id)
        identity = session.get(CrmCompanyIdentity, "待废弃客户")
    assert lead is not None
    assert lead.lifecycle_state == "discarded"
    assert lead.smart_table_record_id == record.record_id
    record_after = adapter.get_record(record.record_id)
    assert record_after is not None and record_after.fields == record.fields
    assert adapter.delete_calls == 0
    assert len(audits) == 1
    assert source is not None and event is not None
    assert identity is None
