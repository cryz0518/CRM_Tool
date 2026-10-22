"""业务审计事实到 Smart Table 管理员镜像子表的测试。"""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import Settings
from app.leads.models import Lead, LeadMessageResolution
from app.messaging.models import (
    AuditMirrorOutbox,
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    OutboxEvent,
    SalesAuthorization,
    utc_now,
)
from app.smart_table.adapter import SmartTableAdapterConfigurationError
from app.smart_table.audit import SmartTableAuditSink, build_mock_audit_schema
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableFieldType, SmartTableSchema
from app.smart_table.wecom_cli import WecomCliProcessError, WecomCliTransportError
from workers import tasks
from workers.celery_app import build_beat_schedule


def test_audit_mirror_worker_tasks_use_a_separate_queue() -> None:
    """验证耗时的审计 CLI 镜像不会占用线索消费队列。"""
    audit_route = tasks.celery_app.amqp.router.route(
        {}, "workers.consume_audit_mirror_outbox", args=(1,), kwargs={}
    )
    batch_route = tasks.celery_app.amqp.router.route(
        {}, "workers.consume_audit_mirror_batch", args=([1, 2],), kwargs={}
    )
    lead_route = tasks.celery_app.amqp.router.route(
        {}, "workers.consume_lead_outbox_event", args=(1,), kwargs={}
    )

    assert audit_route["queue"].name == "audit"
    assert batch_route["queue"].name == "audit"
    assert lead_route["queue"].name == "celery"


def test_audit_mirrors_are_scanned_once_per_minute() -> None:
    """验证审计批次每分钟派发一次，不跟随高频线索扫描。"""
    schedule = build_beat_schedule(Settings())
    assert schedule["consume-pending-audit-mirrors"]["schedule"] == 60


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


def _persist_mirror_jobs(factory: sessionmaker[Session], count: int) -> list[int]:
    """创建指定数量的独立审计镜像任务供扫描退避测试使用。

    参数：factory 为隔离测试会话工厂；count 为需要创建的任务数。
    返回值：按数据库 ID 排序的镜像任务 ID。
    异常：数据库约束错误由测试直接报告。
    副作用：仅写入测试数据库中的业务审计事实及其自动生成的镜像任务。
    """
    with factory.begin() as session:
        for index in range(count):
            session.add(
                BusinessAuditEvent(
                    message_id=f"mirror-message-{index}",
                    sales_user_id="sales-1",
                    event_type=f"mirror-event-{index}",
                    details={"status": "test"},
                )
            )
    with factory() as session:
        return list(
            session.scalars(select(AuditMirrorOutbox.id).order_by(AuditMirrorOutbox.id))
        )


def _rate_limited_transport_error() -> WecomCliTransportError:
    """构造保留 850005 cause 的可重试 CLI transport 错误。"""
    cause = WecomCliProcessError(
        "safe rate-limit summary",
        error_code="rate_limited",
        external_error_code=850005,
    )
    error = WecomCliTransportError("external body must not be persisted")
    error.__cause__ = cause
    return error


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


def _persist_lead_audit_case(
    factory: sessionmaker[Session],
    *,
    events: tuple[tuple[str, dict[str, object]], ...],
    leads: tuple[tuple[str, str | None, str | None], ...] = (),
    resolutions: tuple[tuple[str, int, str | None], ...] = (),
    source_outbox_status: str | None = None,
) -> list[int]:
    """持久化审计事件、线索及分段归属事实，并返回镜像任务 ID。"""
    message_ids = list(dict.fromkeys(message_id for message_id, _ in events))
    for message_id, _, _ in resolutions:
        if message_id not in message_ids:
            message_ids.append(message_id)
    for _, source_message_id, _ in leads:
        if source_message_id is not None and source_message_id not in message_ids:
            message_ids.append(source_message_id)

    with factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-1",
                is_authorized=False,
                is_active=True,
            )
        )
        for sequence, message_id in enumerate(message_ids, start=1):
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id="sales-1",
                    sequence=sequence,
                    raw_payload={},
                    normalized_text=None,
                )
            )
        if source_outbox_status is not None:
            for sequence, (message_id, _) in enumerate(events, start=1):
                session.add(
                    OutboxEvent(
                        message_id=message_id,
                        sales_user_id="sales-1",
                        sequence=sequence,
                        event_type="message_received",
                        status=source_outbox_status,
                    )
                )
        for lead_id, source_message_id, smart_table_record_id in leads:
            session.add(
                Lead(
                    id=lead_id,
                    source_message_id=source_message_id,
                    original_capturing_sales_user_id="sales-1",
                    smart_table_owner_user_id="sales-1",
                    smart_table_record_id=smart_table_record_id,
                    field_values={},
                    enrichment_values={},
                )
            )
        for message_id, segment_index, lead_id in resolutions:
            session.add(
                LeadMessageResolution(
                    message_id=message_id,
                    segment_index=segment_index,
                    lead_id=lead_id,
                    status="assigned" if lead_id is not None else "unassigned",
                )
            )
        for message_id, details in events:
            session.add(
                BusinessAuditEvent(
                    message_id=message_id,
                    sales_user_id="sales-1",
                    event_type=f"audit_{message_id}",
                    details=details,
                )
            )
    with factory() as session:
        return list(session.scalars(select(AuditMirrorOutbox.id).order_by(AuditMirrorOutbox.id)))


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
        first = SmartTableAuditSink(adapter).mirror(event, resolved_lead_id="lead-1")
    with session_factory.begin() as update_session:
        outbox = update_session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        outbox.status = "retrying"
    with session_factory() as session:
        event = session.scalar(select(BusinessAuditEvent))
        assert event is not None
        second = SmartTableAuditSink(adapter).mirror(event, resolved_lead_id="lead-1")

    assert first.record_id == second.record_id
    assert len(adapter.get_records()) == 1
    fields = adapter.get_records()[0].fields
    assert fields["线索归属ID"] == "lead-1"
    assert fields["日志唯一键"] == "message:message-1"
    assert fields["缺失字段"] == "业务线"
    assert "事件类型" not in fields
    assert "lead_id" not in fields
    assert "status" not in fields
    assert "missing_fields" not in fields
    assert "phone" not in fields
    assert "email" not in fields
    assert "raw_response" not in fields


def test_audit_mirror_updates_one_row_for_multiple_events_from_same_message(
    session_factory: sessionmaker[Session],
) -> None:
    """验证同一消息的多个事件共用一行，并保留先前写入的受控原文。"""
    _persist_event(session_factory)
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)
    sink = SmartTableAuditSink(adapter)
    with session_factory() as session:
        first_event = session.scalar(select(BusinessAuditEvent))
        assert first_event is not None
        original = sink.mirror(
            first_event,
            sales_original_message="客户名称：测试公司",
            resolved_lead_id="lead-1",
        )
    with session_factory.begin() as session:
        session.add(
            BusinessAuditEvent(
                message_id="message-1",
                sales_user_id="sales-1",
                event_type="crm_submission_retrying",
                details={"operation": "create", "status": "retrying"},
                created_at=utc_now() + timedelta(seconds=1),
            )
        )
    with session_factory() as session:
        events = list(session.scalars(select(BusinessAuditEvent).order_by(BusinessAuditEvent.id)))

    mirrored = sink.mirror_batch([(events[0], None, "lead-1"), (events[1], None, "lead-1")])

    assert len(adapter.get_records()) == 1
    assert mirrored[0].record_id == original.record_id == mirrored[1].record_id
    fields = mirrored[1].fields
    assert fields["日志唯一键"] == "message:message-1"
    assert fields["事件ID"] == str(events[1].id)
    assert fields["处理状态"] == "retrying"
    assert fields["线索归属ID"] == "lead-1"
    assert fields["销售原始消息"] == "客户名称：测试公司"
    assert "事件类型" not in fields


def test_audit_mirror_writes_wecom_user_id_to_operator_member_field(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证操作人人员字段使用企微发送人标识，不依赖 CRM 映射。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {"status": "succeeded"}),),
    )[0]
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-1")
        assert authorization is not None
        authorization.crm_user_id = "crm-employee-1"
        authorization.display_name = "王工（Jack）"
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    fields = adapter.get_records()[0].fields
    assert fields["操作人"] == "sales-1"
    assert "操作人ID" not in fields
    assert build_mock_audit_schema().get_field("操作人").field_type is SmartTableFieldType.MEMBER


def test_legacy_single_audit_task_coalesces_events_with_one_lookup_and_one_create(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证旧 Redis 单条任务可合并待处理事件并保留每条独立记录。"""
    outbox_ids = _persist_mirror_jobs(session_factory, 3)
    with session_factory.begin() as session:
        for outbox_id in outbox_ids:
            outbox = session.get(AuditMirrorOutbox, outbox_id)
            assert outbox is not None
            outbox.dispatch_lease_expires_at = utc_now() + timedelta(minutes=5)
    engine = session_factory.kw["bind"]
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(tasks, "_session_factory", lambda: (engine, session_factory))
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)
    lookup_calls: list[tuple[str, tuple[str, ...]]] = []
    create_calls: list[int] = []
    original_lookup = adapter.find_records_by_field_values
    original_create = adapter.create_records

    def track_lookup(field_name: str, values) -> list:
        """记录一次批量查重调用并委托内存适配器。"""
        lookup_calls.append((field_name, tuple(values)))
        return original_lookup(field_name, values)

    def track_create(records, *, actor):
        """记录单次批量新增的行数并委托内存适配器。"""
        create_calls.append(len(records))
        return original_create(records, actor=actor)

    monkeypatch.setattr(adapter, "find_records_by_field_values", track_lookup)
    monkeypatch.setattr(adapter, "create_records", track_create)
    monkeypatch.setattr(tasks, "get_smart_table_audit_adapter", lambda: adapter)

    assert tasks.consume_audit_mirror_outbox.run(outbox_ids[0]) == "succeeded"

    assert len(lookup_calls) == 1
    assert lookup_calls[0][0] == "消息ID"
    assert len(lookup_calls[0][1]) == 3
    assert create_calls == [3]
    records = adapter.get_records()
    assert len(records) == 3
    assert len({record.fields["事件ID"] for record in records}) == 3
    with session_factory() as session:
        rows = [session.get(AuditMirrorOutbox, outbox_id) for outbox_id in outbox_ids]
        assert all(row is not None and row.status == "succeeded" for row in rows)


def test_audit_batch_retry_reuses_rows_created_before_unknown_result(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证批量新增结果未知后，稳定键查重会保留已写行并只补缺行。"""
    _persist_mirror_jobs(session_factory, 2)
    with session_factory() as session:
        events = list(session.scalars(select(BusinessAuditEvent).order_by(BusinessAuditEvent.id)))
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)
    original_create = adapter.create_records
    calls: list[int] = []

    def create_partial(records, *, actor):
        """模拟首个批量请求只写入一行后客户端连接中断。"""
        calls.append(len(records))
        if len(calls) == 1:
            original_create(records[:1], actor=actor)
            raise _rate_limited_transport_error()
        return original_create(records, actor=actor)

    monkeypatch.setattr(adapter, "create_records", create_partial)
    sink = SmartTableAuditSink(adapter)
    batch = [(event, None, None) for event in events]

    with pytest.raises(WecomCliTransportError):
        sink.mirror_batch(batch)
    records = sink.mirror_batch(batch)

    assert calls == [2, 1]
    assert len(records) == 2
    assert len(adapter.get_records()) == 2
    assert {record.fields["日志唯一键"] for record in records} == {
        "message:mirror-message-0",
        "message:mirror-message-1",
    }


def test_audit_mirror_rejects_operator_field_that_is_not_personnel(
    session_factory: sessionmaker[Session],
) -> None:
    """验证审计表字段不是人员类型时，不会把企微标识写入其他字段类型。"""
    _persist_event(session_factory)
    with session_factory() as session:
        event = session.scalar(select(BusinessAuditEvent))
        assert event is not None
    schema = build_mock_audit_schema()
    invalid_schema = SmartTableSchema(
        fields=tuple(
            replace(field, field_type=SmartTableFieldType.TEXT)
            if field.name == "操作人"
            else field
            for field in schema.fields
        )
    )
    adapter = MockSmartTableAdapter(schema=invalid_schema, require_owner_field=False)

    with pytest.raises(SmartTableAdapterConfigurationError, match="必须为人员类型"):
        SmartTableAuditSink(adapter).mirror(event)


def test_audit_mirror_maps_internal_details_to_chinese_fields(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证内部英文 details 只以中文字段名输出到管理员表格。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {"lead_id": "lead-1", "status": "succeeded"}),),
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    fields = adapter.get_records()[0].fields
    assert fields["线索归属ID"] == "lead-1"
    assert fields["处理状态"] == "succeeded"
    assert "lead_id" not in fields
    assert "status" not in fields


def test_audit_mirror_resolves_lead_from_message_resolutions(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证没有 details lead_id 时使用唯一消息归属事实。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}),),
        leads=(("lead-1", None, None),),
        resolutions=(("message-1", 0, "lead-1"),),
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert adapter.get_records()[0].fields["线索归属ID"] == "lead-1"


def test_audit_mirror_keeps_same_lead_id_for_different_messages(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证同一线索的多条消息最终显示相同的 Lead.id。"""
    outbox_ids = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}), ("message-2", {})),
        leads=(("lead-1", None, None),),
        resolutions=(
            ("message-1", 0, "lead-1"),
            ("message-2", 0, "lead-1"),
        ),
    )
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    for outbox_id in outbox_ids:
        assert (
            _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter)
            == "succeeded"
        )
    assert [record.fields["线索归属ID"] for record in adapter.get_records()] == [
        "lead-1",
        "lead-1",
    ]


def test_audit_mirror_deduplicates_same_lead_across_message_segments(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证同消息多个 segment 指向同一 Lead 时仍可确定归属。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}),),
        leads=(("lead-1", None, None),),
        resolutions=(
            ("message-1", 0, "lead-1"),
            ("message-1", 1, "lead-1"),
        ),
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert adapter.get_records()[0].fields["线索归属ID"] == "lead-1"


def test_audit_mirror_leaves_ambiguous_segment_resolution_empty(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证同消息 segment 指向多个 Lead 时不任选其一。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}),),
        leads=(("lead-1", None, None), ("lead-2", None, None)),
        resolutions=(
            ("message-1", 0, "lead-1"),
            ("message-1", 1, "lead-2"),
        ),
        source_outbox_status="succeeded",
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert "线索归属ID" not in adapter.get_records()[0].fields


def test_audit_mirror_defers_until_source_outbox_terminal_then_resolves_lead(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 message_received race 会先 defer，业务终态后再镜像唯一归属。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}),),
        source_outbox_status="pending",
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "deferred"
    assert adapter.get_records() == []
    with session_factory() as session:
        audit_outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert audit_outbox is not None
        assert audit_outbox.status == "waiting_source"
        assert audit_outbox.claim_token is None

    with session_factory.begin() as session:
        session.add(
            Lead(
                id="lead-1",
                source_message_id=None,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                field_values={},
                enrichment_values={},
            )
        )
        session.add(
            LeadMessageResolution(
                message_id="message-1",
                segment_index=0,
                lead_id="lead-1",
                status="assigned",
            )
        )
        source_outbox = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "message-1")
        )
        assert source_outbox is not None
        source_outbox.status = "succeeded"

    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=utc_now(), batch_size=10
    )
    assert tasks.consume_pending_audit_mirrors.run() == 1
    assert dispatched == [[outbox_id]]
    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert len(adapter.get_records()) == 1
    fields = adapter.get_records()[0].fields
    assert fields["消息ID"] == "message-1"
    assert fields["线索归属ID"] == "lead-1"


def test_audit_mirror_defers_multiple_messages_then_keeps_one_lead_id(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证同一线索的多条消息分别 defer 后仍镜像同一个 Lead.id。"""
    outbox_ids = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}), ("message-2", {})),
        source_outbox_status="pending",
    )
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    for outbox_id in outbox_ids:
        assert (
            _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter)
            == "deferred"
        )

    with session_factory.begin() as session:
        session.add(
            Lead(
                id="lead-1",
                source_message_id=None,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                field_values={},
                enrichment_values={},
            )
        )
        session.add_all(
            [
                LeadMessageResolution(
                    message_id="message-1",
                    segment_index=0,
                    lead_id="lead-1",
                    status="assigned",
                ),
                LeadMessageResolution(
                    message_id="message-2",
                    segment_index=0,
                    lead_id="lead-1",
                    status="assigned",
                ),
            ]
        )
        for message_id, text in (
            ("message-1", "第一条线索消息"),
            ("message-2", "第二条补充消息"),
        ):
            message = session.get(IncomingMessage, message_id)
            assert message is not None
            message.normalized_text = text
            source_outbox = session.scalar(
                select(OutboxEvent).where(OutboxEvent.message_id == message_id)
            )
            assert source_outbox is not None
            source_outbox.status = "succeeded"

    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=utc_now(), batch_size=10
    )
    assert tasks.consume_pending_audit_mirrors.run() == len(outbox_ids)
    assert dispatched == [outbox_ids]
    assert tasks.consume_audit_mirror_batch.run(outbox_ids) == "succeeded"
    records = adapter.get_records()
    assert [record.fields["消息ID"] for record in records] == ["message-1", "message-2"]
    assert [record.fields["销售原始消息"] for record in records] == [
        "第一条线索消息",
        "第二条补充消息",
    ]
    assert [record.fields["线索归属ID"] for record in records] == ["lead-1", "lead-1"]


def test_audit_mirror_terminal_ignored_message_does_not_wait_for_lead(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 ignored 消息没有归属时仍能在业务终态完成审计镜像。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}),),
        source_outbox_status="ignored",
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert len(adapter.get_records()) == 1
    assert "线索归属ID" not in adapter.get_records()[0].fields


def test_audit_mirror_details_lead_id_has_priority_over_lookup(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 details.lead_id 优先于消息归属查询结果。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {"lead_id": "lead-1"}),),
        leads=(("lead-1", None, None), ("lead-2", None, None)),
        resolutions=(("message-1", 0, "lead-2"),),
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert adapter.get_records()[0].fields["线索归属ID"] == "lead-1"


def test_audit_mirror_resolves_lead_from_smart_table_record_id(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 details.smart_table_record_id 唯一匹配时解析对应 Lead.id。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {"smart_table_record_id": "record-1"}),),
        leads=(("lead-1", None, "record-1"),),
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert adapter.get_records()[0].fields["线索归属ID"] == "lead-1"


def test_audit_mirror_without_lead_facts_leaves_lead_id_empty(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证没有确定归属事实时不猜测线索归属 ID。"""
    outbox_id = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}),),
    )[0]
    adapter = MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)

    assert _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter) == "succeeded"
    assert "线索归属ID" not in adapter.get_records()[0].fields


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
    mirror_ids = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {"lead_id": "lead-1", "status": "succeeded"}),),
        leads=(("lead-1", "message-1", None),),
        resolutions=(("message-1", 0, "lead-1"),),
        source_outbox_status="succeeded",
    )
    outbox_id = mirror_ids[0]

    class FailingAdapter:
        """只用于证明外部审计子表失败不会改变业务审计状态。"""

        def get_schema(self):
            """模拟明确可重试的外部 Smart Table 传输失败。"""
            raise _rate_limited_transport_error()

    engine = session_factory.kw["bind"]
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(tasks, "_session_factory", lambda: (engine, session_factory))
    monkeypatch.setattr(tasks, "get_smart_table_audit_adapter", FailingAdapter)

    assert tasks.consume_audit_mirror_outbox.run(outbox_id) == "retrying"
    with session_factory() as session:
        assert session.scalar(select(BusinessAuditEvent)) is not None
        lead = session.get(Lead, "lead-1")
        resolution = session.scalar(select(LeadMessageResolution))
        business_outbox = session.scalar(select(OutboxEvent))
        assert lead is not None
        assert lead.field_values == {}
        assert resolution is not None and resolution.lead_id == "lead-1"
        assert business_outbox is not None and business_outbox.status == "succeeded"
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "retrying"
        assert outbox.claim_token is None
        assert outbox.failure_category == "transient"
        assert outbox.failure_code == "WecomCliTransportError"
        assert outbox.attempts == 1


def test_audit_mirror_850003_is_permanent_and_not_scanned(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 850003 保持 permanent/manual-review，不会被审计扫描器重投。"""
    outbox_id = _persist_event(session_factory)

    class PermanentFailureAdapter:
        """模拟 wecom-cli 返回未经解释的 850003 结构化错误。"""

        def get_schema(self):
            """抛出适配器已分类的永久业务错误，不包含远端正文。"""
            raise WecomCliProcessError(
                "safe error summary",
                error_code="remote_business_error",
                external_error_code=850003,
            )

    assert (
        _run_worker_with_adapter(
            session_factory,
            monkeypatch,
            outbox_id,
            PermanentFailureAdapter(),
        )
        == "failed_pending_review"
    )
    with session_factory.begin() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "failed_pending_review"
        assert outbox.failure_category == "permanent"
        assert outbox.failure_code == "WecomCliProcessError"
        outbox.updated_at = utc_now() - timedelta(days=2)

    now = utc_now()
    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=now, batch_size=10
    )
    assert tasks.consume_pending_audit_mirrors.run() == 0
    assert dispatched == []


def test_unknown_audit_mirror_failure_requires_review_and_is_not_scanned(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证普通 RuntimeError 分类为 unknown 并永久退出自动扫描。"""
    outbox_id = _persist_event(session_factory)

    class UnknownFailureAdapter:
        """模拟未被 failure framework 识别的审计镜像异常。"""

        def get_schema(self):
            """抛出普通运行时异常，验证 unknown 不自动重试。"""
            raise RuntimeError("external error text must not be stored")

    assert (
        _run_worker_with_adapter(
            session_factory,
            monkeypatch,
            outbox_id,
            UnknownFailureAdapter(),
        )
        == "failed_pending_review"
    )
    with session_factory.begin() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "failed_pending_review"
        assert outbox.failure_category == "unknown"
        assert outbox.failure_code == "RuntimeError"
        assert "external error text" not in (outbox.failure_code or "")
        outbox.updated_at = utc_now() - timedelta(days=2)

    now = utc_now()
    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=now, batch_size=10
    )
    assert tasks.consume_pending_audit_mirrors.run() == 0
    assert dispatched == []


def test_deferred_audit_claims_do_not_consume_retry_attempts(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证等待来源不会消耗尝试或重复认领，来源完成后真正失败仍进入退避。"""
    (outbox_id,) = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}),),
        source_outbox_status="processing",
    )
    token = tasks._claim_audit_mirror_outbox(session_factory, outbox_id)
    assert token is not None
    tasks._defer_audit_mirror(session_factory, outbox_id, token)
    with session_factory() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "waiting_source"
        assert outbox.attempts == 0
    # 已在 Redis 中排队的旧副本不能绕过等待状态再次认领同一条镜像。
    assert tasks._claim_audit_mirror_outbox(session_factory, outbox_id) is None

    with session_factory.begin() as session:
        source = session.scalar(select(OutboxEvent))
        assert source is not None
        source.status = "succeeded"
    now = utc_now()
    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=now, batch_size=10
    )
    assert tasks.consume_pending_audit_mirrors.run() == 1
    assert dispatched == [[outbox_id]]

    class TransientFailureAdapter:
        """模拟首次真正进入镜像调用后发生的可重试传输错误。"""

        def get_schema(self):
            """抛出真实适配器使用的 transient 标记错误类型。"""
            raise _rate_limited_transport_error()

    assert (
        _run_worker_with_adapter(
            session_factory,
            monkeypatch,
            outbox_id,
            TransientFailureAdapter(),
        )
        == "retrying"
    )
    with session_factory() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "retrying"
        assert outbox.failure_category == "transient"
        assert outbox.attempts == 1
        updated_at = outbox.updated_at
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=UTC)

    dispatched = _prepare_scanner(
        session_factory,
        monkeypatch,
        now=updated_at + timedelta(seconds=29),
        batch_size=10,
    )
    assert tasks.consume_pending_audit_mirrors.run() == 0
    assert dispatched == []
    monkeypatch.setattr(tasks, "utc_now", lambda: updated_at + timedelta(seconds=30))
    assert tasks.consume_pending_audit_mirrors.run() == 1
    assert dispatched == [[outbox_id]]


def test_deferred_audit_mirrors_do_not_starve_later_pending_batch(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 defer 刷新 updated_at 后，下一轮 batch 能越过原先最老的十条。"""
    ids = _persist_mirror_jobs(session_factory, 13)
    now = utc_now()
    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=now, batch_size=10
    )

    assert tasks.consume_pending_audit_mirrors.run() == 10
    assert dispatched == [ids[:10]]
    original_timestamps: dict[int, tuple[datetime, datetime]] = {}
    for outbox_id in ids[:10]:
        with session_factory() as session:
            row = session.get(AuditMirrorOutbox, outbox_id)
            assert row is not None
            original_timestamps[outbox_id] = (row.created_at, row.updated_at)
        token = tasks._claim_audit_mirror_outbox(session_factory, outbox_id)
        assert token is not None
        with session_factory() as session:
            row = session.get(AuditMirrorOutbox, outbox_id)
            assert row is not None
            claimed_updated_at = row.updated_at
            assert row.attempts == 1
        tasks._defer_audit_mirror(session_factory, outbox_id, token)
        with session_factory() as session:
            row = session.get(AuditMirrorOutbox, outbox_id)
            assert row is not None
            assert row.attempts == 0
            assert row.created_at == original_timestamps[outbox_id][0]
            assert row.updated_at > claimed_updated_at

    dispatched.clear()
    assert tasks.consume_pending_audit_mirrors.run() == 10
    # 新鲜 defer 的十条排到末尾，先投递此前未被选中的三条。
    assert dispatched == [[*ids[10:], *ids[:7]]]


@pytest.mark.parametrize(
    ("attempts", "expected_seconds"),
    ((1, 30), (2, 60), (3, 120), (4, 300), (20, 300)),
)
def test_audit_retry_delay_seconds_is_bounded_exponential(
    attempts: int, expected_seconds: int
) -> None:
    """验证镜像退避每次翻倍并在五分钟封顶。"""
    assert tasks.audit_retry_delay_seconds(attempts) == expected_seconds


def _prepare_scanner(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    *,
    now: datetime,
    batch_size: int,
) -> list[list[int]]:
    """将审计扫描器绑定到隔离数据库、固定时钟和只记录参数的投递函数。"""
    engine = session_factory.kw["bind"]
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(tasks, "_session_factory", lambda: (engine, session_factory))
    monkeypatch.setattr(tasks, "utc_now", lambda: now)
    monkeypatch.setattr(
        tasks,
        "get_settings",
        lambda: SimpleNamespace(
            lead_processing_timeout_seconds=300,
            audit_mirror_batch_size=batch_size,
        ),
    )
    dispatched: list[list[int]] = []
    monkeypatch.setattr(tasks.consume_audit_mirror_batch, "delay", dispatched.append)
    return dispatched


def test_audit_scanner_dispatches_pending_and_due_retries_only(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 pending 立即投递、retrying 按 attempts 到期后才投递。"""
    ids = _persist_mirror_jobs(session_factory, 9)
    now = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    with session_factory.begin() as session:
        rows = [session.get(AuditMirrorOutbox, outbox_id) for outbox_id in ids]
        assert all(row is not None for row in rows)
        rows[1].status, rows[1].attempts = "retrying", 1
        rows[1].updated_at = now - timedelta(seconds=29)
        rows[2].status, rows[2].attempts = "retrying", 1
        rows[2].updated_at = now - timedelta(seconds=30)
        rows[3].status, rows[3].attempts = "retrying", 2
        rows[3].updated_at = now - timedelta(seconds=59)
        rows[4].status, rows[4].attempts = "retrying", 2
        rows[4].updated_at = now - timedelta(seconds=60)
        rows[5].status, rows[5].attempts = "retrying", 3
        rows[5].updated_at = now - timedelta(seconds=119)
        rows[6].status, rows[6].attempts = "retrying", 3
        rows[6].updated_at = now - timedelta(seconds=120)
        rows[7].status, rows[7].attempts = "retrying", 5
        rows[7].updated_at = now - timedelta(seconds=299)
        rows[8].status, rows[8].attempts = "retrying", 5
        rows[8].updated_at = now - timedelta(seconds=300)

    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=now, batch_size=10
    )
    assert tasks.consume_pending_audit_mirrors.run() == 5
    # 新消息优先于历史重试；重试任务之间仍按原退避时间排序。
    assert dispatched == [[ids[0], ids[8], ids[6], ids[4], ids[2]]]


def test_audit_scanner_applies_batch_limit_before_dispatch(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证大量 pending 镜像不会让单轮投递超过独立 batch size。"""
    ids = _persist_mirror_jobs(session_factory, 13)
    now = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=now, batch_size=10
    )

    assert tasks.consume_pending_audit_mirrors.run() == 10
    assert dispatched == [ids[:10]]


def test_audit_scanner_waits_for_the_current_batch_to_finish(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证排队租约和有效 processing 都占用独立审计 Worker 槽位。"""
    ids = _persist_mirror_jobs(session_factory, 4)
    now = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    with session_factory.begin() as session:
        queued = session.get(AuditMirrorOutbox, ids[0])
        processing = session.get(AuditMirrorOutbox, ids[1])
        expired = session.get(AuditMirrorOutbox, ids[2])
        assert queued is not None and processing is not None and expired is not None
        queued.dispatch_lease_expires_at = now + timedelta(minutes=5)
        processing.status = "processing"
        processing.processing_started_at = now - timedelta(seconds=30)
        expired.status = "processing"
        expired.processing_started_at = now - timedelta(seconds=600)
        expired.updated_at = now - timedelta(seconds=600)

    dispatched = _prepare_scanner(session_factory, monkeypatch, now=now, batch_size=10)
    # 一个排队租约或有效 processing 都会阻止派发第二个批次。
    assert tasks.consume_pending_audit_mirrors.run() == 0
    assert dispatched == []
    assert tasks.consume_pending_audit_mirrors.run() == 0


def test_audit_scanner_waits_for_source_outbox_and_releases_deferred_mirror(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证来源消息未完成时不派发镜像，完成后只重新激活一次。"""
    (outbox_id,) = _persist_lead_audit_case(
        session_factory,
        events=(("message-1", {}),),
        source_outbox_status="processing",
    )
    now = utc_now()
    dispatched = _prepare_scanner(session_factory, monkeypatch, now=now, batch_size=10)

    # 扫描器直接跳过未完成的来源，不向拥塞的共享队列增加等待任务。
    assert tasks.consume_pending_audit_mirrors.run() == 0
    assert dispatched == []

    # 旧队列中已有的重复消息最多把镜像转入等待态，后续副本无法再次认领。
    token = tasks._claim_audit_mirror_outbox(session_factory, outbox_id)
    assert token is not None
    tasks._defer_audit_mirror(session_factory, outbox_id, token)
    assert tasks._claim_audit_mirror_outbox(session_factory, outbox_id) is None
    with session_factory() as session:
        mirror = session.get(AuditMirrorOutbox, outbox_id)
        assert mirror is not None
        assert mirror.status == "waiting_source"

    # 来源进入终态后，扫描器重新激活并投递该镜像。
    with session_factory.begin() as session:
        source = session.scalar(select(OutboxEvent))
        assert source is not None
        source.status = "succeeded"
    assert tasks.consume_pending_audit_mirrors.run() == 1
    assert dispatched == [[outbox_id]]
    with session_factory() as session:
        mirror = session.get(AuditMirrorOutbox, outbox_id)
        assert mirror is not None
        assert mirror.status == "pending"


def test_audit_scanner_skips_large_not_due_retry_backlog(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证未到期 retrying backlog 不投递，也不能被直接认领绕过退避。"""
    ids = _persist_mirror_jobs(session_factory, 25)
    now = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
    with session_factory.begin() as session:
        for outbox_id in ids:
            row = session.get(AuditMirrorOutbox, outbox_id)
            assert row is not None
            row.status = "retrying"
            row.attempts = 1
            row.updated_at = now - timedelta(seconds=29)
    dispatched = _prepare_scanner(session_factory, monkeypatch, now=now, batch_size=10)

    assert tasks.consume_pending_audit_mirrors.run() == 0
    assert dispatched == []
    assert tasks._claim_audit_mirror_outbox(session_factory, ids[0]) is None


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


def test_stale_audit_mirror_worker_cannot_defer_new_claim(
    session_factory: sessionmaker[Session],
) -> None:
    """验证旧 Worker 的 defer 不能释放新认领，当前认领只能进入来源等待态。"""
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

    tasks._defer_audit_mirror(session_factory, outbox_id, token_a)
    with session_factory() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "processing"
        assert outbox.claim_token == token_b

    tasks._defer_audit_mirror(session_factory, outbox_id, token_b)
    with session_factory() as session:
        outbox = session.get(AuditMirrorOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status == "waiting_source"
        assert outbox.claim_token is None
