"""业务审计事实到 Smart Table 管理员镜像子表的测试。"""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

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
    assert fields["缺失字段"] == "业务线"
    assert "lead_id" not in fields
    assert "status" not in fields
    assert "missing_fields" not in fields
    assert "phone" not in fields
    assert "email" not in fields
    assert "raw_response" not in fields


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
        assert audit_outbox.status == "pending"
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

    for outbox_id in outbox_ids:
        assert (
            _run_worker_with_adapter(session_factory, monkeypatch, outbox_id, adapter)
            == "succeeded"
        )
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
            """模拟审计子表外部调用失败。"""
            raise RuntimeError("external body must not be persisted")

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
        assert outbox.failure_category == "retryable"
        assert outbox.failure_code == "RuntimeError"


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
) -> list[int]:
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
    dispatched: list[int] = []
    monkeypatch.setattr(tasks.consume_audit_mirror_outbox, "delay", dispatched.append)
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
    assert dispatched == [ids[0], ids[2], ids[4], ids[6], ids[8]]


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
    assert dispatched == ids[:10]


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
    dispatched = _prepare_scanner(
        session_factory, monkeypatch, now=now, batch_size=10
    )

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
    """验证旧 Worker 的 defer 不能把新 Worker 的认领释放为 pending。"""
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
        assert outbox.status == "pending"
        assert outbox.claim_token is None
