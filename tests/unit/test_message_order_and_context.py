"""同销售顺序消费与当前客户上下文的应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.leads.models import LeadFieldProvenance, LeadMessageResolution, SalesLeadContext
from app.leads.service import FirstTextLeadWorkspaceService, LeadProcessingStatus
from app.messaging.models import Base, IncomingMessage, OutboxEvent, SalesAuthorization, utc_now
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供验证 Outbox 消费顺序的隔离真实事务数据库。

    参数：无。
    返回值：逐例生成 SQLAlchemy 会话工厂。
    异常：建表或数据库连接失败时向 pytest 传播。
    副作用：测试前创建、测试后删除内存数据库中的全部表。
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


def persist_outbox_texts(
    session_factory: sessionmaker[Session], sales_user_id: str, texts: list[str]
) -> list[int]:
    """为一名授权销售按给定顺序写入多条已持久化文本 Outbox 事件。

    参数：session_factory 创建测试事务；sales_user_id 为销售标识；texts 为有序文本。
    返回值：按输入顺序返回新建 Outbox 事件标识。
    异常：违反数据库约束时由 SQLAlchemy 抛出。
    副作用：新增授权、来源消息和待消费 Outbox 事件。
    """
    with session_factory.begin() as session:
        if session.get(SalesAuthorization, sales_user_id) is None:
            session.add(SalesAuthorization(wecom_user_id=sales_user_id, is_authorized=True))
        last_sequence = session.scalar(
            select(OutboxEvent.sequence)
            .where(OutboxEvent.sales_user_id == sales_user_id)
            .order_by(OutboxEvent.sequence.desc())
            .limit(1)
        )
        event_ids: list[int] = []
        for sequence, text in enumerate(texts, start=(last_sequence or 0) + 1):
            message_id = f"{sales_user_id}-message-{sequence}"
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id=sales_user_id,
                    sequence=sequence,
                    raw_payload={"text": text},
                    normalized_text=text,
                )
            )
            event = OutboxEvent(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
            )
            session.add(event)
            session.flush()
            event_ids.append(event.id)
    return event_ids


def test_later_message_waits_for_earlier_pending_message_from_same_salesperson(
    session_factory: sessionmaker[Session],
) -> None:
    """验证同一销售较晚的消息不会越过较早的未完成消息。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：顺序门未生效时由 pytest 报告断言失败。
    副作用：尝试消费第二条 Outbox，但不得创建线索或表格记录。
    """
    first_event_id, second_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：长广溪智造", "客户：不应先处理"],
    )
    service = FirstTextLeadWorkspaceService(
        session_factory,
        MockSmartTableAdapter(schema=build_required_smart_table_schema()),
    )

    result = service.consume(second_event_id)

    assert first_event_id < second_event_id
    assert result.status is LeadProcessingStatus.WAITING_FOR_PREVIOUS


def test_fragment_within_current_context_safely_updates_the_same_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证当前客户上下文内的碎片信息只增量补充同一条线索。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：归属错误或整行覆盖时由 pytest 报告断言失败。
    副作用：依次消费两条 Outbox，并向 Mock 智能表格写入一个字段补丁。
    """
    first_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：长广溪智造"],
    )[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    created = service.consume(first_event_id)
    second_event_id = persist_outbox_texts(session_factory, "sales-1", ["需求：码垛机器人"])[0]
    updated = service.consume(second_event_id)

    assert created.status is LeadProcessingStatus.CREATED
    assert updated.status is LeadProcessingStatus.UPDATED
    assert updated.lead_id == created.lead_id
    assert created.smart_table_record_id is not None
    record = adapter.get_record(created.smart_table_record_id)
    assert record is not None
    assert record.fields["线索名称"] == "长广溪智造"
    assert record.fields["工艺"] == "码垛"
    assert "备注" in record.fields
    with session_factory() as session:
        process_source = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == created.lead_id,
                LeadFieldProvenance.field_name == "工艺",
            )
        )

    assert process_source is not None
    assert process_source.last_ai_synced_value == "码垛"


def test_repeated_current_company_name_updates_instead_of_creating_a_second_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证上下文内重复公司名仍合并字段补丁，不会创建第二条表格记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：同公司消息新建第二条线索时由 pytest 报告断言失败。
    副作用：先创建当前线索，再消费重复公司名和工艺的后续消息。
    """
    first_event_id = persist_outbox_texts(session_factory, "sales-1", ["客户：长广溪智造"])[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    created = service.consume(first_event_id)
    second_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["公司：长广溪智造；需求：码垛机器人"],
    )[0]

    updated = service.consume(second_event_id)

    assert updated.status is LeadProcessingStatus.UPDATED
    assert updated.lead_id == created.lead_id
    assert len(adapter.get_records()) == 1


def test_failed_pending_review_event_is_a_checkpoint_for_later_same_sales_message(
    session_factory: sessionmaker[Session],
) -> None:
    """验证失败待处理消息不会永久阻塞同一销售的后续首次消费。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：失败终态仍阻塞后续消息时由 pytest 报告断言失败。
    副作用：将第一条事件模拟为失败待处理，再消费第二条事件。
    """
    first_event_id, second_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：失败客户", "客户：后续客户"],
    )
    with session_factory.begin() as session:
        first_event = session.get(OutboxEvent, first_event_id)
        assert first_event is not None
        first_event.status = "failed_pending_review"
    service = FirstTextLeadWorkspaceService(
        session_factory,
        MockSmartTableAdapter(schema=build_required_smart_table_schema()),
    )

    result = service.consume(second_event_id)

    assert result.status is LeadProcessingStatus.CREATED


def test_pending_message_from_another_salesperson_does_not_block_consumption(
    session_factory: sessionmaker[Session],
) -> None:
    """验证不同销售的独立持久化序列不会互相阻塞。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：跨销售错误互锁时由 pytest 报告断言失败。
    副作用：保留销售一的待消费事件，并消费销售二的首条事件。
    """
    persist_outbox_texts(session_factory, "sales-1", ["客户：销售一客户"])
    second_sales_event_id = persist_outbox_texts(session_factory, "sales-2", ["客户：销售二客户"])[
        0
    ]
    service = FirstTextLeadWorkspaceService(
        session_factory,
        MockSmartTableAdapter(schema=build_required_smart_table_schema()),
    )

    result = service.consume(second_sales_event_id)

    assert result.status is LeadProcessingStatus.CREATED


def test_failed_retries_automatically_continue_to_the_next_sales_message(
    session_factory: sessionmaker[Session],
) -> None:
    """验证重试耗尽的失败消息自动让同销售下一条消息继续处理。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：失败终态仍未推进后续消息时由 pytest 报告断言失败。
    副作用：使首条表格创建连续失败两次，并检查第二条普通消息已被自动消费。
    """
    first_event_id, second_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：失败客户", "你好，机器人"],
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    with pytest.MonkeyPatch.context() as monkeypatch:

        def raise_table_error(*_: object, **__: object) -> object:
            """模拟智能表格不可用，强制触发消息处理重试。

            参数：位置和关键字参数用于兼容适配器调用。
            返回值：无正常返回。
            异常：始终抛出 RuntimeError。
            副作用：无。
            """
            raise RuntimeError("temporary")

        monkeypatch.setattr(adapter, "create_record", raise_table_error)
        service.consume(first_event_id)
        service.consume(first_event_id)

    with session_factory() as session:
        first_event = session.get(OutboxEvent, first_event_id)
        second_event = session.get(OutboxEvent, second_event_id)

    assert first_event is not None
    assert first_event.status == "failed_pending_review"
    assert first_event.attempts == 2
    assert second_event is not None
    assert second_event.status == "ignored"


def test_expired_processing_lease_becomes_a_checkpoint_for_the_next_message(
    session_factory: sessionmaker[Session],
) -> None:
    """验证失联 processing 任务超时后不会永久阻塞同一销售后续消息。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：租约超时未进入失败检查点或后续消息未继续时由 pytest 报告断言失败。
    副作用：将首条事件模拟为六分钟前开始 processing，再消费它以触发租约恢复。
    """
    first_event_id, second_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：失联客户", "你好，机器人"],
    )
    with session_factory.begin() as session:
        first_event = session.get(OutboxEvent, first_event_id)
        assert first_event is not None
        first_event.status = "processing"
        first_event.processing_started_at = utc_now() - timedelta(minutes=6)
    service = FirstTextLeadWorkspaceService(
        session_factory,
        MockSmartTableAdapter(schema=build_required_smart_table_schema()),
    )

    result = service.consume(first_event_id)

    assert result.status is LeadProcessingStatus.ALREADY_PROCESSED
    with session_factory() as session:
        first_event = session.get(OutboxEvent, first_event_id)
        second_event = session.get(OutboxEvent, second_event_id)
    assert first_event is not None
    assert first_event.status == "failed_pending_review"
    assert second_event is not None
    assert second_event.status == "ignored"


def test_expired_current_context_keeps_weak_fragment_unassigned(
    session_factory: sessionmaker[Session],
) -> None:
    """验证超出当前客户上下文有效期的弱身份碎片不会附着历史线索。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：过期消息被归属到历史线索时由 pytest 报告断言失败。
    副作用：创建首条线索后将第二条消息时间推进三十一分钟并消费。
    """
    first_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：长广溪智造"],
    )[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter, lead_context_ttl_minutes=30)
    created = service.consume(first_event_id)
    assert created.lead_id is not None
    second_event_id = persist_outbox_texts(session_factory, "sales-1", ["预算约 20 万"])[0]
    with session_factory.begin() as session:
        first_message = session.get(IncomingMessage, "sales-1-message-1")
        second_message = session.get(IncomingMessage, "sales-1-message-2")
        assert first_message is not None
        assert second_message is not None
        second_message.received_at = first_message.received_at + timedelta(minutes=31)

    result = service.consume(second_event_id)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    assert adapter.get_records() == [adapter.get_record(created.smart_table_record_id)]
    with session_factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "sales-1-message-2"
            )
        )
    assert resolution is not None
    assert resolution.status == "unassigned"
    assert resolution.lead_id is None


def test_expired_context_uses_unique_phone_as_strong_identity(
    session_factory: sessionmaker[Session],
) -> None:
    """验证过期上下文后的唯一手机号仍可安全定位当前销售自己的既有线索。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：强身份消息未补充到既有线索时由 pytest 报告断言失败。
    副作用：创建带手机号的线索，推进后续消息时间并消费手机号补充。
    """
    first_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：长广溪智造；手机号：13800000000"],
    )[0]
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter, lead_context_ttl_minutes=30)
    created = service.consume(first_event_id)
    second_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["手机号：13800000000；需求：码垛机器人"],
    )[0]
    with session_factory.begin() as session:
        first_message = session.get(IncomingMessage, "sales-1-message-1")
        second_message = session.get(IncomingMessage, "sales-1-message-2")
        assert first_message is not None
        assert second_message is not None
        second_message.received_at = first_message.received_at + timedelta(minutes=31)

    updated = service.consume(second_event_id)

    assert updated.status is LeadProcessingStatus.UPDATED
    assert updated.lead_id == created.lead_id
    assert created.smart_table_record_id is not None
    record = adapter.get_record(created.smart_table_record_id)
    assert record is not None
    assert record.fields["工艺"] == "码垛"


def test_strong_identity_beats_an_active_context_from_another_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证历史客户的唯一手机号优先于仍有效的另一条当前客户上下文。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：手机号补充被串入当前上下文线索时由 pytest 报告断言失败。
    副作用：创建两条销售私有线索，手动恢复首条上下文后消费第二条的手机号补充。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    first_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：客户甲；手机号：13800000001"],
    )[0]
    first = service.consume(first_event_id)
    second_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["客户：客户乙；手机号：13800000002"],
    )[0]
    second = service.consume(second_event_id)
    assert first.lead_id is not None
    assert second.lead_id is not None
    with session_factory.begin() as session:
        context = session.get(SalesLeadContext, "sales-1")
        assert context is not None
        context.lead_id = first.lead_id
    third_event_id = persist_outbox_texts(
        session_factory,
        "sales-1",
        ["手机号：13800000002；需求：码垛机器人"],
    )[0]

    updated = service.consume(third_event_id)

    assert updated.status is LeadProcessingStatus.UPDATED
    assert updated.lead_id == second.lead_id
