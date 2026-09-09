"""首条文本线索进入销售个人审核工作区的应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.leads.models import Lead, LeadFieldProvenance, SmartTableSync
from app.leads.service import FirstTextLeadWorkspaceService, LeadProcessingStatus
from app.messaging.models import (
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    OutboxEvent,
    SalesAuthorization,
)
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供含 T02 与 T05 模型的隔离真实事务数据库。

    参数：无。
    返回值：逐例生成一个 SQLAlchemy 会话工厂。
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


def persist_outbox_text(
    session_factory: sessionmaker[Session],
    *,
    message_id: str,
    sales_user_id: str,
    text: str,
    authorized: bool = True,
) -> int:
    """写入已由 T02 持久化的文本消息和待消费发件箱事件。

    参数：session_factory 创建测试事务；其余参数构成来源消息和授权状态。
    返回值：新建 Outbox 事件的数据库标识。
    异常：违反数据库约束时由 SQLAlchemy 抛出。
    副作用：新增销售授权、原始文本消息和待消费 Outbox 事件。
    """
    with session_factory.begin() as session:
        # 测试故意直接准备 T02 之后的事实，不重复测试 T02 的接收事务。
        session.add(
            SalesAuthorization(
                wecom_user_id=sales_user_id,
                is_authorized=authorized,
                is_active=True,
            )
        )
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=1,
                raw_payload={"text": text},
                normalized_text=text,
            )
        )
        event = OutboxEvent(
            message_id=message_id,
            sales_user_id=sales_user_id,
            sequence=1,
        )
        session.add(event)
        session.flush()
        return event.id


def test_authorized_sales_text_creates_a_personal_review_record(
    session_factory: sessionmaker[Session],
) -> None:
    """验证授权销售的首条有效文本创建线索、来源和个人可见的表格记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：任一业务断言不成立时由 pytest 报告。
    副作用：在测试内创建并同步一条 Mock 智能表格记录。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-1",
        sales_user_id="sales-1",
        text="客户：长广溪智造；联系人：张三；需求：码垛机器人",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(session_factory, adapter).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.lead_id is not None
    assert result.smart_table_record_id is not None
    record = adapter.get_record(result.smart_table_record_id)
    assert record is not None
    assert record.fields == {
        "线索名称": "长广溪智造",
        "联系人": "张三",
        "工艺": "码垛",
        "线索来源": "展会",
        "创建人": "sales-1",
        "负责人": "sales-1",
    }
    with session_factory() as session:
        lead = session.get(Lead, result.lead_id)
        provenance = session.scalars(
            select(LeadFieldProvenance).where(LeadFieldProvenance.lead_id == result.lead_id)
        ).all()
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == result.lead_id)
        )
        event = session.get(OutboxEvent, event_id)
        audits = session.scalars(
            select(BusinessAuditEvent.event_type).where(
                BusinessAuditEvent.message_id == "message-1"
            )
        ).all()

    assert lead is not None
    assert lead.original_capturing_sales_user_id == "sales-1"
    assert lead.smart_table_owner_user_id == "sales-1"
    assert lead.field_values == {
        "线索名称": "长广溪智造",
        "联系人": "张三",
        "工艺": "码垛",
        "线索来源": "展会",
    }
    assert {source.field_name for source in provenance} == {"线索名称", "联系人", "工艺"}
    assert sync is not None
    assert sync.status == "succeeded"
    assert sync.smart_table_record_id == result.smart_table_record_id
    assert event is not None
    assert event.status == "succeeded"
    assert set(audits) >= {"lead_created", "smart_table_record_created"}


def test_non_lead_text_is_ignored_without_polluting_the_review_workspace(
    session_factory: sessionmaker[Session],
) -> None:
    """验证普通问候不会创建线索、表格记录或同步结果。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：任一污染断言失败时由 pytest 报告。
    副作用：消费一条被确定性提取器忽略的 Outbox 事件。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-2",
        sales_user_id="sales-1",
        text="你好，机器人",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(session_factory, adapter).consume(event_id)

    assert result.status is LeadProcessingStatus.IGNORED
    assert adapter.get_records() == []
    with session_factory() as session:
        assert session.query(Lead).count() == 0
        assert session.query(SmartTableSync).count() == 0


def test_outbox_consumer_rechecks_sales_authorization_before_creating_a_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证即使存在异常发件箱，未授权成员也不能产生任何线索或表格副作用。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：权限或副作用断言失败时由 pytest 报告。
    副作用：消费一条故意模拟的未授权 Outbox 事件。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-3",
        sales_user_id="visitor-1",
        text="客户：不应写入；联系人：李四",
        authorized=False,
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(session_factory, adapter).consume(event_id)

    assert result.status is LeadProcessingStatus.UNAUTHORIZED
    assert adapter.get_records() == []
    with session_factory() as session:
        assert session.query(Lead).count() == 0


def test_consuming_the_same_succeeded_event_is_idempotent(
    session_factory: sessionmaker[Session],
) -> None:
    """验证重复消费同一已成功发件箱不会新增第二条线索或表格记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：幂等性断言失败时由 pytest 报告。
    副作用：连续两次消费同一 Outbox 事件。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-4",
        sales_user_id="sales-1",
        text="客户：长广溪智造；联系人：张三",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    first = service.consume(event_id)
    duplicate = service.consume(event_id)

    assert first.status is LeadProcessingStatus.CREATED
    assert duplicate.status is LeadProcessingStatus.ALREADY_PROCESSED
    assert duplicate.lead_id == first.lead_id
    assert adapter.get_records() == [adapter.get_record(first.smart_table_record_id)]


def test_same_company_from_two_sales_creates_two_personal_review_records(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 T05 不做跨销售去重，两位销售各自获得同公司的隔离审核记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：销售隔离断言失败时由 pytest 报告。
    副作用：为不同销售分别创建同公司的 Mock 表格记录。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-5",
        sales_user_id="sales-1",
        text="客户：长广溪智造；联系人：张三",
    )
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="message-6",
        sales_user_id="sales-2",
        text="客户：长广溪智造；联系人：李四",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    first = service.consume(first_event_id)
    second = service.consume(second_event_id)

    assert first.status is LeadProcessingStatus.CREATED
    assert second.status is LeadProcessingStatus.CREATED
    assert first.lead_id != second.lead_id
    assert [record.fields["负责人"] for record in adapter.get_records()] == ["sales-1", "sales-2"]


def test_smart_table_failure_can_be_consumed_again_without_creating_a_second_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证表格写入失败保留可重试事实，后续消费只补写原线索。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：重试或幂等性断言失败时由 pytest 报告。
    副作用：先模拟一次 Adapter 外部失败，再消费相同事件完成写入。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-7",
        sales_user_id="sales-1",
        text="客户：长广溪智造；联系人：张三",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    with patch.object(adapter, "create_record", side_effect=RuntimeError("temporary")):
        failed = service.consume(event_id)
    retried = service.consume(event_id)

    assert failed.status is LeadProcessingStatus.SYNC_FAILED
    assert retried.status is LeadProcessingStatus.CREATED
    assert adapter.get_records() == [adapter.get_record(retried.smart_table_record_id)]
    with session_factory() as session:
        assert session.query(Lead).count() == 1
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == retried.lead_id)
        )
        event = session.get(OutboxEvent, event_id)

    assert sync is not None
    assert sync.status == "succeeded"
    assert event is not None
    assert event.status == "succeeded"


def test_mismatched_outbox_sales_identity_cannot_create_another_sales_record(
    session_factory: sessionmaker[Session],
) -> None:
    """验证异常 Outbox 不能把一名销售的来源消息伪装成另一名销售的记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：身份隔离断言失败时由 pytest 报告。
    副作用：篡改测试 Outbox 的销售标识后尝试消费。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-8",
        sales_user_id="sales-1",
        text="客户：长广溪智造；联系人：张三",
    )
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-2", is_authorized=True, is_active=True))
        event = session.get(OutboxEvent, event_id)
        assert event is not None
        event.sales_user_id = "sales-2"
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(session_factory, adapter).consume(event_id)

    assert result.status is LeadProcessingStatus.INVALID_EVENT
    assert adapter.get_records() == []
    with session_factory() as session:
        assert session.query(Lead).count() == 0


def test_unknown_processing_result_is_not_replayed_into_a_duplicate_table_record(
    session_factory: sessionmaker[Session],
) -> None:
    """验证外部成功但本地回写中断的未知结果不会被重放成重复表格记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：未知结果保护断言失败时由 pytest 报告。
    副作用：构造已进入 processing 的既有 Lead 和同步事实后再次消费事件。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-9",
        sales_user_id="sales-1",
        text="客户：长广溪智造；联系人：张三",
    )
    with session_factory.begin() as session:
        lead = Lead(
            source_message_id="message-9",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            field_values={"线索名称": "长广溪智造", "联系人": "张三", "线索来源": "展会"},
        )
        session.add(lead)
        session.flush()
        lead_id = lead.id
        session.add(SmartTableSync(lead_id=lead_id, source_message_id="message-9"))
        event = session.get(OutboxEvent, event_id)
        assert event is not None
        event.status = "processing"
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(session_factory, adapter).consume(event_id)

    assert result.status is LeadProcessingStatus.ALREADY_PROCESSED
    assert result.lead_id == lead_id
    assert adapter.get_records() == []
