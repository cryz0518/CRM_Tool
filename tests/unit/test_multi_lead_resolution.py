"""多客户消息拆分、待归属与人工重归属的应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.leads.models import (
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    MessageReassignmentAudit,
)
from app.leads.service import FirstTextLeadWorkspaceService, LeadReassignmentService
from app.messaging.models import Base, IncomingMessage, OutboxEvent, SalesAuthorization
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供验证 T07 归属行为的隔离真实事务数据库。

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


def persist_message(
    session_factory: sessionmaker[Session], message_id: str, sales_user_id: str, text: str
) -> int:
    """持久化一条授权销售的待消费文本消息。

    参数：session_factory 创建事务；message_id、sales_user_id 和 text 构成消息事实。
    返回值：新建 Outbox 事件标识。
    异常：违反数据库约束时由 SQLAlchemy 抛出。
    副作用：必要时新增授权、来源消息和 Outbox 事件。
    """
    with session_factory.begin() as session:
        if session.get(SalesAuthorization, sales_user_id) is None:
            session.add(SalesAuthorization(wecom_user_id=sales_user_id, is_authorized=True))
        sequence = (
            session.scalar(
                select(func.max(OutboxEvent.sequence)).where(
                    OutboxEvent.sales_user_id == sales_user_id
                )
            )
            or 0
        ) + 1
        event = OutboxEvent(message_id=message_id, sales_user_id=sales_user_id, sequence=sequence)
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
                raw_payload={"text": text},
                normalized_text=text,
            )
        )
        session.add(event)
        session.flush()
        return event.id


def test_one_message_with_two_customers_creates_independent_source_resolutions(
    session_factory: sessionmaker[Session],
) -> None:
    """验证多客户消息按分段创建独立线索、表格记录和来源归属。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：任一客户未独立创建或来源引用不完整时由 pytest 报告断言失败。
    副作用：消费一条含两个显式客户分段的 Outbox 事件。
    """
    event_id = persist_message(
        session_factory,
        "message-multi",
        "sales-1",
        "客户：客户甲；联系人：张三\n客户：客户乙；联系人：李四",
    )
    with session_factory.begin() as session:
        message = session.get(IncomingMessage, "message-multi")
        assert message is not None
        # 多客户消息的附件没有明确目标时只保留在来源消息，不作为任一线索的附件字段。
        message.raw_payload = {"attachments": [{"storage_reference": "message-only"}]}
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(session_factory, adapter).consume(event_id)

    assert len(result.lead_ids) == 2
    assert [record.fields["线索名称"] for record in adapter.get_records()] == ["客户甲", "客户乙"]
    assert all("附件" not in record.fields for record in adapter.get_records())
    with session_factory() as session:
        resolutions = session.scalars(
            select(LeadMessageResolution)
            .where(LeadMessageResolution.message_id == "message-multi")
            .order_by(LeadMessageResolution.segment_index)
        ).all()
    assert [(item.segment_index, item.status) for item in resolutions] == [
        (0, "assigned"),
        (1, "assigned"),
    ]
    assert {item.lead_id for item in resolutions} == set(result.lead_ids)


def test_reassignment_is_limited_to_the_source_salesperson_and_only_safely_adds_provenance(
    session_factory: sessionmaker[Session],
) -> None:
    """验证销售不能跨人重归属，且重归属不覆盖新目标已有字段。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：越权、覆盖或审计缺失时由 pytest 报告断言失败。
    副作用：构造错误归属消息后执行一次受控人工重归属。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    source_event_id = persist_message(
        session_factory,
        "message-source",
        "sales-1",
        "客户：客户甲；联系人：张三；手机：13800000001",
    )
    target_event_id = persist_message(
        session_factory, "message-target", "sales-1", "客户：客户乙；联系人：李四"
    )
    source = FirstTextLeadWorkspaceService(session_factory, adapter).consume(source_event_id)
    target = FirstTextLeadWorkspaceService(session_factory, adapter).consume(target_event_id)
    assert source.lead_id is not None
    assert target.lead_id is not None
    with session_factory.begin() as session:
        target_lead = session.get(Lead, target.lead_id)
        assert target_lead is not None
        target_lead.field_values = {**target_lead.field_values, "工艺": "码垛"}
        session.add(
            LeadFieldProvenance(
                lead_id=source.lead_id,
                source_message_id="message-source",
                field_name="工艺",
                value="码垛",
            )
        )
        source_lead = session.get(Lead, source.lead_id)
        assert source_lead is not None
        source_lead.field_values = {**source_lead.field_values, "工艺": "码垛"}

    service = LeadReassignmentService(session_factory)
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-2", is_authorized=True))
    with pytest.raises(PermissionError):
        service.reassign("message-source", 0, target.lead_id, "sales-2", "销售填写错误")
    service.reassign("message-source", 0, target.lead_id, "sales-1", "销售填写错误")

    with session_factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-source",
                LeadMessageResolution.segment_index == 0,
            )
        )
        source_lead = session.get(Lead, source.lead_id)
        target_lead = session.get(Lead, target.lead_id)
        audit = session.scalar(select(MessageReassignmentAudit))
    assert resolution is not None
    assert resolution.lead_id == target.lead_id
    assert source_lead is not None
    assert target_lead is not None
    assert source_lead.field_values["工艺"] == "码垛"
    assert target_lead.field_values["工艺"] == "码垛"
    assert target_lead.field_values["手机"] == "13800000001"
    assert audit is not None
    assert audit.status == "succeeded"
    assert (
        audit.previous_lead_id,
        audit.new_lead_id,
        audit.operator_user_id,
        audit.operator_role,
        audit.reason,
    ) == (
        source.lead_id,
        target.lead_id,
        "sales-1",
        "sales",
        "销售填写错误",
    )


def test_administrator_can_reassign_across_sales_boundaries(
    session_factory: sessionmaker[Session],
) -> None:
    """验证授权目录中的管理员可以受审计地处理跨销售归属。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：管理员权限或审计断言失败时由 pytest 报告。
    副作用：将销售一的来源消息重新归属到销售二的线索。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first_event_id = persist_message(
        session_factory, "message-admin-source", "sales-1", "客户：客户甲"
    )
    second_event_id = persist_message(
        session_factory, "message-admin-target", "sales-2", "客户：客户乙"
    )
    source = FirstTextLeadWorkspaceService(session_factory, adapter).consume(first_event_id)
    target = FirstTextLeadWorkspaceService(session_factory, adapter).consume(second_event_id)
    assert source.lead_id is not None
    assert target.lead_id is not None
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="admin-1",
                is_authorized=False,
                is_administrator=True,
            )
        )

    LeadReassignmentService(session_factory).reassign(
        "message-admin-source", 0, target.lead_id, "admin-1", "管理员纠正归属"
    )

    with session_factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-admin-source",
                LeadMessageResolution.segment_index == 0,
            )
        )
        audit = session.scalar(select(MessageReassignmentAudit))
    assert resolution is not None
    assert resolution.lead_id == target.lead_id
    assert audit is not None
    assert audit.operator_user_id == "admin-1"
    assert audit.operator_role == "administrator"


def test_reassignment_never_overwrites_smart_table_manual_value(
    session_factory: sessionmaker[Session],
) -> None:
    """验证重归属只重算后台来源，绝不覆盖表格里的人工值。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：归属或人工值断言不成立时由 pytest 报告。
    副作用：消费两个线索后将来源消息重新归属到目标线索。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    source_event_id = persist_message(
        session_factory, "message-failed-source", "sales-1", "客户：客户甲；手机：13800000001"
    )
    target_event_id = persist_message(
        session_factory, "message-failed-target", "sales-1", "客户：客户乙"
    )
    source = FirstTextLeadWorkspaceService(session_factory, adapter).consume(source_event_id)
    target = FirstTextLeadWorkspaceService(session_factory, adapter).consume(target_event_id)
    assert source.lead_id is not None
    assert target.lead_id is not None
    with session_factory() as session:
        target_lead = session.get(Lead, target.lead_id)
        assert target_lead is not None
        target_record_id = target_lead.smart_table_record_id
    assert target_record_id is not None
    adapter.update_record(target_record_id, {"手机": "13900000002"})
    LeadReassignmentService(session_factory).reassign(
        "message-failed-source", 0, target.lead_id, "sales-1", "销售填写错误"
    )

    with session_factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-failed-source",
                LeadMessageResolution.segment_index == 0,
            )
        )
        audit = session.scalar(select(MessageReassignmentAudit))
    assert resolution is not None
    assert resolution.lead_id == target.lead_id
    assert audit is not None
    assert audit.status == "succeeded"
    assert adapter.get_record(target_record_id).fields["手机"] == "13900000002"
