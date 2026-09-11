"""首条文本线索进入销售个人审核工作区的应用服务测试。"""

from __future__ import annotations

import json
from collections.abc import Generator
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ai.gateway import AIGateway
from app.ai.provider import LLMProviderError, MockLLMProvider
from app.companies.models import QCCCandidate, QCCLookupResult
from app.companies.service import CompanyLeadService, MockQCCAdapter
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
        authorization = session.get(SalesAuthorization, sales_user_id)
        if authorization is None:
            session.add(
                SalesAuthorization(
                    wecom_user_id=sales_user_id,
                    is_authorized=authorized,
                    is_active=True,
                )
            )
        sequence = (
            session.scalar(
                select(func.max(IncomingMessage.sequence)).where(
                    IncomingMessage.sales_user_id == sales_user_id
                )
            )
            or 0
        ) + 1
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
        "备注": (
            "基本信息：长广溪智造；城市、主要产品、年销售额、所属行业未提供。\n"
            "线索需求：未提供。\n"
            "预算情况：未提供。\n"
            "特殊要求：未提供。"
        ),
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
        "备注": (
            "基本信息：长广溪智造；城市、主要产品、年销售额、所属行业未提供。\n"
            "线索需求：未提供。\n"
            "预算情况：未提供。\n"
            "特殊要求：未提供。"
        ),
    }
    assert {source.field_name for source in provenance} == {"线索名称", "联系人", "工艺", "备注"}
    assert sync is not None
    assert sync.status == "succeeded"
    assert sync.smart_table_record_id == result.smart_table_record_id
    assert event is not None
    assert event.status == "succeeded"
    assert set(audits) >= {"lead_created", "smart_table_record_created"}


def test_consumer_upgrades_temporary_lead_when_later_message_names_the_company(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 T10 已接入消费者，联系人临时线索可由后续公司名升级。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：未升级同一草稿、未采用 QCC 标准名或表格未增量更新时由 pytest 报告。
    副作用：连续消费同销售两条消息并调用 Mock QCC。
    """
    temporary_event_id = persist_outbox_text(
        session_factory,
        message_id="temporary-contact",
        sales_user_id="sales-1",
        text="联系人：张三；手机：13800000001",
    )
    company_event_id = persist_outbox_text(
        session_factory,
        message_id="temporary-company",
        sales_user_id="sales-1",
        text="公司：长广溪；电话：0510-12345678",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(
        session_factory,
        adapter,
        MockQCCAdapter(
            {
                "长广溪": QCCLookupResult.matched(
                    QCCCandidate("无锡长广溪智能制造有限公司", "qcc-1")
                )
            }
        ),
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, company_lead_service=company_service
    )

    temporary = service.consume(temporary_event_id)

    assert temporary.status is LeadProcessingStatus.CREATED
    with session_factory() as session:
        lead = session.get(Lead, temporary.lead_id)
        company_event = session.get(OutboxEvent, company_event_id)
    assert lead is not None
    assert company_event is not None
    assert company_event.status == "succeeded"
    assert lead.standard_company_name == "无锡长广溪智能制造有限公司"
    assert lead.field_values["电话"] == "0510-12345678"
    record = adapter.get_record(lead.smart_table_record_id or "")
    assert record is not None
    assert record.fields["线索名称"] == "无锡长广溪智能制造有限公司"


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


def test_free_text_uses_t08_then_t09_with_real_sales_identity(
    session_factory: sessionmaker[Session],
) -> None:
    """验证自由文本经 T08 校验后由 T09 写入审核表，权限字段始终来自真实销售。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：网关接入、置信度分流或销售身份断言失败时由 pytest 报告。
    副作用：消费一条自由文本并创建一条带 AI待确认 的 Mock 表格记录。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-first",
        sales_user_id="sales-1",
        text="刚和长广溪智造聊过，他们想做协作机器人装配。",
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {},
                    "crm_fields": {
                        "线索名称": "长广溪智造",
                        "业务线": "协作机器人",
                        "工艺": "装配",
                    },
                    "enrichment": {},
                    "confidence_by_field": {"线索名称": 0.95, "业务线": 0.9, "工艺": 0.7},
                    "conflicts": [],
                    "warnings": [],
                }
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert len(provider.requests) == 1
    assert result.smart_table_record_id is not None
    record = adapter.get_record(result.smart_table_record_id)
    assert record is not None
    assert record.fields["线索名称"] == "长广溪智造"
    assert record.fields["业务线"] == "协作机器人"
    assert record.fields["工艺"] == "装配"
    assert record.fields["AI待确认"] == ["工艺"]
    assert record.fields["创建人"] == "sales-1"
    assert record.fields["负责人"] == "sales-1"
    with session_factory() as session:
        lead = session.get(Lead, result.lead_id)
    assert lead is not None
    assert lead.original_capturing_sales_user_id == "sales-1"
    assert lead.smart_table_owner_user_id == "sales-1"
    assert lead.field_values["线索名称"] == "长广溪智造"
    assert lead.field_values["工艺"] == "装配"


def test_free_text_ai_failure_is_a_checkpoint_without_creating_a_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 AI 传输失败不会伪装建档成功，并允许同销售后续消息继续消费。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：失败状态、审计或后续消费断言失败时由 pytest 报告。
    副作用：消费失败自由文本后自动消费同销售的确定性后续消息。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-failure",
        sales_user_id="sales-1",
        text="这是无法送达模型的自由文本。",
    )
    with session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-ai-after",
                sales_user_id="sales-1",
                sequence=2,
                raw_payload={"text": "客户：后续客户"},
                normalized_text="客户：后续客户",
            )
        )
        session.add(OutboxEvent(message_id="message-ai-after", sales_user_id="sales-1", sequence=2))
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(MockLLMProvider([LLMProviderError("network")])),
    )

    result = service.consume(first_event_id)

    assert result.status is LeadProcessingStatus.SYNC_FAILED
    with session_factory() as session:
        first_event = session.get(OutboxEvent, first_event_id)
        follow_up = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "message-ai-after")
        )
        failed_audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == "message-ai-failure",
                BusinessAuditEvent.event_type == "ai_gateway_failed_pending_review",
            )
        )
        failed_lead = session.scalar(
            select(Lead).where(Lead.source_message_id == "message-ai-failure")
        )
    assert first_event is not None
    assert first_event.status == "failed_pending_review"
    assert failed_audit is not None
    assert failed_lead is None
    assert follow_up is not None
    assert follow_up.status == "succeeded"
    assert len(adapter.get_records()) == 1


def test_free_text_ai_update_uses_current_context_without_creating_a_second_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 T07 已确定的当前客户上下文可接收 T08/T09 的自由文本增量补充。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：上下文串线、重复建档或审核字段断言失败时由 pytest 报告。
    副作用：先创建确定性首条线索，再消费一条自由文本更新。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-context-first",
        sales_user_id="sales-1",
        text="客户：长广溪智造",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    created = FirstTextLeadWorkspaceService(session_factory, adapter).consume(first_event_id)
    with session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-ai-context-update",
                sales_user_id="sales-1",
                sequence=2,
                raw_payload={"text": "他们现在计划做装配项目。"},
                normalized_text="他们现在计划做装配项目。",
            )
        )
        event = OutboxEvent(
            message_id="message-ai-context-update", sales_user_id="sales-1", sequence=2
        )
        session.add(event)
        session.flush()
        update_event_id = event.id
    gateway = AIGateway(
        MockLLMProvider(
            [
                json.dumps(
                    {
                        "intent": "UPDATE_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"工艺": "装配"},
                        "enrichment": {},
                        "confidence_by_field": {"工艺": 0.9},
                        "conflicts": [],
                        "warnings": [],
                    }
                )
            ]
        )
    )

    updated = FirstTextLeadWorkspaceService(session_factory, adapter, ai_gateway=gateway).consume(
        update_event_id
    )

    assert updated.status is LeadProcessingStatus.UPDATED
    assert updated.lead_id == created.lead_id
    assert len(adapter.get_records()) == 1
    assert created.smart_table_record_id is not None
    record = adapter.get_record(created.smart_table_record_id)
    assert record is not None
    assert record.fields["工艺"] == "装配"
