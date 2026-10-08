"""首条文本线索进入销售个人审核工作区的应用服务测试。"""

from __future__ import annotations

import json
from collections.abc import Generator, Mapping
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ai.gateway import AIGateway
from app.ai.models import ExtractedLeadPatch, LeadAnalysis
from app.ai.provider import LLMProviderError, MockLLMProvider
from app.companies.models import CompanyUpsertCommand, QCCCandidate, QCCLookupResult
from app.companies.service import CompanyLeadService, MockQCCAdapter, MockTYCAdapter
from app.core.failures import RetryableTaskFailure
from app.core.logging import bind_log_context, reset_log_context
from app.leads.models import (
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    SalesLeadContext,
    SmartTableSync,
    serialize_field_value,
)
from app.leads.service import (
    DeterministicFirstTextLeadExtractor,
    FirstTextLeadWorkspaceService,
    LeadProcessingStatus,
)
from app.messaging.models import (
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    MessageAttachment,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableRecord
from app.smart_table.registry import build_required_smart_table_schema
from app.smart_table.wecom_cli import (
    SmartTableWriteVerificationError,
    WecomCliProcessError,
    WecomCliTransportError,
)


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
    sequence: int | None = None,
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
        if sequence is None:
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


class _Synthetic850005Cause(Exception):
    """表示仅用于隔离测试的企业微信 850005 限流原因。"""

    error_code = "rate_limited"
    external_error_code = 850005
    external_error_type = "ApiError"
    http_status = 429


class _ApplyThenRateLimitAdapter(MockSmartTableAdapter):
    """按测试指定次数模拟远端已写入后收到 850005 的暂态更新结果。"""

    def __init__(self) -> None:
        """初始化标准内存表格和一次性暂态错误开关。"""
        super().__init__(schema=build_required_smart_table_schema())
        self.fail_update_count = 0
        self.injected_850005_count = 0

    def update_record(
        self,
        record_id: str,
        fields: Mapping[str, object],
        *,
        skip_preflight: bool = False,
    ) -> SmartTableRecord:
        """写入 Mock 记录后按剩余次数抛出带 850005 的可重试错误。"""
        updated = super().update_record(
            record_id, fields, skip_preflight=skip_preflight
        )
        if self.fail_update_count:
            self.fail_update_count -= 1
            self.injected_850005_count += 1
            try:
                raise _Synthetic850005Cause("synthetic rate limit")
            except _Synthetic850005Cause as cause:
                raise WecomCliTransportError("synthetic transient") from cause
        return updated


def semantic_segments_json(
    segments: list[dict[str, object]], *, intent: str = "MULTI_LEAD"
) -> str:
    """构造测试用语义分段响应，不为模型增加业务目标字段。"""
    return json.dumps(
        {
            "intent": intent,
            "customer_reference": {},
            "crm_fields": {},
            "enrichment": {},
            "confidence_by_field": {},
            "segments": segments,
            "conflicts": [],
            "warnings": [],
        },
        ensure_ascii=False,
    )


def semantic_single_json(
    *,
    intent: str,
    crm_fields: dict[str, object],
    enrichment: dict[str, str] | None = None,
) -> str:
    """构造测试用单客户 AI 分析响应，保持单 Lead API 兼容。"""
    return json.dumps(
        {
            "intent": intent,
            "customer_reference": {},
            "crm_fields": crm_fields,
            "enrichment": enrichment or {},
            "confidence_by_field": {field_name: 0.99 for field_name in crm_fields},
            "conflicts": [],
            "warnings": [],
        },
        ensure_ascii=False,
    )


def test_multiline_labeled_message_keeps_all_fields_in_one_lead() -> None:
    """多行标签应合并为一条线索，不能把联系人等字段吞进公司名。"""
    text = (
        "公司：希捷国际科技（无锡）有限公司\n"
        "联系人：杨总\n"
        "手机：13800000000\n"
        "业务线：协作机器人\n"
        "备注：T21 全流程验收"
    )

    fields = DeterministicFirstTextLeadExtractor().extract_many(text)

    assert fields == [
        {
            "线索名称": "希捷国际科技（无锡）有限公司",
            "联系人": "杨总",
            "手机": "13800000000",
            "业务线": "协作机器人",
            "备注": "T21 全流程验收",
        }
    ]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "合肥光曜新能源孙经理，光伏组件，外观缺陷视觉检测",
            {"线索名称": "合肥光曜新能源", "联系人": "孙经理"},
        ),
        (
            "天津华印包装马经理，印刷包装",
            {"线索名称": "天津华印包装", "联系人": "马经理"},
        ),
        (
            "江苏通达电缆董经理，电缆表面绝缘层瑕疵视觉检测",
            {"线索名称": "江苏通达电缆", "联系人": "董经理"},
        ),
        (
            "苏州微视医疗刘博士，医疗器械视觉检测",
            {"线索名称": "苏州微视医疗", "联系人": "刘博士"},
        ),
    ],
)
def test_leading_company_contact_hint_extracts_only_narrow_sales_format(
    text: str, expected: dict[str, str]
) -> None:
    """窄范围识别公司主体与单姓称谓，避免让自由文本公司名依赖模型。"""
    extractor = DeterministicFirstTextLeadExtractor()

    assert extractor.extract_leading_company_contact_hint(text) == expected
    assert extractor.extract_leading_company_hint(text) == expected["线索名称"]


def test_leading_company_contact_hint_rejects_demand_text_with_phone() -> None:
    """需求片段即使后文出现手机号，也不能升级为公司身份。"""
    extractor = DeterministicFirstTextLeadExtractor()
    text = "表面缺陷视觉检测，预算28万，电话13752288666，官网SEO，下季度招标。"

    assert extractor.extract_leading_company_contact_hint(text) is None
    assert extractor.extract_leading_company_hint(text) is None


@pytest.mark.parametrize("company_prefix", ["视觉检测", "装配", "预算120万"])
def test_leading_company_contact_hint_rejects_registered_process_or_demand_prefix(
    company_prefix: str,
) -> None:
    """注册工艺或明显需求前缀不能仅凭联系人称谓升级为公司。"""
    text = f"{company_prefix}孙经理，客户现场补充信息"

    assert DeterministicFirstTextLeadExtractor().extract_leading_company_contact_hint(text) is None


def test_leading_company_contact_hint_allows_business_word_with_legal_suffix() -> None:
    """公司法律主体后缀是强身份证据，不因包含业务词而误拒。"""
    text = "视觉检测设备有限公司孙经理，设备外观检测"

    assert DeterministicFirstTextLeadExtractor().extract_leading_company_contact_hint(text) == {
        "线索名称": "视觉检测设备有限公司",
        "联系人": "孙经理",
    }


def test_four_message_natural_language_routing_stays_on_two_leads(
    session_factory: sessionmaker[Session],
) -> None:
    """验证真实四消息结构稳定得到 A/A/B/B，且只建立两条销售线索。"""
    messages = (
        (
            "message-real-a1",
            "合肥光曜新能源孙经理，光伏组件，外观缺陷视觉检测",
        ),
        (
            "message-real-a2",
            "手机号17315865903，预算120万，SNEC展会收的名片，今年内定标。",
        ),
        ("message-real-b1", "天津华印包装马经理，印刷包装"),
        (
            "message-real-b2",
            "表面缺陷视觉检测，预算28万，电话13752288666，官网SEO，下季度招标。",
        ),
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {},
                    "crm_fields": {"工艺": "视觉检测"},
                    "enrichment": {"预算": "预算120万"},
                    "confidence_by_field": {"工艺": 0.95},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {},
                    "crm_fields": {"电话": "13752288666"},
                    "enrichment": {"预算": "预算28万"},
                    "confidence_by_field": {"电话": 0.99},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {},
                    "crm_fields": {
                        "线索名称": "天津华印包装",
                        "联系人": "马经理",
                    },
                    "enrichment": {},
                    "confidence_by_field": {
                        "线索名称": 0.99,
                        "联系人": 0.95,
                    },
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {},
                    "crm_fields": {"电话": "13752288666"},
                    "enrichment": {},
                    "confidence_by_field": {"电话": 0.99},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(
        session_factory,
        adapter,
        MockTYCAdapter(
            {
                "合肥光曜新能源": QCCLookupResult.matched(
                    QCCCandidate("合肥光曜新能源有限公司", "qcc-a")
                ),
                "天津华印包装": QCCLookupResult.matched(
                    QCCCandidate("天津华印包装有限公司", "qcc-b")
                ),
            }
        ),
    )
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
        company_lead_service=company_service,
    )

    # 逐条持久化并消费，避免测试辅助逻辑提前推进同销售的后续发件箱事件。
    results = [
        service.consume(
            persist_outbox_text(
                session_factory,
                message_id=message_id,
                sales_user_id="sales-1",
                text=text,
            )
        )
        for message_id, text in messages
    ]

    assert len(provider.requests) == 2
    assert [result.status for result in results] == [
        LeadProcessingStatus.CREATED,
        LeadProcessingStatus.UPDATED,
        LeadProcessingStatus.CREATED,
        LeadProcessingStatus.UPDATED,
    ]
    assert results[0].lead_id is not None
    assert results[1].lead_id == results[0].lead_id
    assert results[2].lead_id is not None
    assert results[2].lead_id != results[0].lead_id
    assert results[3].lead_id == results[2].lead_id

    with session_factory() as session:
        leads = session.scalars(select(Lead).order_by(Lead.created_at)).all()
        resolutions = session.scalars(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id.in_(
                    ("message-real-a1", "message-real-a2", "message-real-b1", "message-real-b2")
                )
            )
        ).all()
        context = session.get(SalesLeadContext, "sales-1")

    assert len(leads) == 2
    resolution_by_message = {item.message_id: item.lead_id for item in resolutions}
    assert resolution_by_message == {
        "message-real-a1": results[0].lead_id,
        "message-real-a2": results[0].lead_id,
        "message-real-b1": results[2].lead_id,
        "message-real-b2": results[2].lead_id,
    }
    assert context is not None
    assert context.lead_id == results[2].lead_id
    assert context.last_message_sequence == 4


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
        "工艺": ["码垛"],
        "是否为国际客户": "国内",
        "线索来源": "展会",
        "创建人": "sales-1",
        "负责人": "sales-1",
        "提交状态": "未提交",
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
        "工艺": ["码垛"],
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
    assert temporary.smart_table_record_id is None
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


def test_consumer_prefills_first_ambiguous_tyc_candidate_and_marks_name_pending(
    session_factory: sessionmaker[Session],
) -> None:
    """验证多候选公司在工作区首项预填且不发行候选确认卡。"""
    event_id = persist_outbox_text(
        session_factory,
        message_id="ambiguous-company-workspace",
        sales_user_id="sales-1",
        text="公司：上海智造；联系人：张三；手机：13800000001",
    )
    candidates = (
        QCCCandidate("上海智造有限公司", "qcc-1"),
        QCCCandidate("上海智造科技有限公司", "qcc-2"),
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(
        session_factory,
        adapter,
        MockQCCAdapter({"上海智造": QCCLookupResult.ambiguous(candidates)}),
    )

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, company_lead_service=company_service
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.smart_table_record_id is not None
    record = adapter.get_record(result.smart_table_record_id)
    assert record is not None
    assert record.fields["线索名称"] == "上海智造有限公司"
    assert record.fields["AI待确认"] == ["线索名称"]
    with session_factory() as session:
        lead = session.get(Lead, result.lead_id)
        audits = session.scalars(
            select(BusinessAuditEvent.event_type).where(
                BusinessAuditEvent.message_id == "ambiguous-company-workspace"
            )
        ).all()
    assert lead is not None
    assert lead.standard_company_name == "上海智造有限公司"
    assert lead.tyc_customer_id is None
    assert lead.company_verification_status == "company_unverified"
    assert "company_tyc_ambiguous_first_candidate_selected" in audits


def test_consumer_repairs_stale_multiline_company_name_before_tyc_resolution(
    session_factory: sessionmaker[Session],
) -> None:
    """验证旧多行公司值只在 temporary 草稿中被新干净名称修复。"""
    old_text = "公司：旧名称\n联系人：旧联系人\n手机：13800000000"
    persist_outbox_text(
        session_factory,
        message_id="stale-company-source",
        sales_user_id="sales-1",
        text=old_text,
    )
    with session_factory.begin() as session:
        old_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "stale-company-source")
        )
        assert old_event is not None
        old_event.status = "succeeded"
        old_message = session.get(IncomingMessage, "stale-company-source")
        assert old_message is not None
        lead = Lead(
            source_message_id=old_message.message_id,
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            lifecycle_state="temporary",
            field_values={"线索名称": old_text},
        )
        session.add(lead)
        session.flush()
        session.add(
            SalesLeadContext(
                sales_user_id="sales-1",
                lead_id=lead.id,
                last_message_received_at=old_message.received_at,
            )
        )

    new_event_id = persist_outbox_text(
        session_factory,
        message_id="stale-company-repair",
        sales_user_id="sales-1",
        text="公司：希捷国际科技（无锡）有限公司\n联系人：杨总\n手机：13800000000",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        company_lead_service=CompanyLeadService(
            session_factory,
            adapter,
            MockQCCAdapter(
                {
                    "希捷国际科技（无锡）有限公司": QCCLookupResult.matched(
                        QCCCandidate("希捷国际科技（无锡）有限公司", "tyc-1")
                    )
                }
            ),
        ),
    )

    result = service.consume(new_event_id)

    assert result.status in {LeadProcessingStatus.CREATED, LeadProcessingStatus.UPDATED}
    assert result.smart_table_record_id is not None
    with session_factory() as session:
        repaired = session.get(Lead, result.lead_id)
        audits = session.scalars(
            select(BusinessAuditEvent.event_type).where(
                BusinessAuditEvent.message_id == "stale-company-repair"
            )
        ).all()
    assert repaired is not None
    assert repaired.field_values["线索名称"] == "希捷国际科技（无锡）有限公司"
    assert repaired.standard_company_name == "希捷国际科技（无锡）有限公司"
    assert "temporary_lead_stale_company_name_repaired" in audits


def test_consumer_keeps_same_sales_company_as_a_new_smart_table_record(
    session_factory: sessionmaker[Session],
) -> None:
    """验证同销售重复公司在首个 Smart Table 副作用前定位既有 Lead。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：产生第二个有效 Lead 或第二条表格记录时由 pytest 报告。
    副作用：连续消费同销售两条可由 Mock QCC 唯一核验的公司消息。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="same-sales-company-first",
        sales_user_id="sales-1",
        text="公司：长广溪；联系人：张三；手机：13800000001",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        company_lead_service=CompanyLeadService(
            session_factory,
            adapter,
            MockQCCAdapter(
                {
                    "长广溪": QCCLookupResult.matched(
                        QCCCandidate("无锡长广溪智能制造有限公司", "qcc-1")
                    )
                }
            ),
        ),
    )

    first = service.consume(first_event_id)
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="same-sales-company-second",
        sales_user_id="sales-1",
        text="公司：长广溪；电话：0510-12345678",
    )
    second = service.consume(second_event_id)

    assert first.smart_table_record_id is not None
    assert second.lead_id != first.lead_id
    assert second.smart_table_record_id != first.smart_table_record_id
    assert [record.record_id for record in adapter.get_records()] == [
        "mock-record-1",
        "mock-record-2",
    ]
    with session_factory() as session:
        leads = session.scalars(
            select(Lead).where(Lead.smart_table_owner_user_id == "sales-1")
        ).all()
    assert len(leads) == 2
    assert {lead.standard_company_name for lead in leads} == {"无锡长广溪智能制造有限公司"}
    assert any(lead.field_values.get("电话") == "0510-12345678" for lead in leads)


def test_consumer_keeps_same_company_isolated_between_salespeople(
    session_factory: sessionmaker[Session],
) -> None:
    """验证不同销售的相同标准公司名不查询、不合并且各自拥有表格记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：跨销售复用 Lead 或 Smart Table record 时由 pytest 报告。
    副作用：消费两名销售各自的同公司消息。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="cross-sales-company-first",
        sales_user_id="sales-1",
        text="公司：长广溪；联系人：张三",
    )
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="cross-sales-company-second",
        sales_user_id="sales-2",
        text="公司：长广溪；联系人：李四",
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

    first = service.consume(first_event_id)
    second = service.consume(second_event_id)

    assert first.lead_id != second.lead_id
    assert first.smart_table_record_id != second.smart_table_record_id
    assert len(adapter.get_records()) == 2
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Lead)) == 2


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


def test_outbox_consumer_ignores_legacy_authorization_flag_before_creating_a_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证异常发件箱中的 active actor 不再受历史授权字段阻断。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：actor 语义或副作用断言失败时由 pytest 报告。
    副作用：消费一条 is_authorized=false 但 active 的 Outbox 事件。
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

    assert result.status is LeadProcessingStatus.CREATED
    assert adapter.get_records()
    with session_factory() as session:
        assert session.query(Lead).count() == 1


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

    with patch.object(adapter, "create_record", side_effect=ConnectionError("temporary")):
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


def test_retrying_ai_record_creation_does_not_block_followups_or_create_duplicate_records(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 Smart Table 瞬态失败不阻塞后续消息，并最终只保留同一条记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：瞬态失败阻塞后续消息、丢失同步状态或重复创建记录时由 pytest 报告。
    副作用：模拟首条 AI 线索创建失败，随后用两条补充消息恢复同一条远端记录。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-smart-table-recovery-a",
        sales_user_id="sales-1",
        text="刚接触到恢复客户，首条需求待补充",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                json.dumps(
                    {
                        "intent": "NEW_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"线索名称": "恢复客户", "联系人": "张三"},
                        "enrichment": {},
                        "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                json.dumps(
                    {
                        "intent": "UPDATE_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"工艺": "装配"},
                        "enrichment": {},
                        "confidence_by_field": {"工艺": 0.95},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                json.dumps(
                    {
                        "intent": "UPDATE_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"业务线": "协作机器人"},
                        "enrichment": {},
                        "confidence_by_field": {"业务线": 0.95},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                json.dumps(
                    {
                        "intent": "NEW_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"线索名称": "恢复客户", "联系人": "张三"},
                        "enrichment": {},
                        "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
            ]
        )
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original_create = adapter.create_record
    create_attempts = 0

    def fail_first_create(fields: dict[str, object], **kwargs: object):
        """让首次建行失败一次，随后恢复到真实 Mock 建行行为。"""
        nonlocal create_attempts
        create_attempts += 1
        if create_attempts == 1:
            raise RetryableTaskFailure("temporary smart table failure")
        return original_create(fields, **kwargs)

    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=gateway,
    )
    with patch.object(adapter, "create_record", side_effect=fail_first_create):
        first = service.consume(first_event_id)

        with session_factory() as session:
            first_event = session.get(OutboxEvent, first_event_id)
            first_sync = session.scalar(
                select(SmartTableSync).where(SmartTableSync.lead_id == first.lead_id)
            )
        assert first.status is LeadProcessingStatus.SYNC_FAILED
        assert first_event is not None and first_event.status == "retrying"
        assert first_sync is not None and first_sync.status == "retrying"

        second_event_id = persist_outbox_text(
            session_factory,
            message_id="message-smart-table-recovery-b",
            sales_user_id="sales-1",
            text="补充该客户的装配工艺需求",
        )
        second = service.consume(second_event_id)

        third_event_id = persist_outbox_text(
            session_factory,
            message_id="message-smart-table-recovery-c",
            sales_user_id="sales-1",
            text="补充该客户的业务线",
        )
        third = service.consume(third_event_id)

    assert first.lead_id is not None
    assert second.status is LeadProcessingStatus.UPDATED
    assert second.lead_id == first.lead_id
    assert third.status is LeadProcessingStatus.UPDATED
    assert third.lead_id == first.lead_id
    assert create_attempts == 2
    assert len(adapter.get_records()) == 1

    # 失败消息恢复时只能复用已经绑定的 Lead/record，不能再新增第二行。
    replayed = service.consume(first_event_id)
    assert replayed.lead_id == first.lead_id
    assert len(adapter.get_records()) == 1

    with session_factory() as session:
        sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == first.lead_id))
        first_event = session.get(OutboxEvent, first_event_id)
        second_event = session.get(OutboxEvent, second_event_id)
        third_event = session.get(OutboxEvent, third_event_id)
    assert sync is not None and sync.status == "succeeded"
    assert first_event is not None and first_event.status == "succeeded"
    assert second_event is not None and second_event.status == "succeeded"
    assert third_event is not None and third_event.status == "succeeded"


def test_existing_lead_without_record_reenters_create_path_after_transient_failure(
    session_factory: sessionmaker[Session],
) -> None:
    """验证已有 Lead 缺少 Smart Table record 时，后续同线索消息仍能重新建行。"""
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-missing-record-recovery-a",
        sales_user_id="sales-1",
        text="客户：缺记录恢复客户；联系人：张三",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original_create = adapter.create_record
    create_attempts = 0

    def fail_first_create(fields: dict[str, object], **kwargs: object):
        """让首条消息的建行失败，验证后续消息能重新进入建行流程。"""
        nonlocal create_attempts
        create_attempts += 1
        if create_attempts == 1:
            raise RetryableTaskFailure("temporary smart table failure")
        return original_create(fields, **kwargs)

    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    with patch.object(adapter, "create_record", side_effect=fail_first_create):
        first = service.consume(first_event_id)
        with session_factory() as session:
            first_lead = session.get(Lead, first.lead_id)
        assert first_lead is not None
        assert first_lead.field_values.get("线索名称") == "缺记录恢复客户"
        with session_factory.begin() as session:
            first_event = session.get(OutboxEvent, first_event_id)
            first_sync = session.scalar(
                select(SmartTableSync).where(SmartTableSync.lead_id == first.lead_id)
            )
            assert first_event is not None and first_sync is not None
            first_event.status = "failed_pending_review"
            first_sync.status = "failed_pending_review"
        second_event_id = persist_outbox_text(
            session_factory,
            message_id="message-missing-record-recovery-b",
            sales_user_id="sales-1",
            text="客户：缺记录恢复客户；需求：装配",
        )
        second = service.consume(second_event_id)

    assert first.status is LeadProcessingStatus.SYNC_FAILED
    assert second.status is LeadProcessingStatus.UPDATED
    assert second.lead_id == first.lead_id
    assert create_attempts == 2
    assert len(adapter.get_records()) == 1
    record = adapter.get_records()[0]
    assert record.fields["线索名称"] == "缺记录恢复客户"
    assert record.fields["联系人"] == "张三"
    assert record.fields["工艺"] == ["装配"]


def test_smart_table_failure_pins_deterministic_lead_context_before_retry(
    session_factory: sessionmaker[Session],
) -> None:
    """验证确定性首录在表格失败后仍保留消息归属和销售上下文。"""
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-deterministic-context-pin",
        sales_user_id="sales-1",
        text="客户：表格失败上下文客户；联系人：张三",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    with patch.object(
        adapter,
        "create_record",
        side_effect=WecomCliProcessError(
            "safe cli failure",
            error_code="remote_business_error",
            external_error_code=640027,
        ),
    ):
        failed = service.consume(event_id)

    assert failed.status is LeadProcessingStatus.SYNC_FAILED
    assert failed.lead_id is not None
    with session_factory() as session:
        context = session.get(SalesLeadContext, "sales-1")
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-deterministic-context-pin"
            )
        )
        failure_audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == "message-deterministic-context-pin",
                BusinessAuditEvent.event_type == "smart_table_sync_failed_pending_review",
            )
        )
        pin_audit = session.scalar(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.message_id == "message-deterministic-context-pin",
                BusinessAuditEvent.event_type == "lead_routing_context_pinned",
            )
        )
    assert context is not None and context.lead_id == failed.lead_id
    assert context is not None and context.last_message_sequence == 1
    assert resolution is not None and resolution.lead_id == failed.lead_id
    assert resolution.status == "processing"
    assert pin_audit is not None and pin_audit.details == {"lead_id": failed.lead_id}
    assert failure_audit is not None
    assert failure_audit.details["failure_category"] == "permanent"
    assert failure_audit.details["failure_code"] == "640027"


def test_remark_failure_after_record_creation_recovers_without_sticking(
    session_factory: sessionmaker[Session],
) -> None:
    """验证表格记录已创建但 T09 备注失败后，后续消费会恢复而非永久停留 retrying。"""
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-remark-recovery",
        sales_user_id="sales-1",
        text="客户：备注恢复客户；联系人：张三",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)

    with patch.object(adapter, "update_record", side_effect=ConnectionError("temporary")):
        failed = service.consume(event_id)
    retried = service.consume(event_id)

    assert failed.status is LeadProcessingStatus.SYNC_FAILED
    assert retried.status is LeadProcessingStatus.CREATED
    assert len(adapter.get_records()) == 1
    with session_factory() as session:
        lead = session.scalar(
            select(Lead).where(Lead.source_message_id == "message-remark-recovery")
        )
        sync = (
            session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead.id))
            if lead
            else None
        )
        event = session.get(OutboxEvent, event_id)
    assert lead is not None and sync is not None and sync.status == "succeeded"
    assert event is not None and event.status == "succeeded"


def test_ai_review_transport_failure_retries_same_patch_before_leaving_partial_row(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 AI 首录的暂态表格失败不会留下只有系统字段的半成品行。"""
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-review-retry",
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
                    "confidence_by_field": {"线索名称": 0.95, "业务线": 0.95, "工艺": 0.95},
                    "conflicts": [],
                    "warnings": [],
                }
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original_update = adapter.update_record
    failed_once = True

    def update_with_one_transient_failure(
        record_id: str, fields: dict[str, object], **kwargs: object
    ):
        """让首次补丁写入失败一次，随后执行真实 Mock 增量更新。"""
        nonlocal failed_once
        if failed_once:
            failed_once = False
            raise RetryableTaskFailure("temporary smart table process failure")
        return original_update(record_id, fields, **kwargs)

    with patch.object(adapter, "update_record", side_effect=update_with_one_transient_failure):
        result = FirstTextLeadWorkspaceService(
            session_factory,
            adapter,
            ai_gateway=AIGateway(provider),
        ).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    record = adapter.get_record(result.smart_table_record_id or "")
    assert record is not None
    assert record.fields["线索名称"] == "长广溪智造"
    assert record.fields["业务线"] == "协作机器人"
    assert record.fields["工艺"] == "装配"
    assert record.fields["创建人"] == "sales-1"
    assert record.fields["负责人"] == "sales-1"
    assert len(adapter.get_records()) == 1


def test_later_message_recovers_pending_fields_after_first_smart_table_patch_fails(
    session_factory: sessionmaker[Session],
) -> None:
    """验证首条字段补丁失败后，后续补充仍会恢复同一 Lead 的全部字段。"""
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-pending-fields-first",
        sales_user_id="sales-1",
        text="公司A的林总，需要视觉检测方案",
    )
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="message-pending-fields-second",
        sales_user_id="sales-1",
        text="13500000000",
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {},
                    "crm_fields": {
                        "线索名称": "公司A",
                        "联系人": "林总",
                        "工艺": "视觉检测",
                    },
                    "enrichment": {"需求": "视觉检测方案"},
                    "confidence_by_field": {
                        "线索名称": 0.99,
                        "联系人": 0.99,
                        "工艺": 0.99,
                    },
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {},
                    "crm_fields": {"手机": "13500000000"},
                    "enrichment": {},
                    "confidence_by_field": {"手机": 0.99},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original_update = adapter.update_record
    failed_updates = 0

    def fail_first_patch(record_id: str, fields: dict[str, object], **kwargs: object):
        """让首条消息的两次有限补丁尝试失败，后续消息恢复同一行。"""
        nonlocal failed_updates
        if failed_updates < 2:
            failed_updates += 1
            raise RetryableTaskFailure("temporary smart table patch failure")
        return original_update(record_id, fields, **kwargs)

    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
    )
    with patch.object(adapter, "update_record", side_effect=fail_first_patch):
        first = service.consume(first_event_id)
    with session_factory() as session:
        first_lead = session.get(Lead, first.lead_id)
        first_sources = session.scalars(
            select(LeadFieldProvenance).where(LeadFieldProvenance.lead_id == first.lead_id)
        ).all()
    assert first_lead is not None
    assert first_lead.field_values["线索名称"] == "公司A"
    assert first_lead.field_values["联系人"] == "林总"
    assert first_lead.field_values["工艺"] == "视觉检测"
    assert {source.field_name for source in first_sources} >= {
        "线索名称",
        "联系人",
        "工艺",
    }
    assert all(
        source.last_ai_synced_value is None
        for source in first_sources
        if source.field_name in {"线索名称", "联系人", "工艺"}
    )
    second = service.consume(second_event_id)

    assert first.status is LeadProcessingStatus.SYNC_FAILED
    assert second.status is LeadProcessingStatus.UPDATED
    assert len(adapter.get_records()) == 1
    record = adapter.get_record(second.smart_table_record_id or "")
    assert record is not None
    assert record.fields["线索名称"] == "公司A"
    assert record.fields["联系人"] == "林总"
    assert record.fields["工艺"] == "视觉检测"
    assert record.fields["手机"] == "13500000000"
    with session_factory() as session:
        lead = session.get(Lead, second.lead_id)
    assert lead is not None
    assert lead.field_values["线索名称"] == "公司A"
    assert lead.field_values["联系人"] == "林总"
    assert lead.field_values["工艺"] == "视觉检测"
    assert lead.field_values["手机"] == "13500000000"


def test_same_lead_three_messages_create_once_and_merge_incremental_fields(
    session_factory: sessionmaker[Session],
) -> None:
    """验证同一 Lead 连续三条消息只创建一行并保留所有增量字段。"""
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {},
                    "crm_fields": {"线索名称": "公司三", "联系人": "王总"},
                    "enrichment": {},
                    "confidence_by_field": {"线索名称": 0.99, "联系人": 0.99},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {},
                    "crm_fields": {"手机": "13600000000"},
                    "enrichment": {},
                    "confidence_by_field": {"手机": 0.99},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {},
                    "crm_fields": {"工艺": "视觉检测"},
                    "enrichment": {"预算": "16万"},
                    "confidence_by_field": {"工艺": 0.99},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
    )
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-three-step-first",
        sales_user_id="sales-1",
        text="公司三的王总，需要视觉方案",
    )
    first = service.consume(first_event_id)
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="message-three-step-second",
        sales_user_id="sales-1",
        text="13600000000",
    )
    second = service.consume(second_event_id)
    third_event_id = persist_outbox_text(
        session_factory,
        message_id="message-three-step-third",
        sales_user_id="sales-1",
        text="补充工艺，预算16万",
    )
    third = service.consume(third_event_id)

    assert first.status is LeadProcessingStatus.CREATED
    assert second.status is LeadProcessingStatus.UPDATED
    assert third.status is LeadProcessingStatus.UPDATED
    assert first.lead_id == second.lead_id == third.lead_id
    assert len(adapter.get_records()) == 1
    record = adapter.get_record(first.smart_table_record_id or "")
    assert record is not None
    assert record.fields["线索名称"] == "公司三"
    assert record.fields["联系人"] == "王总"
    assert record.fields["手机"] == "13600000000"
    assert record.fields["工艺"] == "视觉检测"
    with session_factory() as session:
        lead = session.get(Lead, first.lead_id)
    assert lead is not None
    assert lead.field_values["线索名称"] == "公司三"
    assert lead.field_values["联系人"] == "王总"
    assert lead.field_values["手机"] == "13600000000"
    assert lead.field_values["工艺"] == "视觉检测"
    assert lead.enrichment_values["预算"] == "16万"


def test_verification_pending_retries_same_patch_from_remote_record(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 ACK 后核实等待会重读同一行，并避免重复更新或新建。"""
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-verification-pending-retry",
        sales_user_id="sales-1",
        text="公司核实等待测试的林总，需要视觉检测",
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {},
                    "crm_fields": {
                        "线索名称": "公司核实等待测试",
                        "联系人": "林总",
                    },
                    "enrichment": {},
                    "confidence_by_field": {"线索名称": 0.99, "联系人": 0.99},
                    "conflicts": [],
                    "warnings": [],
                }
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    original_update = adapter.update_record
    update_calls = 0

    def update_then_report_pending(
        record_id: str, fields: dict[str, object], **kwargs: object
    ) -> object:
        """先让远端字段落地，再模拟 ACK 后的暂时核实失败。"""
        nonlocal update_calls
        update_calls += 1
        record = original_update(record_id, fields, **kwargs)
        if update_calls == 1:
            raise SmartTableWriteVerificationError(
                tuple(fields), remote_record_id=record_id
            )
        return record

    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
    )
    with patch.object(adapter, "update_record", side_effect=update_then_report_pending):
        result = service.consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert update_calls == 1
    assert len(adapter.get_records()) == 1
    with session_factory() as session:
        event = session.get(OutboxEvent, event_id)
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-verification-pending-retry"
            )
        )
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == resolution.lead_id)
        ) if resolution is not None and resolution.lead_id is not None else None
    assert event is not None and event.status == "succeeded"
    assert event.failure_category is None
    assert event.failure_summary is None
    assert event.failed_at is None
    assert resolution is not None and resolution.status == "assigned"
    assert sync is not None and sync.status == "succeeded"


def test_ai_create_verification_failure_persists_acknowledged_record_id_and_never_readds(
    session_factory: sessionmaker[Session],
) -> None:
    """验证新增 ACK 后核实失败会冻结远端 ID，并保留同一行的可恢复状态。"""
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-create-verification-failed",
        sales_user_id="sales-1",
        text="刚和测试机器人公司聊过，他们想做协作机器人装配。",
    )
    ai_response = json.dumps(
        {
            "intent": "NEW_LEAD",
            "customer_reference": {},
            "crm_fields": {"线索名称": "测试机器人公司"},
            "enrichment": {},
            "confidence_by_field": {"线索名称": 0.95},
            "conflicts": [],
            "warnings": [],
        }
    )
    provider = MockLLMProvider([ai_response, ai_response])
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    create_record = patch.object(
        adapter,
        "create_record",
        side_effect=SmartTableWriteVerificationError(
            ("线索名称",), remote_record_id="acked-record-1"
        ),
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    )

    with create_record as create_spy:
        failed = service.consume(event_id)
        with session_factory() as session:
            first_sync = session.scalar(
                select(SmartTableSync).where(SmartTableSync.lead_id.is_not(None))
            )
            first_event = session.get(OutboxEvent, event_id)
        replay = service.consume(event_id)

    assert failed.status is LeadProcessingStatus.SYNC_FAILED
    assert replay.status is LeadProcessingStatus.SYNC_FAILED
    assert create_spy.call_count == 1
    assert first_sync is not None and first_sync.error_summary == "write_verification_pending"
    assert first_event is not None and first_event.status == "retrying"
    with session_factory() as session:
        lead = session.scalar(
            select(Lead).where(Lead.source_message_id == "message-ai-create-verification-failed")
        )
        sync = (
            session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead.id))
            if lead is not None
            else None
        )
        event = session.get(OutboxEvent, event_id)

    assert lead is not None and lead.smart_table_record_id == "acked-record-1"
    assert sync is not None
    assert sync.smart_table_record_id == "acked-record-1"
    assert sync.status == "retrying"
    assert sync.error_summary == "field_patch_pending"
    assert event is not None and event.status == "retrying"


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


def test_free_text_with_strong_identity_creates_new_lead_when_ai_says_update_without_context(
    session_factory: sessionmaker[Session],
) -> None:
    """验证自然语言首条消息不会因 AI 误判 UPDATE 而进入待归属。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：自然语言首录未创建线索或归属状态错误时由 pytest 报告。
    副作用：模拟 Qwen 返回 UPDATE_LEAD，并验证销售内强身份兜底完成表格写入。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-update-without-context",
        sales_user_id="sales-1",
        text=(
            "国外客户 Acme Robotics，联系人 Alice，邮箱 alice@example.com。"
            "客户需求/痛点：装配工位人工效率低，希望使用协作机器人完成装配。"
        ),
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {"company": "Acme Robotics"},
                    "crm_fields": {
                        "线索名称": "Acme Robotics",
                        "联系人": "Alice",
                        "邮箱": "alice@example.com",
                    },
                    "enrichment": {"客户需求/痛点": "装配工位人工效率低"},
                    "confidence_by_field": {
                        "线索名称": 0.95,
                        "联系人": 0.95,
                        "邮箱": 0.99,
                    },
                    "conflicts": [],
                    "warnings": [],
                }
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(session_factory, adapter, MockQCCAdapter())

    result = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
        company_lead_service=company_service,
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.lead_id is not None
    assert result.smart_table_record_id is not None
    record = adapter.get_record(result.smart_table_record_id)
    assert record is not None
    assert record.fields["线索名称"] == "Acme Robotics"
    assert record.fields["联系人"] == "Alice"
    assert record.fields["邮箱"] == "alice@example.com"
    with session_factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-ai-update-without-context"
            )
        )
    assert resolution is not None
    assert resolution.status == "assigned"
    assert resolution.lead_id == result.lead_id


def test_ai_created_record_is_updated_when_tyc_resolves_company_name(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 AI 先建表格原始名称后，天眼查首候选仍会增量更新同一行。"""
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-tyc-name-update",
        sales_user_id="sales-1",
        text="汇川技术，张华杰经理，17318902311，做电气自动化，主要想做喷涂方面，预算25万左右",
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {"company": "汇川技术"},
                    "crm_fields": {
                        "线索名称": "汇川技术",
                        "联系人": "张华杰",
                        "手机": "17318902311",
                    },
                    "enrichment": {"预算": "预算25万左右"},
                    "confidence_by_field": {
                        "线索名称": 0.95,
                        "联系人": 0.95,
                        "手机": 0.99,
                    },
                    "conflicts": [],
                    "warnings": [],
                }
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(
        session_factory,
        adapter,
        MockTYCAdapter(
            {
                "汇川技术": QCCLookupResult.matched(
                    QCCCandidate("深圳市汇川技术股份有限公司", "tyc-hc")
                )
            }
        ),
    )

    result = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
        company_lead_service=company_service,
    ).consume(event_id)

    assert result.smart_table_record_id is not None
    record = adapter.get_record(result.smart_table_record_id)
    assert record is not None
    assert record.fields["线索名称"] == "深圳市汇川技术股份有限公司"


def test_free_text_company_hint_prevents_context_cross_lead_and_keeps_tyc_pending_name(
    session_factory: sessionmaker[Session],
) -> None:
    """验证连续自由文本客户不会串到当前线索，且多候选名称保留待确认标记。"""
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-company-a",
        sales_user_id="sales-1",
        text="汇川技术，张华杰经理，17318902311，做电气自动化，主要想做喷涂方面，预算25万左右",
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {},
                    "crm_fields": {
                        "线索名称": "汇川技术",
                        "联系人": "张华杰",
                        "手机": "17318902311",
                    },
                    "enrichment": {"预算": "预算25万左右"},
                    "confidence_by_field": {
                        "线索名称": 0.95,
                        "联系人": 0.95,
                        "手机": 0.99,
                    },
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {},
                    "crm_fields": {"联系人": "杨经理", "手机": "17318902085"},
                    "enrichment": {"预算": "预算25万左右"},
                    "confidence_by_field": {"联系人": 0.95, "手机": 0.99},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(
        session_factory,
        adapter,
        MockTYCAdapter(
            {
                "汇川技术": QCCLookupResult.matched(
                    QCCCandidate("深圳市汇川技术股份有限公司", "tyc-hc")
                ),
                "艾利特": QCCLookupResult.ambiguous(
                    (
                        QCCCandidate("艾利特智能机器人股份有限公司", "tyc-alt-1"),
                        QCCCandidate("艾利特机器人科技有限公司", "tyc-alt-2"),
                    )
                ),
            }
        ),
    )
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
        company_lead_service=company_service,
    )

    first = service.consume(first_event_id)
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-company-b",
        sales_user_id="sales-1",
        text="艾利特，杨经理，17318902085，做智能机器人，主要想做视觉检测，预算25万左右",
    )
    second = service.consume(second_event_id)

    assert first.lead_id is not None
    assert second.lead_id is not None
    assert first.lead_id != second.lead_id
    first_record = adapter.get_record(first.smart_table_record_id or "")
    second_record = adapter.get_record(second.smart_table_record_id or "")
    assert first_record is not None
    assert second_record is not None
    assert first_record.fields["联系人"] == "张华杰"
    assert first_record.fields["手机"] == "17318902311"
    assert second_record.fields["联系人"] == "杨经理"
    assert second_record.fields["手机"] == "17318902085"
    assert second_record.fields["线索名称"] == "艾利特智能机器人股份有限公司"
    assert second_record.fields["AI待确认"] == ["线索名称"]


def test_new_company_update_intent_starts_new_lead_and_follow_up_uses_new_context(
    session_factory: sessionmaker[Session],
) -> None:
    """验证未命中历史的新公司不会被 UPDATE_LEAD 错误归入当前线索。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：新公司覆盖旧线索、补充消息创建第三条线索或表格记录数量错误时由 pytest 报告。
    副作用：模拟三条连续销售消息，并写入测试用智能表格记录与归属事实。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-routing-company-a",
        sales_user_id="sales-1",
        text="公司A的张经理，需要协作机器人方案",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                json.dumps(
                    {
                        "intent": "NEW_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"线索名称": "公司A", "联系人": "张经理"},
                        "enrichment": {},
                        "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                json.dumps(
                    {
                        "intent": "UPDATE_LEAD",
                        "customer_reference": {},
                        "crm_fields": {
                            "线索名称": "公司B",
                            "联系人": "董经理",
                            "工艺": "视觉检测",
                        },
                        "enrichment": {},
                        "confidence_by_field": {
                            "线索名称": 0.99,
                            "联系人": 0.95,
                            "工艺": 0.95,
                        },
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                json.dumps(
                    {
                        "intent": "UPDATE_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"手机": "13800000000"},
                        "enrichment": {"预算": "预算38万", "线索来源": "电缆行业展会"},
                        "confidence_by_field": {"手机": 0.99},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
            ]
        )
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter, ai_gateway=gateway)

    # 先消费首条消息，避免同销售顺序消费器自动推进尚未建立的后续消息。
    first = service.consume(first_event_id)

    second_event_id = persist_outbox_text(
        session_factory,
        message_id="message-routing-company-b",
        sales_user_id="sales-1",
        text="公司B的董经理，电缆表面绝缘层瑕疵视觉检测",
    )
    second = service.consume(second_event_id)

    third_event_id = persist_outbox_text(
        session_factory,
        message_id="message-routing-company-b-follow-up",
        sales_user_id="sales-1",
        text="预算38万，测试手机号，电缆行业展会，预计今年底",
    )
    third = service.consume(third_event_id)

    assert first.status is LeadProcessingStatus.CREATED
    assert second.status is LeadProcessingStatus.CREATED
    assert third.status is LeadProcessingStatus.UPDATED
    assert first.lead_id is not None
    assert second.lead_id is not None
    assert third.lead_id == second.lead_id
    assert first.lead_id != second.lead_id
    assert first.smart_table_record_id != second.smart_table_record_id

    with session_factory() as session:
        # 数据库中应只有两条线索，且 A 的公司字段不能被 B 的新公司覆盖。
        leads = session.scalars(select(Lead).order_by(Lead.created_at)).all()
        assert len(leads) == 2
        assert {lead.field_values.get("线索名称") for lead in leads} == {"公司A", "公司B"}
        resolutions = {
            resolution.message_id: resolution.lead_id
            for resolution in session.scalars(
                select(LeadMessageResolution).where(
                    LeadMessageResolution.message_id.in_(
                        (
                            "message-routing-company-a",
                            "message-routing-company-b",
                            "message-routing-company-b-follow-up",
                        )
                    )
                )
            )
        }
        assert resolutions == {
            "message-routing-company-a": first.lead_id,
            "message-routing-company-b": second.lead_id,
            "message-routing-company-b-follow-up": second.lead_id,
        }

    # 两条 Lead 必须对应两条独立审核记录，补充消息不能新建第三条记录。
    records = adapter.get_records()
    assert len(records) == 2
    assert {record.fields["线索名称"] for record in records} == {"公司A", "公司B"}


def test_new_lead_context_is_pinned_before_smart_table_failure_and_follow_up_uses_it(
    session_factory: sessionmaker[Session],
) -> None:
    """验证新线索表格失败后仍固定业务归属，后续补充不会回到旧上下文。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：B 的表格失败后 context 未推进、C 串回 A 或线索数量错误时由 pytest 报告。
    副作用：模拟 A 成功、B 表格结构读取永久失败、C 无公司名补充的连续消息。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-context-pin-a",
        sales_user_id="sales-1",
        text="公司A的张经理，需要协作机器人方案",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                json.dumps(
                    {
                        "intent": "NEW_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"线索名称": "公司A", "联系人": "张经理"},
                        "enrichment": {},
                        "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                json.dumps(
                    {
                        "intent": "UPDATE_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"线索名称": "公司B", "联系人": "董经理"},
                        "enrichment": {},
                        "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                json.dumps(
                    {
                        "intent": "UPDATE_LEAD",
                        "customer_reference": {},
                        "crm_fields": {"手机": "13800000000"},
                        "enrichment": {"预算": "预算38万", "线索来源": "电缆行业展会"},
                        "confidence_by_field": {"手机": 0.99},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
            ]
        )
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=gateway,
    )

    first = service.consume(first_event_id)
    assert first.status is LeadProcessingStatus.CREATED

    second_event_id = persist_outbox_text(
        session_factory,
        message_id="message-context-pin-b",
        sales_user_id="sales-1",
        text="公司B的董经理，电缆表面绝缘层瑕疵视觉检测",
    )

    def fail_update(*_args: object, **_kwargs: object) -> object:
        """模拟创建后的远端写后核验失败，验证字段补写任务仍可恢复。"""
        record_id = str(_args[0]) if _args else None
        raise SmartTableWriteVerificationError(
            ("线索名称",), remote_record_id=record_id
        )

    with patch.object(adapter, "update_record", side_effect=fail_update):
        second = service.consume(second_event_id)

    assert second.status is LeadProcessingStatus.SYNC_FAILED
    assert second.lead_id is not None
    assert second.lead_id != first.lead_id
    with session_factory() as session:
        context_after_failure = session.get(SalesLeadContext, "sales-1")
        second_event = session.get(OutboxEvent, second_event_id)
        second_sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == second.lead_id)
        )
        second_resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-context-pin-b"
            )
        )
    assert context_after_failure is not None
    assert context_after_failure.lead_id == second.lead_id
    assert second_event is not None and second_event.status == "retrying"
    assert second_sync is not None
    assert second_sync.status == "retrying"
    assert second_sync.error_summary == "field_patch_pending"
    second_record_id = second_sync.smart_table_record_id
    assert second_record_id is not None
    assert second_resolution is not None and second_resolution.lead_id == second.lead_id
    assert second_resolution.status == "processing"

    third_event_id = persist_outbox_text(
        session_factory,
        message_id="message-context-pin-c",
        sales_user_id="sales-1",
        text="预算38万，项目预计今年底",
    )
    third = service.consume(third_event_id)

    assert third.status is LeadProcessingStatus.UPDATED
    assert third.lead_id == second.lead_id
    assert third.smart_table_record_id == second_record_id
    with session_factory() as session:
        final_context = session.get(SalesLeadContext, "sales-1")
        assert session.query(Lead).count() == 2
    assert final_context is not None
    assert final_context.lead_id == second.lead_id
    assert final_context.last_message_sequence == 3


def test_free_form_company_contact_phone_message_is_not_unassigned(
    session_factory: sessionmaker[Session],
) -> None:
    """验证真实销售常用的公司分隔联系人格式不会因 AI 未填 crm_fields 而待归属。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：消息仍进入 unassigned 或未写入智能表格时由 pytest 报告。
    副作用：模拟一次 Qwen 成功但只返回客户引用的提取，并验证完整归属链路。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-free-form-identity",
        sales_user_id="sales-1",
        text=(
            "锋元机器人～沈秋冰\n"
            "18959247813，做焊接非标项目，\n"
            "主要是汽车铝型材的焊接，当前用的都是发那科的工业，也会有客户\n"
            "涉及协作焊接机器人需求"
        ),
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {
                        "company": "锋元机器人",
                        "contact": "沈秋冰",
                        "phone": "18959247813",
                    },
                    "crm_fields": {
                        "线索名称": "锋元机器人～沈秋冰",
                        "联系人": "沈秋冰",
                    },
                    "enrichment": {
                        "客户需求/痛点": "客户希望提高焊接自动化效率",
                    },
                    "confidence_by_field": {"线索名称": 0.90, "联系人": 0.90},
                    "conflicts": [],
                    "warnings": [],
                }
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.smart_table_record_id is not None
    record = adapter.get_record(result.smart_table_record_id)
    assert record is not None
    assert record.fields["线索名称"] == "锋元机器人"
    assert record.fields["联系人"] == "沈秋冰"
    assert record.fields["手机"] == "18959247813"
    with session_factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-free-form-identity"
            )
        )
    assert resolution is not None
    assert resolution.status == "assigned"


def test_ai_semantic_segments_create_independent_resolutions(
    session_factory: sessionmaker[Session],
) -> None:
    """验证无标签多客户消息按 AI 原文分段分别进入服务端归属。"""
    source_text = (
        "今天见了苏州安科的李经理，需要视觉检测，预算30万；"
        "另外无锡宏达王总想做机器人上下料，预算45万。"
    )
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-semantic-two-leads",
        sales_user_id="sales-1",
        text=source_text,
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "MULTI_LEAD",
                    "customer_reference": {},
                    "crm_fields": {},
                    "enrichment": {},
                    "confidence_by_field": {},
                    "segments": [
                        {
                            "segment_index": 0,
                            "source_text_span": "苏州安科的李经理，需要视觉检测，预算30万",
                            "customer_reference": {"company": "苏州安科", "contact": "李经理"},
                            "crm_fields": {"线索名称": "苏州安科", "联系人": "李经理"},
                            "enrichment": {"预算": "预算30万"},
                            "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                        },
                        {
                            "segment_index": 1,
                            "source_text_span": "无锡宏达王总想做机器人上下料，预算45万",
                            "customer_reference": {"company": "无锡宏达", "contact": "王总"},
                            "crm_fields": {"线索名称": "无锡宏达", "联系人": "王总"},
                            "enrichment": {"预算": "预算45万"},
                            "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                        },
                    ],
                    "conflicts": [],
                    "warnings": [],
                }
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider),
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert len(result.lead_ids) == 2
    with session_factory() as session:
        resolutions = session.scalars(
            select(LeadMessageResolution)
            .where(LeadMessageResolution.message_id == "message-ai-semantic-two-leads")
            .order_by(LeadMessageResolution.segment_index)
        ).all()
        leads = session.scalars(
            select(Lead).where(Lead.source_message_id == "message-ai-semantic-two-leads")
        ).all()
    assert [resolution.segment_index for resolution in resolutions] == [0, 1]
    assert [resolution.lead_id for resolution in resolutions] == list(result.lead_ids)
    assert {lead.field_values["线索名称"] for lead in leads} == {"苏州安科", "无锡宏达"}


def test_ai_semantic_segments_without_punctuation_still_route_independently(
    session_factory: sessionmaker[Session],
) -> None:
    """验证无标签、无分号的连续自然语言仍可由可靠原文 span 拆成两条线索。"""
    source_text = "苏州安科李经理要视觉检测预算30万，另外无锡宏达王总准备做机器人上下料"
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-semantic-no-punctuation",
        sales_user_id="sales-1",
        text=source_text,
    )
    provider = MockLLMProvider(
        [
            semantic_segments_json(
                [
                    {
                        "segment_index": 0,
                        "source_text_span": "苏州安科李经理要视觉检测预算30万",
                        "customer_reference": {"company": "苏州安科", "contact": "李经理"},
                        "crm_fields": {"线索名称": "苏州安科", "联系人": "李经理"},
                        "enrichment": {},
                        "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                    },
                    {
                        "segment_index": 1,
                        "source_text_span": "无锡宏达王总准备做机器人上下料",
                        "customer_reference": {"company": "无锡宏达", "contact": "王总"},
                        "crm_fields": {"线索名称": "无锡宏达", "联系人": "王总"},
                        "enrichment": {},
                        "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
                    },
                ]
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert len(result.lead_ids) == 2
    with session_factory() as session:
        resolutions = session.scalars(
            select(LeadMessageResolution)
            .where(LeadMessageResolution.message_id == "message-ai-semantic-no-punctuation")
            .order_by(LeadMessageResolution.segment_index)
        ).all()
    assert [resolution.lead_id for resolution in resolutions] == list(result.lead_ids)


def test_ai_semantic_three_segments_keep_unique_segment_indexes(
    session_factory: sessionmaker[Session],
) -> None:
    """验证同一消息三个可靠客户候选不会共享 segment_index 或归属。"""
    source_text = "甲公司李工要视觉检测；乙2号王总要上下料；丙视觉科技刘博士要装配"
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-semantic-three-leads",
        sales_user_id="sales-1",
        text=source_text,
    )
    segments = [
        {
            "segment_index": 0,
            "source_text_span": "甲公司李工要视觉检测",
            "customer_reference": {"company": "甲公司", "contact": "李工"},
            "crm_fields": {"线索名称": "甲公司", "联系人": "李工"},
            "enrichment": {},
            "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
        },
        {
            "segment_index": 1,
            "source_text_span": "乙2号王总要上下料",
            "customer_reference": {"company": "乙2号", "contact": "王总"},
            "crm_fields": {"线索名称": "乙2号", "联系人": "王总"},
            "enrichment": {},
            "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
        },
        {
            "segment_index": 2,
            "source_text_span": "丙视觉科技刘博士要装配",
            "customer_reference": {"company": "丙视觉科技", "contact": "刘博士"},
            "crm_fields": {"线索名称": "丙视觉科技", "联系人": "刘博士"},
            "enrichment": {},
            "confidence_by_field": {"线索名称": 0.99, "联系人": 0.95},
        },
    ]
    provider = MockLLMProvider([semantic_segments_json(segments)])
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert len(result.lead_ids) == 3
    with session_factory() as session:
        resolutions = session.scalars(
            select(LeadMessageResolution)
            .where(LeadMessageResolution.message_id == "message-ai-semantic-three-leads")
            .order_by(LeadMessageResolution.segment_index)
        ).all()
    assert [resolution.segment_index for resolution in resolutions] == [0, 1, 2]
    assert len({resolution.lead_id for resolution in resolutions}) == 3


def test_shared_ambiguous_budget_is_not_copied_to_each_segment() -> None:
    """验证无法归属的共享预算不会被模型结果复制到多个客户。"""
    source_text = "安科和宏达都想做检测，预算差不多30万"
    provider = MockLLMProvider(
        [
            semantic_segments_json(
                [
                    {
                        "segment_index": 0,
                        "source_text_span": "安科",
                        "customer_reference": {"company": "安科"},
                        "crm_fields": {"线索名称": "安科"},
                        "enrichment": {"预算": "预算差不多30万"},
                        "confidence_by_field": {"线索名称": 0.99},
                    },
                    {
                        "segment_index": 1,
                        "source_text_span": "宏达",
                        "customer_reference": {"company": "宏达"},
                        "crm_fields": {"线索名称": "宏达"},
                        "enrichment": {"预算": "预算差不多30万"},
                        "confidence_by_field": {"线索名称": 0.99},
                    },
                ]
            )
        ]
    )

    result = AIGateway(provider).extract_fields(source_text)

    assert len(result.segments) == 2
    assert all(segment.patch.enrichment == {} for segment in result.segments)


def test_ambiguous_multi_lead_without_reliable_spans_is_unassigned(
    session_factory: sessionmaker[Session],
) -> None:
    """验证模型无法给出可靠边界时，服务器不猜测任何 Lead 目标。"""
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-ambiguous-multi",
        sales_user_id="sales-1",
        text="安科和宏达都想做检测，预算差不多30万",
    )
    provider = MockLLMProvider(
        [
            semantic_single_json(
                intent="MULTI_LEAD_AMBIGUOUS",
                crm_fields={},
            )
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    with session_factory() as session:
        assert session.query(Lead).count() == 0


def test_ai_hallucinated_company_cannot_replace_active_context(
    session_factory: sessionmaker[Session],
) -> None:
    """验证不在原文中的 AI 公司名不能劫持有效当前客户上下文。"""
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-grounding-first",
        sales_user_id="sales-1",
        text="客户：苏州安科",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    first = service.consume(first_event_id)
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="message-ai-grounding-update",
        sales_user_id="sales-1",
        text="他们现在想做视觉检测",
    )
    provider = MockLLMProvider(
        [
            semantic_single_json(
                intent="UPDATE_LEAD",
                crm_fields={"线索名称": "模型猜测公司", "工艺": "视觉检测"},
            )
        ]
    )

    updated = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    ).consume(second_event_id)

    assert first.lead_id is not None
    assert updated.lead_id == first.lead_id
    with session_factory() as session:
        assert session.query(Lead).count() == 1
        lead = session.get(Lead, first.lead_id)
    assert lead is not None
    assert lead.field_values["线索名称"] == "苏州安科"


def test_eight_natural_language_messages_follow_aaaaabbb_context_rule(
    session_factory: sessionmaker[Session],
) -> None:
    """验证五条连续补充后切换第二客户，后续消息仍全部留在第二客户。"""
    messages = [
        ("message-aaaaa-1", "刚见了苏州安科的李经理"),
        ("message-aaaaa-2", "他们想做视觉检测"),
        ("message-aaaaa-3", "预算大概35万"),
        ("message-aaaaa-4", "手机号13800000001"),
        ("message-aaaaa-5", "预计明年Q1启动"),
        ("message-bbb-1", "另外无锡宏达王总要做上下料"),
        ("message-bbb-2", "预算50万"),
        ("message-bbb-3", "手机号13900000002"),
    ]
    first_event_id = 0
    for message_id, text in messages:
        event_id = persist_outbox_text(
            session_factory,
            message_id=message_id,
            sales_user_id="sales-1",
            text=text,
        )
        if first_event_id == 0:
            first_event_id = event_id
    provider = MockLLMProvider(
        [
            semantic_single_json(
                intent="NEW_LEAD",
                crm_fields={"线索名称": "苏州安科", "联系人": "李经理"},
            ),
            semantic_single_json(intent="UPDATE_LEAD", crm_fields={"工艺": ["视觉检测"]}),
            semantic_single_json(
                intent="UPDATE_LEAD", crm_fields={}, enrichment={"预算": "预算大概35万"}
            ),
            semantic_single_json(
                intent="UPDATE_LEAD", crm_fields={"手机": "13800000001"}
            ),
            semantic_single_json(
                intent="UPDATE_LEAD", crm_fields={}, enrichment={"特殊要求": "预计明年Q1启动"}
            ),
            semantic_single_json(
                intent="NEW_LEAD",
                crm_fields={"线索名称": "无锡宏达", "联系人": "王总", "工艺": ["装配"]},
            ),
            semantic_single_json(
                intent="UPDATE_LEAD", crm_fields={}, enrichment={"预算": "预算50万"}
            ),
            semantic_single_json(
                intent="UPDATE_LEAD", crm_fields={"手机": "13900000002"}
            ),
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    ).consume(first_event_id)

    with session_factory() as session:
        resolutions = session.scalars(
            select(LeadMessageResolution)
            .where(
                LeadMessageResolution.message_id.in_([message_id for message_id, _ in messages])
            )
            .order_by(LeadMessageResolution.message_id)
        ).all()
        leads = session.scalars(select(Lead)).all()
        context = session.scalar(
            select(SalesLeadContext).where(SalesLeadContext.sales_user_id == "sales-1")
        )
    lead_by_company = {
        lead.field_values.get("线索名称"): lead.id for lead in leads
    }
    resolution_by_message = {
        resolution.message_id: resolution.lead_id for resolution in resolutions
    }
    assert len(leads) == 2
    assert [resolution_by_message[message_id] for message_id, _ in messages] == [
        lead_by_company["苏州安科"],
        lead_by_company["苏州安科"],
        lead_by_company["苏州安科"],
        lead_by_company["苏州安科"],
        lead_by_company["苏州安科"],
        lead_by_company["无锡宏达"],
        lead_by_company["无锡宏达"],
        lead_by_company["无锡宏达"],
    ]
    assert context is not None
    assert context.lead_id == lead_by_company["无锡宏达"]
    assert context.last_message_sequence == 8


def test_different_company_with_shared_phone_gets_tyc_name_on_new_record(
    session_factory: sessionmaker[Session],
) -> None:
    """验证不同公司复用手机号时新建线索并同步天眼查首候选名称。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：消息串入旧线索、天眼查候选未同步或待确认标记缺失时由 pytest 报告断言失败。
    副作用：写入两条测试线索及一条带候选审核元数据的智能表格记录。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-company-phone-first",
        sales_user_id="sales-1",
        text="客户：汇川技术；手机：17318902311",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(
        session_factory,
        adapter,
        MockTYCAdapter(
            {
                "汇川技术": QCCLookupResult.matched(
                    QCCCandidate("深圳市汇川技术股份有限公司", "tyc-hc")
                ),
                "艾利特": QCCLookupResult.ambiguous(
                    (
                        QCCCandidate("艾利特智能机器人股份有限公司", "tyc-alt-1"),
                        QCCCandidate("艾利特机器人科技有限公司", "tyc-alt-2"),
                    )
                ),
            }
        ),
    )
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        company_lead_service=company_service,
    )

    first = service.consume(first_event_id)
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="message-company-phone-second",
        sales_user_id="sales-1",
        text="客户：艾利特；手机：17318902311",
    )
    second = service.consume(second_event_id)

    assert first.status is LeadProcessingStatus.CREATED
    assert second.status is LeadProcessingStatus.CREATED
    assert second.lead_id != first.lead_id
    assert second.smart_table_record_id is not None
    record = adapter.get_record(second.smart_table_record_id)
    assert record is not None
    assert record.fields["线索名称"] == "艾利特智能机器人股份有限公司"
    assert record.fields["AI待确认"] == ["线索名称"]

    # 模拟历史链路先同步原始名称、再由天眼查升级后台名称的来源基线。
    with session_factory.begin() as session:
        provenance = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == second.lead_id,
                LeadFieldProvenance.field_name == "线索名称",
            )
        )
        assert provenance is not None
        provenance.last_ai_synced_value = "艾利特"
    with session_factory() as session:
        retry_message = session.get(IncomingMessage, "message-company-phone-second")
        existing = session.get(Lead, second.lead_id)
        assert retry_message is not None
        assert existing is not None
        assert (
            service._get_strong_identity_lead(
                session,
                retry_message,
                {"线索名称": "艾利特", "手机": "17318902311"},
            )
            == existing
        )

    retry_event_id = persist_outbox_text(
        session_factory,
        message_id="message-company-phone-retry",
        sales_user_id="sales-1",
        text="客户：艾利特；手机：17318902311",
    )
    retry = service.consume(retry_event_id)

    assert retry.status is LeadProcessingStatus.UPDATED
    assert retry.lead_id == second.lead_id
    retried_record = adapter.get_record(second.smart_table_record_id)
    assert retried_record is not None
    assert retried_record.fields["线索名称"] == "艾利特智能机器人股份有限公司"


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
    replay = service.consume(first_event_id)

    assert result.status is LeadProcessingStatus.SYNC_FAILED
    assert replay.status is LeadProcessingStatus.ALREADY_PROCESSED
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
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.source_message_id == "message-ai-failure",
                NotificationRecord.notification_type == "lead_processing_failed",
            )
        ).all()
    assert first_event is not None
    assert first_event.status == "failed_pending_review"
    assert failed_audit is not None
    assert failed_lead is None
    assert len(notices) == 1
    assert notices[0].content == (
        "这条线索消息未能完成解析，已进入待人工处理，请稍后重试或补充信息。"
    )
    assert "network" not in (notices[0].content or "")
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


def test_card_and_follow_up_fragment_stay_with_current_context_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证名片 OCR 和后续数量补充都归属于销售当前客户，而不是各自新建线索。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：任一消息被拆成独立线索或表格记录时由 pytest 报告。
    副作用：按真实销售发送顺序消费一条需求、一张名片和一条数量补充消息。
    """
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="message-context-card-first",
        sales_user_id="sales-1",
        text="邢总公司想要买协作机器人，用于喷涂，预算100万",
    )
    persist_outbox_text(
        session_factory,
        message_id="message-context-card",
        sales_user_id="sales-1",
        text=(
            "邢伟伟\n数字化业务中心总监 / 合伙人\nMobile: 158 6169 9724\n"
            "长广溪智能制造（无锡）有限公司"
        ),
    )
    persist_outbox_text(
        session_factory,
        message_id="message-context-card-follow-up",
        sales_user_id="sales-1",
        text="想采购10台左右",
    )
    with session_factory.begin() as session:
        # 用已完成 OCR 的附件模拟真实名片消息，确保测试覆盖媒体消息归属路径。
        card_message = session.get(IncomingMessage, "message-context-card")
        assert card_message is not None
        card_message.requires_media_enrichment = True
        session.add(
            MessageAttachment(
                id="attachment-context-card",
                message_id=card_message.message_id,
                media_kind="image",
                scan_status="clean",
                processing_status="succeeded",
                recognized_text=card_message.normalized_text,
            )
        )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {"company": "邢总公司", "contact": "邢总"},
                    "crm_fields": {
                        "线索名称": "邢总公司",
                        "联系人": "邢总",
                        "业务线": "协作机器人",
                    },
                    "enrichment": {
                        "客户需求/痛点": "想要买协作机器人，用于喷涂",
                        "预算": "预算100万",
                    },
                    "confidence_by_field": {
                        "线索名称": 0.95,
                        "联系人": 0.90,
                        "业务线": 0.95,
                    },
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {
                        "company": "长广溪智能制造（无锡）有限公司",
                        "contact": "邢伟伟",
                        "title": "数字化业务中心总监 / 合伙人",
                        "phone": "15861699724",
                    },
                    "crm_fields": {
                        "线索名称": "长广溪智能制造（无锡）有限公司",
                        "联系人": "邢伟伟",
                        "职务": "数字化业务中心总监 / 合伙人",
                        "手机": "15861699724",
                    },
                    "enrichment": {},
                    "confidence_by_field": {
                        "线索名称": 0.99,
                        "联系人": 0.99,
                        "职务": 0.99,
                        "手机": 0.99,
                    },
                    "conflicts": [],
                    "warnings": [],
                }
            ),
            json.dumps(
                {
                    "intent": "IGNORE",
                    "customer_reference": {},
                    "crm_fields": {},
                    "enrichment": {},
                    "confidence_by_field": {},
                    "conflicts": [],
                    "warnings": [],
                }
            ),
        ]
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    )

    first_result = service.consume(first_event_id)

    assert first_result.status is LeadProcessingStatus.CREATED
    assert len(adapter.get_records()) == 1
    with session_factory() as session:
        resolutions = session.scalars(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id.in_(
                    [
                        "message-context-card-first",
                        "message-context-card",
                        "message-context-card-follow-up",
                    ]
                )
            )
    ).all()
    assert {resolution.lead_id for resolution in resolutions} == {first_result.lead_id}
    record = adapter.get_record(first_result.smart_table_record_id or "")
    assert record is not None
    assert record.fields["线索名称"] == "长广溪智能制造（无锡）有限公司"
    assert "预算100万" in record.fields["备注"]
    assert "想采购10台左右" in record.fields["备注"]


def test_controlled_temporary_confirmation_keeps_lifecycle_and_creates_first_record(
    session_factory: sessionmaker[Session],
) -> None:
    """验证受控确认先写人工来源，再在原 temporary Lead 上首次创建表格记录。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：生命周期替换、未核验审计或首次入表行为错误时由 pytest 报告。
    副作用：消费一条无公司名消息后通过受控入口确认公司名称。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id="controlled-confirm-temporary",
        sales_user_id="sales-1",
        text="联系人：张三；手机：13800000001",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(session_factory, adapter, MockQCCAdapter())
    workspace = FirstTextLeadWorkspaceService(
        session_factory, adapter, company_lead_service=company_service
    )
    temporary = workspace.consume(event_id)

    confirmed = workspace.confirm_temporary_company(temporary.lead_id or "", "sales-1", "上海智造")

    assert confirmed.lead_id == temporary.lead_id
    assert confirmed.smart_table_record_id is not None
    assert len(adapter.get_records()) == 1
    with session_factory() as session:
        lead = session.get(Lead, confirmed.lead_id)
        provenance = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == confirmed.lead_id,
                LeadFieldProvenance.field_name == "线索名称",
                LeadFieldProvenance.is_user_confirmed.is_(True),
            )
        )
    assert lead is not None
    assert lead.company_confirmed_by_user is True
    assert lead.company_verification_status == "user_confirmed_unverified"
    assert provenance is not None


def test_controlled_confirmation_matching_existing_record_creates_independent_record(
    session_factory: sessionmaker[Session],
) -> None:
    """验证确认命中同销售既有公司时复用 record 并保留销售人工编辑。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：产生第二个 record 或覆盖人工字段时由 pytest 报告。
    副作用：创建正式与 temporary Lead，模拟销售编辑后执行受控确认。
    """
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    company_service = CompanyLeadService(
        session_factory,
        adapter,
        MockQCCAdapter(
            {"已有公司": QCCLookupResult.matched(QCCCandidate("已有公司有限公司", "qcc"))}
        ),
    )
    workspace = FirstTextLeadWorkspaceService(
        session_factory, adapter, company_lead_service=company_service
    )
    temporary_event = persist_outbox_text(
        session_factory,
        message_id="controlled-existing-temporary",
        sales_user_id="sales-1",
        text="联系人：李四；电话：0510-12345678；手机：13800000001",
    )
    temporary_seed = company_service.upsert(
        CompanyUpsertCommand(
            source_message_id="controlled-existing-temporary",
            sales_user_id="sales-1",
            fields={"联系人": "李四", "电话": "0510-12345678", "手机": "13800000001"},
        )
    )
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, temporary_event)
        assert event is not None
        # 此来源只供受控确认审计；避免顺序续消费把无公司名碎片塞入已有上下文。
        event.status = "succeeded"
    existing_event = persist_outbox_text(
        session_factory,
        message_id="controlled-existing",
        sales_user_id="sales-1",
        text="公司：已有公司；联系人：张三",
    )
    existing = workspace.consume(existing_event)
    assert existing.smart_table_record_id is not None
    assert temporary_seed.smart_table_record_id is None
    adapter.update_record(existing.smart_table_record_id, {"联系人": "销售手工联系人"})

    confirmed = workspace.confirm_temporary_company(
        temporary_seed.lead_id, "sales-1", "已有公司"
    )

    assert confirmed.lead_id == temporary_seed.lead_id
    assert confirmed.smart_table_record_id != existing.smart_table_record_id
    assert len(adapter.get_records()) == 2
    record = adapter.get_record(existing.smart_table_record_id)
    assert record is not None
    assert record.fields["联系人"] == "销售手工联系人"
    confirmed_record = adapter.get_record(confirmed.smart_table_record_id or "")
    assert confirmed_record is not None
    assert confirmed_record.fields["手机"] == "13800000001"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("西门子的陈总想了解焊接产品", {"线索名称": "西门子", "联系人": "陈总"}),
        ("无锡宝通的 CIO 郭总对双臂机器人感兴趣", {"线索名称": "无锡宝通", "联系人": "郭总"}),
        ("无锡宝通的 cio 郭总对双臂机器人感兴趣", {"线索名称": "无锡宝通", "联系人": "郭总"}),
        ("隆盛科技的采购负责人张总有上下料需求", {"线索名称": "隆盛科技", "联系人": "张总"}),
    ],
)
def test_natural_company_contact_identity_is_deterministic(
    text: str, expected: dict[str, str]
) -> None:
    """识别公司、受控职位和联系人称谓，不依赖 AI 或 active context。"""
    extractor = DeterministicFirstTextLeadExtractor()

    assert extractor.extract_leading_company_contact_hint(text) == expected
    assert extractor.extract_leading_company_hint(text) == expected["线索名称"]


@pytest.mark.parametrize(
    ("field_name", "source_text", "model_value", "expected_value"),
    [
        ("手机", "补充联系电话：138 0000 0001", "13900000009", "13800000001"),
        ("电话", "固定电话：010-12345678", "010-87654321", None),
        ("邮箱", "邮箱：Alice@example.com", "bob@example.com", "Alice@example.com"),
    ],
)
def test_ai_contact_candidates_require_matching_source_evidence(
    field_name: str,
    source_text: str,
    model_value: str,
    expected_value: str | None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """合法格式的 AI 联系方式若不匹配当前原文，不得进入正式字段或诊断日志。"""
    gateway = AIGateway(
        MockLLMProvider(
            [semantic_single_json(intent="UPDATE_LEAD", crm_fields={field_name: model_value})]
        )
    )

    patch = gateway.extract_fields(source_text, source_message_id="contact-source-test")

    assert patch.fields.get(field_name) == expected_value
    assert patch.analysis.crm_fields.get(field_name) == expected_value
    assert model_value not in caplog.text
    assert "ai_contact_source_mismatch" in caplog.text
    mismatch_records = [
        record for record in caplog.records
        if record.getMessage() == "ai_contact_source_mismatch"
    ]
    assert mismatch_records
    assert all(record.field_name == field_name for record in mismatch_records)
    assert all(record.reason == "missing_source_evidence" for record in mismatch_records)


def test_phone_evidence_accepts_safe_formatting_and_explicit_china_prefix() -> None:
    """空格、连接符与明确的 +86 区号可归一化比较同一号码。"""
    assert AIGateway._contact_value_has_source_evidence(
        "手机", "13800000001", "联系电话：+86 (138) 0000-0001"
    )


def test_contact_evidence_is_limited_to_each_ai_segment() -> None:
    """模型不得把其它客户分段的号码带入当前 segment。"""
    source_a = "甲公司 联系电话 13800000001"
    source_b = "乙公司 联系电话 13900000002"
    response = json.dumps(
        {
            "intent": "MULTI_LEAD",
            "customer_reference": {},
            "crm_fields": {},
            "enrichment": {},
            "confidence_by_field": {},
            "segments": [
                {
                    "segment_index": 0,
                    "source_text_span": source_a,
                    "customer_reference": {},
                    "crm_fields": {"手机": "13900000002"},
                    "enrichment": {},
                    "confidence_by_field": {"手机": 0.99},
                },
                {
                    "segment_index": 1,
                    "source_text_span": source_b,
                    "customer_reference": {},
                    "crm_fields": {"手机": "13800000001"},
                    "enrichment": {},
                    "confidence_by_field": {"手机": 0.99},
                },
            ],
            "conflicts": [],
            "warnings": [],
        },
        ensure_ascii=False,
    )
    gateway = AIGateway(MockLLMProvider([response]))

    patch = gateway.extract_fields(f"{source_a}；{source_b}")

    assert len(patch.segments) == 2
    assert patch.segments[0].patch.fields["手机"] == "13800000001"
    assert patch.segments[1].patch.fields["手机"] == "13900000002"


def test_ai_update_intent_cannot_override_deterministic_company_boundary(
    session_factory: sessionmaker[Session],
) -> None:
    """模型返回旧公司名和 UPDATE_LEAD 时，服务器仍为确定性新公司建立独立目标。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="hotfix-boundary-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None
    message_id = "hotfix-boundary-new-company"
    event_id = persist_outbox_text(
        session_factory,
        message_id=message_id,
        sales_user_id="sales-1",
        text="无锡宝通的 CIO 郭总对双臂机器人感兴趣",
    )
    analysis = LeadAnalysis(
        intent="UPDATE_LEAD",
        crm_fields={"线索名称": "西门子", "联系人": "郭总"},
        confidence_by_field={"线索名称": 0.99, "联系人": 0.99},
    )
    stale_patch = ExtractedLeadPatch(
        trace_id="stub-conflicting-update",
        analysis=analysis,
        fields={"线索名称": "西门子", "联系人": "郭总"},
        pending_confirmation_fields=(),
        low_confidence_candidates={},
    )

    class StubGateway:
        """返回固定冲突建议以隔离验证确定性身份边界。"""

        def extract_fields(self, _text: str, **_kwargs: object) -> ExtractedLeadPatch:
            """返回测试预置的旧公司 UPDATE_LEAD 候选。"""
            return stale_patch

    service = FirstTextLeadWorkspaceService(session_factory, adapter, ai_gateway=StubGateway())
    with session_factory.begin() as session:
        event = session.get(OutboxEvent, event_id)
        message = session.get(IncomingMessage, message_id)
        active_lead = session.get(Lead, seed.lead_id)
        assert event is not None and message is not None and active_lead is not None

        # 私有评审方法会绑定当前线索日志上下文；直接单测时显式恢复，避免污染后续用例。
        context_token = bind_log_context()
        try:
            request, result = service._prepare_ai_review(
                session,
                event,
                message,
                active_lead,
                company_name_hint="无锡宝通",
            )
        finally:
            reset_log_context(context_token)

        assert result is None
        assert request is not None and not isinstance(request, tuple)
        assert request.lead_id != seed.lead_id
        assert request.patch.fields["线索名称"] == "无锡宝通"


def test_unresolved_company_identity_fails_closed_instead_of_matching_phone_context(
    session_factory: sessionmaker[Session],
) -> None:
    """公司身份证据无法提取时，即使号码命中旧客户也必须待归属。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="hotfix-unresolved-seed",
        sales_user_id="sales-1",
        text="客户：西门子；手机：13800000001",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None
    unresolved_event = persist_outbox_text(
        session_factory,
        message_id="hotfix-unresolved-company",
        sales_user_id="sales-1",
        text="某未确认制造有限公司的联系人补充联系电话 13800000001",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [semantic_single_json(intent="UPDATE_LEAD", crm_fields={"手机": "13800000001"})]
        )
    )

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    ).consume(unresolved_event)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    assert result.lead_id is None
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Lead)) == 1
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "hotfix-unresolved-company"
            )
        )
        assert resolution is not None
        assert resolution.status == "unassigned"
        assert resolution.lead_id is None


def test_incident_four_message_sequence_keeps_three_leads_and_contact_provenance(
    session_factory: sessionmaker[Session],
) -> None:
    """隔离复现事故消息，验证公司边界、联系方式来源、表格 ID 和 850005 检查点。"""
    phone_a = "13800000001"
    model_phone_b = "13900000009"
    phone_b = "13700000002"
    phone_c = "13600000003"
    adapter = _ApplyThenRateLimitAdapter()
    gateway = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="NEW_LEAD",
                    crm_fields={},
                    enrichment={"客户需求/痛点": "了解焊接产品"},
                ),
                semantic_single_json(
                    intent="UPDATE_LEAD", crm_fields={"手机": model_phone_b}
                ),
                semantic_single_json(
                    intent="NEW_LEAD",
                    crm_fields={},
                    enrichment={"客户需求/痛点": "双臂机器人感兴趣"},
                ),
                semantic_single_json(
                    intent="NEW_LEAD",
                    crm_fields={},
                    enrichment={"客户需求/痛点": "上下料需求"},
                ),
            ]
        )
    )
    service = FirstTextLeadWorkspaceService(session_factory, adapter, ai_gateway=gateway)

    event_a = persist_outbox_text(
        session_factory,
        message_id="incident-seq-5",
        sales_user_id="sales-1",
        text="西门子的陈总想了解焊接产品，希望展会联系。",
        sequence=5,
    )
    lead_a_result = service.consume(event_a)
    assert lead_a_result.status is LeadProcessingStatus.CREATED

    # 只在 seq 6 更新时注入远端已应用后返回的 850005，验证同补丁恢复后上下文不串线。
    adapter.fail_update_count = 1
    event_a_phone = persist_outbox_text(
        session_factory,
        message_id="incident-seq-6",
        sales_user_id="sales-1",
        text=f"补充联系电话：{phone_a}",
        sequence=6,
    )
    phone_update = service.consume(event_a_phone)
    assert phone_update.status is LeadProcessingStatus.UPDATED

    event_b = persist_outbox_text(
        session_factory,
        message_id="incident-seq-8",
        sales_user_id="sales-1",
        text=f"无锡宝通的 CIO 郭总对双臂机器人感兴趣，手机：{phone_b}",
        sequence=8,
    )
    lead_b_result = service.consume(event_b)
    assert lead_b_result.status is LeadProcessingStatus.CREATED

    event_c = persist_outbox_text(
        session_factory,
        message_id="incident-seq-9",
        sales_user_id="sales-1",
        text=f"隆盛科技的采购负责人张总有上下料需求，手机：{phone_c}",
        sequence=9,
    )
    lead_c_result = service.consume(event_c)
    assert lead_c_result.status is LeadProcessingStatus.CREATED

    with session_factory() as session:
        leads = session.scalars(select(Lead)).all()
        assert len(leads) == 3
        lead_by_source = {lead.source_message_id: lead for lead in leads}
        lead_a = lead_by_source["incident-seq-5"]
        lead_b = lead_by_source["incident-seq-8"]
        lead_c = lead_by_source["incident-seq-9"]
        assert lead_a_result.lead_id == lead_a.id
        assert lead_b_result.lead_id == lead_b.id
        assert lead_c_result.lead_id == lead_c.id
        assert lead_a.field_values["线索名称"] == "西门子"
        assert lead_b.field_values["线索名称"] == "无锡宝通"
        assert lead_c.field_values["线索名称"] == "隆盛科技"
        assert lead_a.field_values["联系人"] == "陈总"
        assert lead_b.field_values["联系人"] == "郭总"
        assert lead_c.field_values["联系人"] == "张总"
        assert lead_a.field_values["手机"] == phone_a
        assert lead_b.field_values["手机"] == phone_b
        assert lead_c.field_values["手机"] == phone_c
        assert lead_a.enrichment_values["客户需求/痛点"] == "了解焊接产品"
        assert lead_b.enrichment_values["客户需求/痛点"] == "双臂机器人感兴趣"
        assert lead_c.enrichment_values["客户需求/痛点"] == "上下料需求"
        company_names = {"西门子", "无锡宝通", "隆盛科技"}
        for lead in leads:
            other_company_names = company_names - {lead.field_values["线索名称"]}
            assert all(
                name not in str(lead.enrichment_values) for name in other_company_names
            )
        sequence_by_message = {
            message.message_id: message.sequence
            for message in session.scalars(select(IncomingMessage)).all()
        }
        assert sequence_by_message == {
            "incident-seq-5": 5,
            "incident-seq-6": 6,
            "incident-seq-8": 8,
            "incident-seq-9": 9,
        }
        phone_provenance = session.scalars(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id.in_([lead_a.id, lead_b.id, lead_c.id])
            )
        ).all()
        provenance_by_lead_field = {
            (item.lead_id, item.field_name): item for item in phone_provenance
        }
        assert (
            provenance_by_lead_field[(lead_a.id, "线索名称")].source_message_id
            == "incident-seq-5"
        )
        assert (
            provenance_by_lead_field[(lead_a.id, "联系人")].source_message_id
            == "incident-seq-5"
        )
        assert (
            provenance_by_lead_field[(lead_a.id, "手机")].source_message_id
            == "incident-seq-6"
        )
        assert (
            provenance_by_lead_field[(lead_a.id, "手机")].value
            == serialize_field_value(phone_a)
        )
        for lead, message_id, phone in (
            (lead_b, "incident-seq-8", phone_b),
            (lead_c, "incident-seq-9", phone_c),
        ):
            for field_name in ("线索名称", "联系人", "手机"):
                provenance = provenance_by_lead_field[(lead.id, field_name)]
                assert provenance.source_message_id == message_id
                if field_name == "手机":
                    assert provenance.value == serialize_field_value(phone)
        phone_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "incident-seq-6")
        )
        assert phone_event is not None and phone_event.status == "succeeded"

    # Smart Table 已应用更新后返回模拟 850005；既有同补丁恢复成功，下一条公司仍独立建档。
    assert adapter.injected_850005_count == 1

    records = adapter.get_records()
    assert len(records) == 3
    assert len({record.record_id for record in records}) == 3
    records_by_owner_name = {record.fields["线索名称"]: record for record in records}
    assert records_by_owner_name["西门子"].fields["手机"] == phone_a
    assert records_by_owner_name["无锡宝通"].fields["手机"] == phone_b
    assert records_by_owner_name["隆盛科技"].fields["手机"] == phone_c
    assert all(record.fields.get("手机") != model_phone_b for record in records)
