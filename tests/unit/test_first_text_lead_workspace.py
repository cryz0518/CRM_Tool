"""首条文本线索进入销售个人审核工作区的应用服务测试。"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Generator, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import app.smart_table.enums as enum_module
from app.ai.gateway import AIGateway
from app.ai.models import ExtractedLeadPatch, LeadAnalysis
from app.ai.provider import LLMProviderError, MockLLMProvider
from app.companies.models import CompanyUpsertCommand, QCCCandidate, QCCLookupResult
from app.companies.service import CompanyLeadService, MockQCCAdapter, MockTYCAdapter
from app.core.config import Settings
from app.core.failures import RetryableTaskFailure
from app.leads.models import (
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    SalesLeadContext,
    SmartTableSync,
    serialize_field_value,
)
from app.leads.service import (
    AIReviewRequest,
    DeterministicFirstTextLeadExtractor,
    FirstTextLeadWorkspaceService,
    LeadProcessingResult,
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
from app.notifications.outbound import WecomOutboundNotificationSender
from app.smart_table.adapter import SmartTableActor
from app.smart_table.enums import EnumSnapshotService
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.models import SmartTableOption, SmartTableRecord
from app.smart_table.registry import DEFAULT_LEAD_BUSINESS_VALUES, build_required_smart_table_schema
from app.smart_table.wecom_cli import (
    SmartTableWriteVerificationError,
    WecomCliProcessError,
    WecomCliTransportError,
)


def test_dynamic_industry_and_process_write_then_preserve_user_edit_after_refresh(
    session_factory: sessionmaker[Session],
) -> None:
    """新增行业与多选工艺走完整录入链路，刷新改名后仍永久保护销售人工修改。"""
    schema = build_required_smart_table_schema()
    additions = {"客户行业": "半导体", "工艺": "激光切割"}
    schema = replace(schema, fields=tuple(
        replace(field, options=(*field.options, SmartTableOption("new-" + field.name, additions[
            field.name
        ]))) if field.name in additions else field for field in schema.fields
    ))
    adapter = MockSmartTableAdapter(schema=schema)
    provider = MockLLMProvider([
        json.dumps({
            "intent": "NEW_LEAD", "customer_reference": {"company": "测试制造公司"},
            "crm_fields": {"线索名称": "测试制造公司", "客户行业": "半导体", "工艺": ["激光切割"]},
            "confidence_by_field": {"线索名称": 0.99, "客户行业": 0.99, "工艺": 0.99},
            "enrichment": {}, "conflicts": [], "warnings": [],
        }, ensure_ascii=False),
        json.dumps({
            "intent": "UPDATE_LEAD", "customer_reference": {"company": "测试制造公司"},
            "crm_fields": {"客户行业": "芯片制造"}, "confidence_by_field": {"客户行业": 0.99},
            "enrichment": {}, "conflicts": [], "warnings": [],
        }, ensure_ascii=False),
    ])
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(provider)
    )
    event_id = persist_outbox_text(
        session_factory, message_id="dynamic-first", sales_user_id="sales-dynamic",
        text="测试制造公司，客户行业：半导体，工艺：激光切割",
    )
    first = service.consume(event_id)
    assert first.status is LeadProcessingStatus.CREATED
    record = adapter.get_records()[0]
    assert record.fields["客户行业"] == "半导体" and record.fields["工艺"] == ["激光切割"]
    adapter.update_record(record.record_id, {"客户行业": "医疗"})
    adapter._schema = replace(schema, fields=tuple(
        replace(field, options=tuple(
            replace(option, name="芯片制造") if option.name == "半导体" else option
            for option in field.options
        )) if field.name == "客户行业" else field for field in schema.fields
    ))
    service._enum_snapshots._deadline = 0
    next_id = persist_outbox_text(
        session_factory, message_id="dynamic-next", sales_user_id="sales-dynamic",
        text="测试制造公司补充，客户行业：芯片制造",
    )
    assert service.consume(next_id).status is LeadProcessingStatus.UPDATED
    assert len(adapter.get_records()) == 1
    assert adapter.get_record(record.record_id).fields["客户行业"] == "医疗"
    with session_factory() as session:
        provenance = session.scalar(select(LeadFieldProvenance).where(
            LeadFieldProvenance.lead_id == first.lead_id,
            LeadFieldProvenance.field_name == "客户行业",
        ))
        assert provenance is not None and provenance.is_user_modified


def test_sequential_messages_refresh_enum_snapshot_after_ttl_expiry(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """连续消费间 TTL 到期后刷新选项；保留销售顺序、模型重试及重复消费幂等。"""
    now = [10.0]
    monkeypatch.setattr(enum_module, "monotonic", lambda: now[0])
    original_schema = build_required_smart_table_schema()
    updated_schema = replace(
        original_schema,
        fields=tuple(
            replace(
                field,
                options=(*field.options, SmartTableOption("industry-semiconductor", "半导体")),
            )
            if field.name == "客户行业"
            else field
            for field in original_schema.fields
        ),
    )
    adapter = MockSmartTableAdapter(schema=original_schema)
    snapshots = EnumSnapshotService(adapter.get_schema, ttl_seconds=300)
    adapter._enum_snapshots = snapshots

    def response(company: str, industry: str) -> str:
        """构造包含原文行业候选的单线索响应；不调用外部模型。"""
        return json.dumps(
            {
                "intent": "NEW_LEAD",
                "customer_reference": {"company": company},
                "crm_fields": {"线索名称": company, "客户行业": industry},
                "confidence_by_field": {"线索名称": 0.99, "客户行业": 0.99},
                "enrichment": {},
                "conflicts": [],
                "warnings": [],
            },
            ensure_ascii=False,
        )

    provider = MockLLMProvider(
        [
            LLMProviderError("temporary"),
            response("甲公司", "医疗"),
            response("西安芯汇半导体", "半导体"),
        ]
    )
    original_create = adapter.create_record
    created_count = 0

    def create_and_change_options(
        fields: Mapping[str, object],
        *,
        actor: SmartTableActor,
        member_names: Mapping[str, str] | None = None,
    ) -> SmartTableRecord:
        """在首条消息写入后模拟管理员改表，并令共享 TTL 到期。"""
        nonlocal created_count
        record = original_create(fields, actor=actor, member_names=member_names)
        created_count += 1
        if created_count == 1:
            adapter._schema = updated_schema
            now[0] = 310.0
        return record

    monkeypatch.setattr(adapter, "create_record", create_and_change_options)
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(provider, enum_snapshots=snapshots),
    )
    first_event_id = persist_outbox_text(
        session_factory,
        message_id="enum-refresh-first",
        sales_user_id="enum-refresh-sales",
        text="甲公司，客户行业：医疗",
    )
    second_event_id = persist_outbox_text(
        session_factory,
        message_id="enum-refresh-second",
        sales_user_id="enum-refresh-sales",
        text="西安芯汇半导体，客户行业：半导体",
    )

    first = service.consume(first_event_id)

    assert first.status is LeadProcessingStatus.CREATED
    assert [record.fields["客户行业"] for record in adapter.get_records()] == ["医疗", "半导体"]
    industry_schema = provider.requests[0].json_schema["properties"]["crm_fields"]["properties"][
        "客户行业"
    ]
    refreshed_industry_schema = provider.requests[2].json_schema["properties"]["crm_fields"]
    assert "半导体" not in industry_schema["enum"]
    assert provider.requests[0].json_schema == provider.requests[1].json_schema
    assert "半导体" in refreshed_industry_schema["properties"]["客户行业"]["enum"]
    assert len(provider.requests) == 3
    with session_factory() as session:
        events = list(session.scalars(select(OutboxEvent).order_by(OutboxEvent.sequence)))
    assert [event.id for event in events] == [first_event_id, second_event_id]
    assert [event.sequence for event in events] == [1, 2]
    assert [event.status for event in events] == ["succeeded", "succeeded"]

    service.consume(first_event_id)
    assert len(provider.requests) == 3
    assert len(adapter.get_records()) == 2


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


def seed_first_success_notice_facts(
    session_factory: sessionmaker[Session],
    *,
    sales_user_id: str,
    message_id: str,
    lead_id: str,
    sync_status: str = "succeeded",
    resolution_status: str = "assigned",
    active: bool = True,
    event_type: str = "message_received",
    has_record_id: bool = True,
) -> None:
    """准备首次成功通知判定所需的持久化消息、归属和同步事实。

    参数：session_factory 为隔离数据库；其余参数控制销售状态、来源事件、线索归属与远端同步结论。
    返回值：无。
    异常：数据库约束错误向测试传播。
    副作用：新增通知判定所需的测试记录，不调用外部服务。
    """
    record_id = f"table-{lead_id}" if has_record_id else None
    with session_factory.begin() as session:
        actor = session.get(SalesAuthorization, sales_user_id)
        if actor is None:
            session.add(
                SalesAuthorization(
                    wecom_user_id=sales_user_id,
                    is_authorized=True,
                    is_active=active,
                )
            )
        else:
            actor.is_active = active
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
                raw_payload={"text": "客户需求"},
                normalized_text="客户需求",
            )
        )
        session.add(
            OutboxEvent(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
                event_type=event_type,
            )
        )
        session.add(
            Lead(
                id=lead_id,
                source_message_id=message_id,
                original_capturing_sales_user_id=sales_user_id,
                smart_table_owner_user_id=sales_user_id,
                smart_table_record_id=record_id,
                lifecycle_state="synced",
                field_values={"线索名称": lead_id},
            )
        )
        session.add(
            SmartTableSync(
                lead_id=lead_id,
                source_message_id=message_id,
                smart_table_record_id=record_id,
                status=sync_status,
            )
        )
        session.add(
            LeadMessageResolution(
                message_id=message_id,
                segment_index=0,
                lead_id=lead_id if resolution_status == "assigned" else None,
                status=resolution_status,
            )
        )


def issue_first_success_notice(
    session_factory: sessionmaker[Session], message_id: str, lead_id: str
) -> None:
    """在测试事务中调用生产的首次成功通知确定性门禁。

    参数：session_factory 为隔离数据库；message_id 与 lead_id 指向已准备好的来源事实。
    返回值：无。
    异常：查询或插入错误向测试传播。
    副作用：满足门禁时新增一条持久化首次成功通知。
    """
    with session_factory.begin() as session:
        event = session.scalar(select(OutboxEvent).where(OutboxEvent.message_id == message_id))
        message = session.get(IncomingMessage, message_id)
        lead = session.get(Lead, lead_id)
        sync = session.scalar(select(SmartTableSync).where(SmartTableSync.lead_id == lead_id))
        assert event is not None and message is not None and lead is not None and sync is not None
        FirstTextLeadWorkspaceService._queue_first_success_notification(
            session, event, message, lead, sync, 0
        )


def configure_first_success_notice(
    monkeypatch: pytest.MonkeyPatch, url: str | None = "https://example.test/smart-table"
) -> None:
    """为通知用例注入隔离链接，阻止测试读取本机受保护配置。"""
    settings = Settings(
        _env_file=None,
        lead_first_success_link_enabled=True,
        lead_smart_table_url=url,
    )
    monkeypatch.setattr("app.leads.service.get_settings", lambda: settings)


def sync_ai_review_for_new_lead(
    session_factory: sessionmaker[Session],
    adapter: MockSmartTableAdapter,
    *,
    message_id: str,
    sales_user_id: str,
    lead_id: str,
) -> tuple[int, LeadProcessingResult]:
    """通过 AI 审核最终同步路径创建一条测试线索及其已核实表格记录。

    参数：session_factory 为隔离数据库；adapter 为表格边界；其余参数标识来源消息、销售和线索。
    返回值：Outbox 事件标识及 AI 审核消费结果。
    异常：数据库或表格同步失败时由被测业务服务转换或传播。
    副作用：创建测试消息、Lead、SmartTableSync，并调用 AI 审核表格路径。
    """
    event_id = persist_outbox_text(
        session_factory,
        message_id=message_id,
        sales_user_id=sales_user_id,
        text="客户：AI审核客户；联系人：审核联系人",
    )
    with session_factory.begin() as session:
        session.add(
            Lead(
                id=lead_id,
                source_message_id=message_id,
                original_capturing_sales_user_id=sales_user_id,
                smart_table_owner_user_id=sales_user_id,
                lifecycle_state="pending_create",
                field_values={"线索名称": "AI审核客户"},
            )
        )
        session.add(
            SmartTableSync(
                lead_id=lead_id,
                source_message_id=message_id,
                status="pending",
            )
        )
    request = AIReviewRequest(
        source_message_id=message_id,
        sales_user_id=sales_user_id,
        lead_id=lead_id,
        outbox_event_id=event_id,
        patch=ExtractedLeadPatch(
            trace_id=f"trace-{message_id}",
            analysis=LeadAnalysis(intent="NEW_LEAD"),
            fields={"线索名称": "AI审核客户"},
            pending_confirmation_fields=(),
            low_confidence_candidates={},
        ),
        creates_lead=True,
    )
    result = FirstTextLeadWorkspaceService(session_factory, adapter)._sync_ai_review(request)
    return event_id, result


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
            "手机号17315865903",
        ),
        ("message-real-b1", "天津华印包装马经理，印刷包装"),
        (
            "message-real-b2",
            "电话13752288666",
        ),
    )
    provider = MockLLMProvider(
        [
            json.dumps(
                {
                    "intent": "NEW_LEAD",
                    "customer_reference": {},
                    "crm_fields": {
                        "线索名称": "合肥光曜新能源",
                        "联系人": "孙经理",
                        "工艺": "视觉检测",
                    },
                    "enrichment": {"预算": "预算120万"},
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
                    "crm_fields": {"电话": "17315865903"},
                    "enrichment": {},
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

    assert len(provider.requests) == 4
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
    with session_factory() as session:
        received = session.get(IncomingMessage, "message-1").received_at
    capture_time = (
        received.replace(tzinfo=UTC)
        .astimezone(ZoneInfo("Asia/Shanghai"))
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    assert record.fields == {
        **DEFAULT_LEAD_BUSINESS_VALUES,
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
        **DEFAULT_LEAD_BUSINESS_VALUES,
        "录入时间": capture_time,
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
    assert {source.field_name for source in provenance} == {
        "线索名称",
        "联系人",
        "工艺",
        "备注",
        *DEFAULT_LEAD_BUSINESS_VALUES,
    }
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
    with session_factory() as session:
        first_capture = session.get(Lead, failed.lead_id).field_values["录入时间"]
    retried = service.consume(event_id)

    assert failed.status is LeadProcessingStatus.SYNC_FAILED
    assert retried.status is LeadProcessingStatus.CREATED
    record = adapter.get_record(retried.smart_table_record_id)
    assert not {"创建时间", "录入时间"}.intersection(record.fields)
    with session_factory() as session:
        assert session.get(Lead, retried.lead_id).field_values["录入时间"] == first_capture
    assert all(record.fields[name] == value for name, value in DEFAULT_LEAD_BUSINESS_VALUES.items())
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
                        "crm_fields": {"手机": "13600000000"},
                        "enrichment": {},
                        "confidence_by_field": {"手机": 0.95},
                        "conflicts": [],
                        "warnings": [],
                    }
                ),
                json.dumps(
                    {
                        "intent": "UPDATE_LEAD",
                        "customer_reference": {},
                        "crm_fields": {},
                        "enrichment": {"预算": "预算16万"},
                        "confidence_by_field": {},
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
            text="手机号13600000000",
        )
        second = service.consume(second_event_id)

        third_event_id = persist_outbox_text(
            session_factory,
            message_id="message-smart-table-recovery-c",
            sales_user_id="sales-1",
            text="预算16万",
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
                    "crm_fields": {"线索名称": "公司三", "工艺": "视觉检测"},
                    "enrichment": {"预算": "16万"},
                    "confidence_by_field": {"线索名称": 0.99, "工艺": 0.99},
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
        text="公司三补充工艺：视觉检测，预算16万",
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
        notifications = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        ).all()

    assert lead is not None and lead.smart_table_record_id == "acked-record-1"
    assert sync is not None
    assert sync.smart_table_record_id == "acked-record-1"
    assert sync.status == "retrying"
    assert sync.error_summary == "field_patch_pending"
    assert event is not None and event.status == "retrying"
    assert notifications == []


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
                    "crm_fields": {
                        "线索名称": "艾利特",
                        "联系人": "杨经理",
                        "手机": "17318902085",
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
                        "crm_fields": {},
                        "enrichment": {"预算": "预算38万"},
                        "confidence_by_field": {},
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
        text="预算38万",
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
                        "crm_fields": {},
                        "enrichment": {"预算": "预算38万"},
                        "confidence_by_field": {},
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
        text="预算38万",
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
    assert updated.status is LeadProcessingStatus.UNASSIGNED
    assert updated.lead_id is None
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
        ("message-aaaaa-2", "苏州安科想做视觉检测"),
        ("message-aaaaa-3", "预算大概35万"),
        ("message-aaaaa-4", "手机号13800000001"),
        ("message-aaaaa-5", "苏州安科预计明年Q1启动"),
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
            semantic_single_json(
                intent="UPDATE_LEAD",
                crm_fields={"线索名称": "苏州安科", "工艺": ["视觉检测"]},
            ),
            semantic_single_json(
                intent="UPDATE_LEAD", crm_fields={}, enrichment={"预算": "预算大概35万"}
            ),
            semantic_single_json(
                intent="UPDATE_LEAD", crm_fields={"手机": "13800000001"}
            ),
            semantic_single_json(
                intent="UPDATE_LEAD",
                crm_fields={"线索名称": "苏州安科"},
                enrichment={"特殊要求": "预计明年Q1启动"},
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


def test_grounded_same_company_ai_update_uses_context_without_creating_second_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 AI 从原文确认同一公司后可更新当前线索，而无需创建第二条记录。

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
                raw_payload={"text": "长广溪智造现在计划做装配项目。"},
                normalized_text="长广溪智造现在计划做装配项目。",
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
                        "crm_fields": {"线索名称": "长广溪智造", "工艺": "装配"},
                        "enrichment": {},
                        "confidence_by_field": {"线索名称": 0.99, "工艺": 0.9},
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


def test_new_company_card_does_not_inherit_active_context_and_quantity_is_unassigned(
    session_factory: sessionmaker[Session],
) -> None:
    """验证名片中的新公司不会继承旧上下文，后续无身份需求也不继承新上下文。

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
            "邢伟伟\n数字化业务中心总监 / 合伙人\nMobile: 155 0000 1234\n"
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
                    "intent": "UPDATE_LEAD",
                    "customer_reference": {
                        "company": "长广溪智能制造（无锡）有限公司",
                        "contact": "邢伟伟",
                        "title": "数字化业务中心总监 / 合伙人",
                        "phone": "15500001234",
                    },
                    "crm_fields": {
                        "线索名称": "长广溪智能制造（无锡）有限公司",
                        "联系人": "邢伟伟",
                        "职务": "数字化业务中心总监 / 合伙人",
                        "手机": "15500001234",
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
    assert len(adapter.get_records()) == 2
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
    resolution_by_message = {resolution.message_id: resolution for resolution in resolutions}
    assert resolution_by_message["message-context-card-first"].lead_id == first_result.lead_id
    card_lead_id = resolution_by_message["message-context-card"].lead_id
    assert card_lead_id is not None
    assert card_lead_id != first_result.lead_id
    assert resolution_by_message["message-context-card-follow-up"].status == "unassigned"
    assert resolution_by_message["message-context-card-follow-up"].lead_id is None
    first_record = adapter.get_record(first_result.smart_table_record_id or "")
    assert first_record is not None
    assert first_record.fields["线索名称"] == "邢总公司"
    assert first_record.fields.get("手机") != "15500001234"
    card_record_id = next(
        record.record_id
        for record in adapter.get_records()
        if record.fields.get("线索名称") == "长广溪智能制造（无锡）有限公司"
    )
    assert card_record_id != first_result.smart_table_record_id
    card_record = adapter.get_record(card_record_id)
    assert card_record is not None
    assert card_record.fields["联系人"] == "邢伟伟"
    assert card_record.fields["手机"] == "15500001234"


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
            [semantic_single_json(intent="NEW_LEAD", crm_fields={"手机": "13800000001"})]
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
                    crm_fields={"线索名称": "西门子", "联系人": "陈总"},
                    enrichment={"客户需求/痛点": "了解焊接产品"},
                ),
                semantic_single_json(
                    intent="UPDATE_LEAD", crm_fields={"手机": model_phone_b}
                ),
                semantic_single_json(
                    intent="NEW_LEAD",
                    crm_fields={"线索名称": "无锡宝通", "联系人": "郭总"},
                    enrichment={"客户需求/痛点": "双臂机器人感兴趣"},
                ),
                semantic_single_json(
                    intent="NEW_LEAD",
                    crm_fields={"线索名称": "隆盛科技", "联系人": "张总"},
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


@pytest.mark.parametrize(
    ("text", "crm_fields", "enrichment"),
    [
        (
            "客户对双臂机器人感兴趣，想采购一套",
            {"线索名称": "双臂机器人"},
            {"客户需求/痛点": "双臂机器人感兴趣，想采购一套"},
        ),
        (
            "隆盛科技的采购负责人张总有上下料需求",
            {
                "线索名称": "隆盛科技的采购负责人张总有上下料需求",
                "联系人": "张总",
                "职务": "采购负责人",
            },
            {"客户需求/痛点": "上下料需求"},
        ),
    ],
)
def test_company_candidate_overlapping_demand_or_identity_details_fails_closed(
    session_factory: sessionmaker[Session],
    text: str,
    crm_fields: dict[str, object],
    enrichment: dict[str, str],
) -> None:
    """需求词或拼接的联系人/职位/需求不能仅凭原文子串成为公司身份。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="semantic-company-grounding-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    seed = service.consume(seed_event)
    assert seed.lead_id is not None
    event_id = persist_outbox_text(
        session_factory,
        message_id="semantic-company-grounding-candidate",
        sales_user_id="sales-1",
        text=text,
    )
    gateway = AIGateway(
        MockLLMProvider(
            [semantic_single_json(intent="NEW_LEAD", crm_fields=crm_fields, enrichment=enrichment)]
        )
    )

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    assert result.lead_id is None
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Lead)) == 1


def test_expired_context_does_not_assign_weak_ai_update(
    session_factory: sessionmaker[Session],
) -> None:
    """超过上下文 TTL 的需求片段不能被模型 UPDATE 意图猜测到旧客户。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="expired-context-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None
    event_id = persist_outbox_text(
        session_factory,
        message_id="expired-context-weak-fragment",
        sales_user_id="sales-1",
        text="预算25万，想做装配",
    )
    with session_factory.begin() as session:
        context = session.get(SalesLeadContext, "sales-1")
        assert context is not None
        context.last_message_received_at -= timedelta(hours=1)
    gateway = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="UPDATE_LEAD",
                    crm_fields={"工艺": ["装配"]},
                    enrichment={"预算": "预算25万"},
                )
            ]
        )
    )

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    assert result.lead_id is None


def test_ai_request_failure_does_not_bind_to_active_context(
    session_factory: sessionmaker[Session],
) -> None:
    """AI 失败时保留待处理检查点，不把新消息猜测绑定到 active Lead。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="ai-failure-context-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None
    event_id = persist_outbox_text(
        session_factory,
        message_id="ai-failure-context-new-message",
        sales_user_id="sales-1",
        text="今天接触了一家新企业，需要机器人方案",
    )
    gateway = AIGateway(MockLLMProvider([LLMProviderError("network")]))

    result = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    ).consume(event_id)

    assert result.status is LeadProcessingStatus.SYNC_FAILED
    with session_factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "ai-failure-context-new-message"
            )
        )
        event = session.get(OutboxEvent, event_id)
        assert session.scalar(select(func.count()).select_from(Lead)) == 1
    assert resolution is None
    assert event is not None and event.status == "failed_pending_review"


def test_ai_company_subject_from_free_language_overrides_active_context(
    session_factory: sessionmaker[Session],
) -> None:
    """自由表达中新公司即使被模型标为 UPDATE，也不得归到当前旧 Lead。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="semantic-boundary-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    seed = service.consume(seed_event)
    assert seed.lead_id is not None

    new_message = "今天接触了无锡宝通，对双臂机器人感兴趣"
    new_event = persist_outbox_text(
        session_factory,
        message_id="semantic-boundary-new-company",
        sales_user_id="sales-1",
        text=new_message,
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="UPDATE_LEAD",
                    crm_fields={"线索名称": "无锡宝通"},
                    enrichment={"客户需求/痛点": "对双臂机器人感兴趣"},
                )
            ]
        )
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    )

    result = service.consume(new_event)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.lead_id is not None and result.lead_id != seed.lead_id
    record = adapter.get_record(result.smart_table_record_id or "")
    assert record is not None
    assert record.fields["线索名称"] == "无锡宝通"


def test_unverified_ai_company_candidate_does_not_fall_back_to_active_context(
    session_factory: sessionmaker[Session],
) -> None:
    """AI 公司候选无法回指当前来源时必须待归属，不能退回旧 Lead。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="unverified-identity-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    service = FirstTextLeadWorkspaceService(session_factory, adapter)
    seed = service.consume(seed_event)
    assert seed.lead_id is not None

    message_id = "unverified-identity-new-customer"
    event_id = persist_outbox_text(
        session_factory,
        message_id=message_id,
        sales_user_id="sales-1",
        text="今天接触了一家企业，对焊接自动化有兴趣",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [semantic_single_json(intent="UPDATE_LEAD", crm_fields={"线索名称": "西门子"})]
        )
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    )

    result = service.consume(event_id)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    assert result.lead_id is None
    assert len(adapter.get_records()) == 1
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Lead)) == 1
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == message_id
            )
        )
        assert resolution is not None and resolution.lead_id is None


def test_ai_company_identity_is_not_rejected_by_business_word_prefix(
    session_factory: sessionmaker[Session],
) -> None:
    """公司候选不会因名称以业务常用词开头而被确定性负向词表拒绝。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="business-word-company-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None

    event_id = persist_outbox_text(
        session_factory,
        message_id="business-word-company-new",
        sales_user_id="sales-1",
        text="机器人创新中心的王总想了解焊接产品",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="NEW_LEAD",
                    crm_fields={"线索名称": "机器人创新中心", "联系人": "王总"},
                    enrichment={"客户需求/痛点": "想了解焊接产品"},
                )
            ]
        )
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    )

    result = service.consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.lead_id is not None and result.lead_id != seed.lead_id
    record = adapter.get_record(result.smart_table_record_id or "")
    assert record is not None and record.fields["线索名称"] == "机器人创新中心"


@pytest.mark.parametrize(
    ("message_text", "company_name", "contact_name", "enrichment"),
    [
        (
            "西门子的陈总想了解焊接产品",
            "西门子",
            "陈总",
            "想了解焊接产品",
        ),
        (
            "无锡宝通的 CIO 郭总对双臂机器人感兴趣",
            "无锡宝通",
            "郭总",
            "双臂机器人感兴趣",
        ),
        (
            "隆盛科技的采购负责人张总有上下料需求",
            "隆盛科技",
            "张总",
            "上下料需求",
        ),
    ],
)
def test_ai_lead_subject_and_contact_are_extracted_from_free_language(
    session_factory: sessionmaker[Session],
    message_text: str,
    company_name: str,
    contact_name: str,
    enrichment: str,
) -> None:
    """AI 从自由表达理解公司主体、联系人和需求，不依赖职位句式正则。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="free-language-context-seed",
        sales_user_id="sales-1",
        text="客户：旧客户甲；联系人：陈总",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None
    event_id = persist_outbox_text(
        session_factory,
        message_id="free-language-new-company",
        sales_user_id="sales-1",
        text=message_text,
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="UPDATE_LEAD",
                    crm_fields={"线索名称": company_name, "联系人": contact_name},
                    enrichment={"客户需求/痛点": enrichment},
                )
            ]
        )
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    )

    result = service.consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.lead_id is not None and result.lead_id != seed.lead_id
    record = adapter.get_record(result.smart_table_record_id or "")
    assert record is not None
    assert record.fields["线索名称"] == company_name
    assert record.fields["联系人"] == contact_name


def test_same_company_ai_candidate_updates_current_lead(
    session_factory: sessionmaker[Session],
) -> None:
    """原文语义主体与当前公司一致时，AI 补充更新已有 Lead。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="same-company-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None
    event_id = persist_outbox_text(
        session_factory,
        message_id="same-company-follow-up",
        sales_user_id="sales-1",
        text="西门子本次还希望评估视觉检测工位",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="NEW_LEAD",
                    crm_fields={"线索名称": "西门子"},
                    enrichment={"客户需求/痛点": "希望评估视觉检测工位"},
                )
            ]
        )
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    )

    result = service.consume(event_id)

    assert result.status is LeadProcessingStatus.UPDATED
    assert result.lead_id == seed.lead_id
    assert len(adapter.get_records()) == 1


def test_mentioned_company_without_company_subject_is_not_routed_by_context(
    session_factory: sessionmaker[Session],
) -> None:
    """AI 未确认原文提及的公司是客户主体时，不得把需求写入当前线索。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="mention-only-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None
    record_before = adapter.get_record(seed.smart_table_record_id or "")
    assert record_before is not None
    fields_before = dict(record_before.fields)
    event_id = persist_outbox_text(
        session_factory,
        message_id="mention-only-follow-up",
        sales_user_id="sales-1",
        text="西门子项目还提到了无锡宝通的竞品型号，需求仍是焊接自动化",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="UPDATE_LEAD",
                    crm_fields={},
                    enrichment={"客户需求/痛点": "需求仍是焊接自动化"},
                )
            ]
        )
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    )

    result = service.consume(event_id)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    assert result.lead_id is None
    assert len(adapter.get_records()) == 1
    record_after = adapter.get_record(seed.smart_table_record_id or "")
    assert record_after is not None
    assert record_after.fields == fields_before


def test_new_customer_without_reliable_company_candidate_is_unassigned(
    session_factory: sessionmaker[Session],
) -> None:
    """AI 判定为新客户但没有可靠公司候选时，不用活动上下文补猜归属。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="unknown-company-seed",
        sales_user_id="sales-1",
        text="客户：西门子；联系人：陈总",
    )
    FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    event_id = persist_outbox_text(
        session_factory,
        message_id="unknown-company-message",
        sales_user_id="sales-1",
        text="今天遇到一位客户，需求暂时不清楚",
    )
    gateway = AIGateway(
        MockLLMProvider(
            [semantic_single_json(intent="NEW_LEAD", crm_fields={})]
        )
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    )

    result = service.consume(event_id)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    assert result.lead_id is None
    assert len(adapter.get_records()) == 1


@pytest.mark.parametrize(
    ("message_text", "crm_fields"),
    [
        ("今天接触了无锡宝通，对双臂机器人有需求", {}),
        (
            "今天接触了无锡宝通，电话13800000002，对双臂机器人有需求",
            {"电话": "13800000002"},
        ),
    ],
)
def test_update_without_source_company_identity_cannot_write_active_lead(
    session_factory: sessionmaker[Session],
    message_text: str,
    crm_fields: dict[str, object],
) -> None:
    """AI 漏掉新公司身份时，不能把自然语言需求写入活动线索或智能表格。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed_event = persist_outbox_text(
        session_factory,
        message_id="identity-gate-seed-a",
        sales_user_id="identity-gate-sales",
        text="客户：西门子；联系人：陈总",
    )
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(seed_event)
    assert seed.lead_id is not None
    record_before = adapter.get_record(seed.smart_table_record_id or "")
    assert record_before is not None
    fields_before = dict(record_before.fields)
    with session_factory() as session:
        lead_before = session.get(Lead, seed.lead_id)
        assert lead_before is not None
        field_values_before = dict(lead_before.field_values)
        enrichment_before = dict(lead_before.enrichment_values)

    message_id = "identity-gate-new-company-without-ai-name"
    event_id = persist_outbox_text(
        session_factory,
        message_id=message_id,
        sales_user_id="identity-gate-sales",
        text=message_text,
    )
    gateway = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="UPDATE_LEAD",
                    crm_fields=crm_fields,
                    enrichment={"客户需求/痛点": "对双臂机器人有需求"},
                )
            ]
        )
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=gateway
    )

    with (
        patch.object(adapter, "create_record", wraps=adapter.create_record) as create_record,
        patch.object(adapter, "update_record", wraps=adapter.update_record) as update_record,
    ):
        result = service.consume(event_id)

    assert result.status is LeadProcessingStatus.UNASSIGNED
    assert result.lead_id is None
    create_record.assert_not_called()
    update_record.assert_not_called()
    record_after = adapter.get_record(seed.smart_table_record_id or "")
    assert record_after is not None
    assert record_after.fields == fields_before
    with session_factory() as session:
        leads = session.scalars(select(Lead)).all()
        lead_after = session.get(Lead, seed.lead_id)
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == message_id
            )
        )
        provenance_count = session.scalar(
            select(func.count()).select_from(LeadFieldProvenance).where(
                LeadFieldProvenance.source_message_id == message_id
            )
        )
        sync_count = session.scalar(
            select(func.count()).select_from(SmartTableSync).where(
                SmartTableSync.source_message_id == message_id
            )
        )
    assert len(leads) == 1
    assert lead_after is not None
    assert lead_after.field_values == field_values_before
    assert lead_after.enrichment_values == enrichment_before
    assert resolution is not None and resolution.status == "unassigned"
    assert resolution.lead_id is None
    assert provenance_count == 0
    assert sync_count == 0


def test_confirmed_first_smart_table_success_queues_one_link_notice(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证真实 Lead 流程确认远端记录后才持久化首次成功链接通知。"""
    configure_first_success_notice(monkeypatch)
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-first-success-link",
        sales_user_id="sales-first-link",
        text="客户：链接通知测试公司；联系人：陈工；需求：码垛机器人",
    )
    service = FirstTextLeadWorkspaceService(
        session_factory,
        MockSmartTableAdapter(schema=build_required_smart_table_schema()),
    )

    result = service.consume(event_id)
    replay = service.consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.smart_table_record_id is not None
    assert replay.status is LeadProcessingStatus.ALREADY_PROCESSED
    with session_factory() as session:
        notice = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        )
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == result.lead_id)
        )
    assert sync is not None and sync.status == "succeeded"
    assert sync.smart_table_record_id == result.smart_table_record_id
    assert notice is not None
    assert notice.sales_user_id == "sales-first-link"
    assert "[📋 打开需求登记智能表格](https://example.test/smart-table)" in (notice.content or "")
    assert "后续可继续发送客户需求，我会自动录入。你可以随时打开智能表格查看和完善信息。" in (
        notice.content or ""
    )


def test_ai_review_and_deterministic_success_share_one_notice_and_replay_does_not_recreate(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 AI 与确定性建档共用首次通知，并在通知重试和消息重放后保持表格建档幂等。"""
    configure_first_success_notice(monkeypatch)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    deterministic_event_id = persist_outbox_text(
        session_factory,
        message_id="message-deterministic-first-success",
        sales_user_id="sales-cross-path",
        text="客户：确定性首次客户；联系人：确定性联系人；需求：码垛机器人",
    )
    deterministic_result = FirstTextLeadWorkspaceService(session_factory, adapter).consume(
        deterministic_event_id
    )
    assert deterministic_result.status is LeadProcessingStatus.CREATED

    ai_event_id, ai_result = sync_ai_review_for_new_lead(
        session_factory,
        adapter,
        message_id="message-ai-second-success",
        sales_user_id="sales-cross-path",
        lead_id="lead-ai-second-success",
    )
    assert ai_result.status is LeadProcessingStatus.CREATED
    assert ai_result.smart_table_record_id is not None
    record_ids_before_recovery = {record.record_id for record in adapter.get_records()}

    class FailOnceClient:
        """首次发送失败、重启后的第二次发送成功。"""

        def __init__(self) -> None:
            """初始化一次失败计数。"""
            self.calls = 0

        async def send_message(
            self, userid_or_chatid: str, body: dict[str, object]
        ) -> dict[str, str]:
            """模拟可靠通知的暂时失败及恢复。"""
            del userid_or_chatid, body
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("temporary")
            return {"msgid": "notice-delivered"}

    client = FailOnceClient()
    sender = WecomOutboundNotificationSender(session_factory, client)
    assert asyncio.run(sender.send_pending_once()) == 0
    replay = FirstTextLeadWorkspaceService(session_factory, adapter).consume(ai_event_id)
    assert replay.status is LeadProcessingStatus.ALREADY_PROCESSED
    restarted_sender = WecomOutboundNotificationSender(session_factory, client)
    assert asyncio.run(restarted_sender.send_pending_once()) == 1

    with session_factory() as session:
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        ).all()
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == "lead-ai-second-success")
        )
        receipt_or_command_events = session.scalars(
            select(OutboxEvent).where(
                OutboxEvent.message_id.in_(
                    ["message-ai-second-success", "message-deterministic-first-success"]
                )
            )
        ).all()
    assert len(notices) == 1 and notices[0].status == "succeeded"
    assert sync is not None and sync.status == "succeeded"
    assert len(receipt_or_command_events) == 2
    assert {record.record_id for record in adapter.get_records()} == record_ids_before_recovery


def test_ai_review_patch_failure_does_not_issue_first_success_notice(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 AI 审核字段补丁失败时保留可恢复同步事实且不发行首次链接。"""
    configure_first_success_notice(monkeypatch)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    with patch.object(adapter, "update_record", side_effect=RuntimeError("patch failed")):
        _, result = sync_ai_review_for_new_lead(
            session_factory,
            adapter,
            message_id="message-ai-patch-failed",
            sales_user_id="sales-ai-patch-failed",
            lead_id="lead-ai-patch-failed",
        )

    assert result.status is LeadProcessingStatus.SYNC_FAILED
    with session_factory() as session:
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == "lead-ai-patch-failed")
        )
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        ).all()
    assert sync is not None and sync.status == "retrying"
    assert len(adapter.get_records()) == 1
    assert notices == []


def test_ai_review_unverified_remote_create_does_not_issue_first_success_notice(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 AI 建档的远端 ACK 尚未核实时不发行首次链接。"""
    configure_first_success_notice(monkeypatch)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    unverified = SmartTableWriteVerificationError(
        ["线索名称"], remote_record_id="remote-unverified-record"
    )
    with patch.object(adapter, "create_record", side_effect=unverified):
        _, result = sync_ai_review_for_new_lead(
            session_factory,
            adapter,
            message_id="message-ai-unverified-create",
            sales_user_id="sales-ai-unverified",
            lead_id="lead-ai-unverified",
        )

    assert result.status is LeadProcessingStatus.SYNC_FAILED
    with session_factory() as session:
        lead = session.get(Lead, "lead-ai-unverified")
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == "lead-ai-unverified")
        )
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        ).all()
    assert lead is not None and lead.smart_table_record_id == "remote-unverified-record"
    assert sync is not None and sync.status == "retrying"
    assert notices == []


def test_ai_review_first_success_queues_one_link_notice(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 AI 审核路径首次核实表格记录后与成功事务一同登记链接通知。"""
    configure_first_success_notice(monkeypatch)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    _, result = sync_ai_review_for_new_lead(
        session_factory,
        adapter,
        message_id="message-ai-first-only-success",
        sales_user_id="sales-ai-first-only",
        lead_id="lead-ai-first-only",
    )

    assert result.status is LeadProcessingStatus.CREATED
    with session_factory() as session:
        notice = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        )
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == "lead-ai-first-only")
        )
    assert notice is not None and notice.sales_user_id == "sales-ai-first-only"
    assert "[📋 打开需求登记智能表格](https://example.test/smart-table)" in (
        notice.content or ""
    )
    assert sync is not None and sync.status == "succeeded"
    assert sync.smart_table_record_id == result.smart_table_record_id


def test_failed_first_sync_then_success_and_multiple_leads_issue_per_user_once(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证失败不消耗首次机会，多 Lead 和重放同用户只形成一条逻辑通知。"""
    configure_first_success_notice(monkeypatch)
    seed_first_success_notice_facts(
        session_factory,
        sales_user_id="sales-repeat",
        message_id="message-first-failed",
        lead_id="lead-first-failed",
        sync_status="retrying",
        resolution_status="processing",
    )
    issue_first_success_notice(session_factory, "message-first-failed", "lead-first-failed")
    seed_first_success_notice_facts(
        session_factory,
        sales_user_id="sales-repeat",
        message_id="message-first-ok",
        lead_id="lead-first-ok",
    )
    issue_first_success_notice(session_factory, "message-first-ok", "lead-first-ok")
    seed_first_success_notice_facts(
        session_factory,
        sales_user_id="sales-repeat",
        message_id="message-second-lead",
        lead_id="lead-second-lead",
    )
    issue_first_success_notice(session_factory, "message-second-lead", "lead-second-lead")
    issue_first_success_notice(session_factory, "message-first-ok", "lead-first-ok")
    seed_first_success_notice_facts(
        session_factory,
        sales_user_id="sales-independent",
        message_id="message-independent-user",
        lead_id="lead-independent-user",
    )
    issue_first_success_notice(
        session_factory, "message-independent-user", "lead-independent-user"
    )

    with session_factory() as session:
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        ).all()

    assert sorted(notice.sales_user_id for notice in notices) == [
        "sales-independent",
        "sales-repeat",
    ]


def test_first_success_notice_is_sales_scoped_and_suppresses_historical_users(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证首次通知按销售隔离，已有成功记录的历史销售不会获得上线补发。"""
    configure_first_success_notice(monkeypatch)
    seed_first_success_notice_facts(
        session_factory,
        sales_user_id="sales-history",
        message_id="message-history-old",
        lead_id="lead-history-old",
    )
    seed_first_success_notice_facts(
        session_factory,
        sales_user_id="sales-history",
        message_id="message-history-new",
        lead_id="lead-history-new",
    )
    issue_first_success_notice(session_factory, "message-history-new", "lead-history-new")
    seed_first_success_notice_facts(
        session_factory,
        sales_user_id="sales-new-user",
        message_id="message-new-user",
        lead_id="lead-new-user",
    )
    issue_first_success_notice(session_factory, "message-new-user", "lead-new-user")

    with session_factory() as session:
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        ).all()

    assert [notice.sales_user_id for notice in notices] == ["sales-new-user"]


def test_first_success_notice_requires_active_assigned_verified_facts_and_configured_link(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证停用、待归属、未核实、非线索指令和空链接均不能发行首次通知。"""
    configure_first_success_notice(monkeypatch)
    cases = (
        (
            "sales-inactive",
            "inactive",
            "succeeded",
            "assigned",
            False,
            "message_received",
            True,
        ),
        (
            "sales-unassigned",
            "unassigned",
            "succeeded",
            "unassigned",
            True,
            "message_received",
            True,
        ),
        (
            "sales-unverified",
            "unverified",
            "retrying",
            "processing",
            True,
            "message_received",
            True,
        ),
        (
            "sales-review-required",
            "review-required",
            "failed_pending_review",
            "processing",
            True,
            "message_received",
            True,
        ),
        (
            "sales-command",
            "command",
            "succeeded",
            "assigned",
            True,
            "crm_submission_command",
            True,
        ),
        (
            "sales-no-record",
            "no-record",
            "succeeded",
            "assigned",
            True,
            "message_received",
            False,
        ),
    )
    for user_id, suffix, status, resolution, active, event_type, has_record_id in cases:
        message_id = f"message-{suffix}"
        lead_id = f"lead-{suffix}"
        seed_first_success_notice_facts(
            session_factory,
            sales_user_id=user_id,
            message_id=message_id,
            lead_id=lead_id,
            sync_status=status,
            resolution_status=resolution,
            active=active,
            event_type=event_type,
            has_record_id=has_record_id,
        )
        issue_first_success_notice(session_factory, message_id, lead_id)

    configure_first_success_notice(monkeypatch, url=None)
    seed_first_success_notice_facts(
        session_factory,
        sales_user_id="sales-no-url",
        message_id="message-no-url",
        lead_id="lead-no-url",
    )
    issue_first_success_notice(session_factory, "message-no-url", "lead-no-url")

    with session_factory() as session:
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        ).all()
    assert notices == []


def test_missing_link_configuration_does_not_block_smart_table_ingest(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证未配置链接时智能表格成功记录仍保留成功态且不创建空链接通知。"""
    configure_first_success_notice(monkeypatch, url=None)
    event_id = persist_outbox_text(
        session_factory,
        message_id="message-no-configured-link",
        sales_user_id="sales-no-configured-link",
        text="客户：无链接配置测试公司；联系人：周工；需求：装配机器人",
    )
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())

    result = FirstTextLeadWorkspaceService(session_factory, adapter).consume(event_id)

    assert result.status is LeadProcessingStatus.CREATED
    assert result.smart_table_record_id is not None
    assert len(adapter.get_records()) == 1
    with session_factory() as session:
        sync = session.scalar(
            select(SmartTableSync).where(SmartTableSync.lead_id == result.lead_id)
        )
        notices = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_first_smart_table_success"
            )
        ).all()
    assert sync is not None and sync.status == "succeeded"
    assert notices == []


@pytest.mark.parametrize("identity_shape", ["separate", "combined", "reference_only"])
def test_existing_semiconductor_company_with_city_enrichment_updates_original(
    session_factory: sessionmaker[Session],
    identity_shape: str,
) -> None:
    """原句中的城市与公司名重叠仍应更新本人唯一公司，不重复建行。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    seed = FirstTextLeadWorkspaceService(session_factory, adapter).consume(
        persist_outbox_text(
            session_factory,
            message_id="semiconductor-seed",
            sales_user_id="sales-1",
            text="客户：西安芯汇半导体；联系人：周工",
        )
    )
    text = (
        "西安芯汇半导体的周工，做晶圆封测的，想了解洁净室协作机器人做FOUP料盒搬运，"
        "预算60万，电话13800000000，是行业协会推荐过来的，预计明年3月前完成选型。"
    )
    response = json.loads(
        semantic_single_json(
            intent="UPDATE_LEAD",
            crm_fields={"线索名称": "西安芯汇半导体", "联系人": "周工", "手机": "13800000000"},
            enrichment={
                "城市/地区": "西安",
                "主营产品": "晶圆封测",
                "预算": "60万",
                "客户需求/痛点": "洁净室协作机器人做FOUP料盒搬运",
            },
        )
    )
    response["customer_reference"] = {"company_name": "西安芯汇半导体", "contact": "周工"}
    if identity_shape == "combined":
        response["crm_fields"]["线索名称"] = "西安芯汇半导体的周工"
    elif identity_shape == "reference_only":
        response["crm_fields"].pop("线索名称")
        response["confidence_by_field"].pop("线索名称")
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(MockLLMProvider([json.dumps(response)]))
    )
    event_id = persist_outbox_text(
        session_factory, message_id="semiconductor-update", sales_user_id="sales-1", text=text
    )
    result = service.consume(event_id)
    with session_factory() as session:
        audits = tuple(
            session.scalars(
                select(BusinessAuditEvent).where(
                    BusinessAuditEvent.message_id == "semiconductor-update"
                )
            )
        )
        assert result.status is LeadProcessingStatus.UPDATED, [
            (item.event_type, item.details) for item in audits
        ]
        lead = session.get(Lead, seed.lead_id)
        assert lead is not None and lead.enrichment_values["预算"] == "60万"
    assert result.lead_id == seed.lead_id
    assert len(adapter.get_records()) == 1
    assert adapter.get_record(seed.smart_table_record_id).fields["手机"] == "13800000000"
    assert service.consume(event_id).status is LeadProcessingStatus.ALREADY_PROCESSED


@pytest.mark.parametrize("confidence", [0.99, 0.70, 0.40])
def test_system_defaults_reliable_replacement_and_immutable_capture_time(
    session_factory: sessionmaker[Session],
    confidence: float,
) -> None:
    """四个默认值只让位于高置信度原文证据，接收时间跨日补充和重放不变。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first_event = persist_outbox_text(
        session_factory,
        message_id="defaults-first",
        sales_user_id="sales-1",
        text="客户：甲科技；联系人：李工",
    )
    received = datetime(2026, 10, 8, 15, 59, 58, tzinfo=UTC)
    with session_factory.begin() as session:
        session.get(IncomingMessage, "defaults-first").received_at = received
    first = FirstTextLeadWorkspaceService(session_factory, adapter).consume(first_event)
    record = adapter.get_record(first.smart_table_record_id)
    assert all(record.fields[name] == value for name, value in DEFAULT_LEAD_BUSINESS_VALUES.items())
    assert not {"创建时间", "录入时间"}.intersection(record.fields)
    # 模拟企业微信在建行时自动生成不同于消息接收时间的分钟级系统值；不能通过更新接口填它。
    adapter._records[record.record_id] = SmartTableRecord(
        record.record_id, {**record.fields, "创建时间": "2026-10-09 10:20"}
    )
    replacements = {
        "业务线": "车载机器人",
        "职务": "工程师",
        "沟通方式": "微信",
        "客户行业": "汽车",
    }
    text = "甲科技的李工，业务线车载机器人，职务工程师，沟通方式微信，客户行业汽车。"
    response = json.loads(
        semantic_single_json(
            intent="UPDATE_LEAD",
            crm_fields={
                "线索名称": "甲科技",
                **replacements,
            },
        )
    )
    response["confidence_by_field"].update({name: confidence for name in replacements})
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(MockLLMProvider([json.dumps(response)]))
    )
    event = persist_outbox_text(
        session_factory, message_id="defaults-update", sales_user_id="sales-1", text=text
    )
    assert service.consume(event).lead_id == first.lead_id
    assert service.consume(event).status is LeadProcessingStatus.ALREADY_PROCESSED
    assert len(adapter.get_records()) == 1
    final = adapter.get_record(first.smart_table_record_id)
    expected = replacements if confidence == 0.99 else DEFAULT_LEAD_BUSINESS_VALUES
    assert all(final.fields[name] == value for name, value in expected.items())
    assert "录入时间" not in final.fields
    assert final.fields["创建时间"] == "2026-10-09 10:20"
    with session_factory() as session:
        lead = session.get(Lead, first.lead_id)
        assert lead.field_values["录入时间"] == "2026-10-08 23:59:58"
        sources = session.scalars(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == first.lead_id,
                LeadFieldProvenance.field_name.in_(replacements),
            )
        ).all()
        assert len(sources) == 4
        assert all(source.is_system_default == (confidence != 0.99) for source in sources)


def test_system_defaults_respect_human_changes_and_confirmation(
    session_factory: sessionmaker[Session],
) -> None:
    """默认值被销售修改或显式确认后，高置信度 AI 也不能覆盖。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first = FirstTextLeadWorkspaceService(session_factory, adapter).consume(
        persist_outbox_text(
            session_factory,
            message_id="protected-defaults",
            sales_user_id="sales-1",
            text="客户：甲科技；联系人：李工",
        )
    )
    adapter.update_record(first.smart_table_record_id, {"职务": "总监", "客户行业": "新能源"})
    with session_factory.begin() as session:
        source = session.scalar(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == first.lead_id,
                LeadFieldProvenance.field_name == "业务线",
            )
        )
        source.is_user_confirmed = True
    response = semantic_single_json(
        intent="UPDATE_LEAD",
        crm_fields={
            "线索名称": "甲科技",
            "业务线": "车载机器人",
            "职务": "工程师",
            "客户行业": "汽车",
        },
    )
    service = FirstTextLeadWorkspaceService(
        session_factory, adapter, ai_gateway=AIGateway(MockLLMProvider([response]))
    )
    result = service.consume(
        persist_outbox_text(
            session_factory,
            message_id="protected-defaults-update",
            sales_user_id="sales-1",
            text="甲科技的李工，业务线车载机器人，职务工程师，客户行业汽车。",
        )
    )
    assert result.status is LeadProcessingStatus.UPDATED
    record = adapter.get_record(first.smart_table_record_id)
    assert record.fields["业务线"] == "协作机器人"
    assert record.fields["职务"] == "总监"
    assert record.fields["客户行业"] == "新能源"


@pytest.mark.parametrize("other_owner", ["sales-1", "sales-other"])
def test_free_text_company_match_stays_with_owner_and_ambiguous_targets_wait(
    session_factory: sessionmaker[Session],
    other_owner: str,
) -> None:
    """本人多条同名记录继续待归属，他人同名记录不影响本人的唯一目标。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first = FirstTextLeadWorkspaceService(session_factory, adapter).consume(
        persist_outbox_text(
            session_factory,
            message_id="identity-boundary-seed",
            sales_user_id="sales-1",
            text="客户：西安芯汇半导体；联系人：周工",
        )
    )
    from app.smart_table.adapter import SmartTableActor

    other_record = adapter.create_record(
        {"负责人": other_owner, "线索名称": "西安芯汇半导体"}, actor=SmartTableActor.ROBOT
    )
    with session_factory.begin() as session:
        if other_owner != "sales-1":
            session.add(
                SalesAuthorization(wecom_user_id=other_owner, is_authorized=True, is_active=True)
            )
        session.add(
            Lead(
                id="other-identity",
                original_capturing_sales_user_id=other_owner,
                smart_table_owner_user_id=other_owner,
                smart_table_record_id=other_record.record_id,
                field_values={"线索名称": "西安芯汇半导体"},
            )
        )
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(
            MockLLMProvider(
                [
                    semantic_single_json(
                        intent="UPDATE_LEAD",
                        crm_fields={
                            "线索名称": "西安芯汇半导体",
                            "联系人": "周工",
                            "手机": "13800000000",
                        },
                        enrichment={"城市/地区": "西安", "预算": "60万"},
                    )
                ]
            )
        ),
    )
    result = service.consume(
        persist_outbox_text(
            session_factory,
            message_id="identity-boundary-update",
            sales_user_id="sales-1",
            text="西安芯汇半导体的周工，预算60万，电话13800000000。",
        )
    )
    expected = (
        LeadProcessingStatus.UNASSIGNED
        if other_owner == "sales-1"
        else LeadProcessingStatus.UPDATED
    )
    assert result.status is expected
    assert len(adapter.get_records()) == 2
    if other_owner != "sales-1":
        assert result.lead_id == first.lead_id
    assert "手机" not in adapter.get_record(other_record.record_id).fields


def test_invalid_ai_enum_preserves_system_default(
    session_factory: sessionmaker[Session],
) -> None:
    """高置信度非法枚举只能保留为候选，不能覆盖合法系统默认值。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    event = persist_outbox_text(
        session_factory,
        message_id="invalid-default-enum",
        sales_user_id="sales-1",
        text="甲科技的李工，想了解工业机器人。",
    )
    service = FirstTextLeadWorkspaceService(
        session_factory,
        adapter,
        ai_gateway=AIGateway(
            MockLLMProvider(
                [
                    semantic_single_json(
                        intent="NEW_LEAD",
                        crm_fields={
                            "线索名称": "甲科技",
                            "联系人": "李工",
                            "业务线": "工业机器人",
                        },
                    )
                ]
            )
        ),
    )
    result = service.consume(event)
    assert result.status is LeadProcessingStatus.CREATED
    assert adapter.get_record(result.smart_table_record_id).fields["业务线"] == "协作机器人"


@pytest.mark.parametrize("human_edit", [False, True])
def test_default_replacement_failure_recovers_with_old_baseline_and_capture_time(
    session_factory: sessionmaker[Session],
    human_edit: bool,
) -> None:
    """可靠 AI 替换失败后复用旧同步基线恢复；期间人工修改仍优先，首次时间不变。"""
    from app.leads.review import LeadReviewService

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    first = FirstTextLeadWorkspaceService(session_factory, adapter).consume(
        persist_outbox_text(
            session_factory,
            message_id="default-retry-seed",
            sales_user_id="sales-1",
            text="客户：甲科技；联系人：李工",
        )
    )
    with session_factory() as session:
        first_time = session.get(Lead, first.lead_id).field_values["录入时间"]
    persist_outbox_text(
        session_factory,
        message_id="default-retry-update",
        sales_user_id="sales-1",
        text="甲科技职务工程师",
    )
    ai_patch = AIGateway(
        MockLLMProvider(
            [
                semantic_single_json(
                    intent="UPDATE_LEAD",
                    crm_fields={
                        "职务": "工程师",
                    },
                )
            ]
        )
    ).extract_fields("甲科技职务工程师")
    review = LeadReviewService(session_factory, adapter)
    with patch.object(
        adapter, "update_record", side_effect=ConnectionError("isolated update failure")
    ):
        with pytest.raises(ConnectionError):
            review.sync_ai_patch(first.lead_id, "default-retry-update", ai_patch)
    if human_edit:
        adapter.update_record(first.smart_table_record_id, {"职务": "总监"})
    # 后续消息无需重新产生已持久化的可靠补丁；只重试尚未成功同步的字段计划。
    review.sync_ai_patch(
        first.lead_id,
        "default-retry-update",
        ExtractedLeadPatch(
            trace_id="recover-default",
            analysis=LeadAnalysis(intent="UPDATE_LEAD"),
            fields={"创建时间": "2026-10-10 12:00", "录入时间": "2026-10-10 12:00:00"},
            pending_confirmation_fields=(),
            low_confidence_candidates={},
        ),
    )
    record = adapter.get_record(first.smart_table_record_id)
    assert record.fields["职务"] == ("总监" if human_edit else "工程师")
    assert not {"创建时间", "录入时间"}.intersection(record.fields)
    with session_factory() as session:
        assert session.get(Lead, first.lead_id).field_values["录入时间"] == first_time
