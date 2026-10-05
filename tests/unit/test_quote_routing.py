"""企业微信 quote routing 的确定性匹配、归属和恢复回归测试。"""

from __future__ import annotations

import json

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ai.gateway import AIGateway
from app.ai.provider import MockLLMProvider
from app.leads.models import (
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    SalesLeadContext,
    SmartTableSync,
)
from app.leads.quote_routing import resolve_message_quote
from app.leads.service import FirstTextLeadWorkspaceService, LeadProcessingStatus
from app.messaging.models import (
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    MessageQuoteResolution,
    OutboxEvent,
    SalesAuthorization,
    utc_now,
)
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema


def _session_factory() -> sessionmaker[Session]:
    """提供 quote routing 使用的隔离 SQLAlchemy 内存数据库。"""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(engine)


def _persist_message(
    factory: sessionmaker[Session],
    message_id: str,
    sales_user_id: str,
    sequence: int,
    text: str,
    *,
    chat_type: str | None = "single",
    chat_id: str | None = None,
    quote_text: str | None = None,
) -> int:
    """写入一条可供 Worker 消费的消息和 Outbox 事件。"""
    with factory.begin() as session:
        if session.get(SalesAuthorization, sales_user_id) is None:
            session.add(SalesAuthorization(wecom_user_id=sales_user_id, is_authorized=True))
        body: dict[str, object] = {"msgid": message_id, "chattype": chat_type, "msgtype": "text"}
        if chat_id is not None:
            body["chatid"] = chat_id
        if quote_text is not None:
            body["quote"] = {"msgtype": "text", "text": {"content": quote_text}}
        body["text"] = {"content": text}
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=sequence,
                raw_payload={"body": body},
                normalized_text=text,
                chat_id=chat_id,
                chat_type=chat_type,
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


def _adapter() -> MockSmartTableAdapter:
    """返回不连接真实企业微信的智能表格 mock。"""
    return MockSmartTableAdapter(schema=build_required_smart_table_schema())


def test_adapter_preserves_chat_scope_and_text_quote() -> None:
    """验证 adapter 读取 chattype、可选 chatid 和 quote.text.content。"""
    from app.messaging.service import MessageIntakeService
    from app.wecom_bot.adapter import WecomTextMessageAdapter

    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine)
    try:
        frame = {
            "body": {
                "msgid": "reply",
                "from": {"userid": "sales-1"},
                "msgtype": "text",
                "chattype": "single",
                "quote": {"msgtype": "text", "text": {"content": "预算30万"}},
                "text": {"content": "补充需求"},
            }
        }
        result = WecomTextMessageAdapter(MessageIntakeService(factory)).receive_text_frame(frame)
        assert result is not None and result.accepted is True
        with factory() as session:
            message = session.get(IncomingMessage, "reply")
            assert message is not None
            assert message.chat_type == "single"
            assert message.chat_id is None
            assert message.raw_payload["body"]["quote"]["text"]["content"] == "预算30万"
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_no_quote_keeps_old_message_intake_behavior() -> None:
    """验证没有 quote 时不产生 MessageQuoteResolution。"""
    factory = _session_factory()
    event_id = _persist_message(factory, "plain", "sales-1", 1, "客户：甲公司")
    result = FirstTextLeadWorkspaceService(factory, _adapter()).consume(event_id)
    assert result.status is LeadProcessingStatus.CREATED
    with factory() as session:
        assert session.scalars(select(MessageQuoteResolution)).all() == []


def test_exact_match_respects_sequence_sales_chat_and_null_chat_scope() -> None:
    """验证 matcher 的四个 scope 条件和 NULL chatid 弱 scope。"""
    factory = _session_factory()
    _persist_message(factory, "source", "sales-1", 10, "预算30万", chat_type="single")
    current_id = _persist_message(
        factory,
        "current",
        "sales-1",
        15,
        "补充",
        chat_type="single",
        quote_text="预算30万",
    )
    with factory.begin() as session:
        current = session.get(IncomingMessage, "current")
        assert current is not None
        result = resolve_message_quote(session, current)
        assert result is not None
        assert result.resolution.resolution_status == "resolved"
        assert result.resolution.candidate_count == 1
        assert result.resolution.quoted_source_message_id == "source"
    assert current_id == 2


def test_quote_candidate_must_be_older_same_sales_and_same_chat_type() -> None:
    """验证 newer、other-sales 和 other-chat-type 消息均不成为候选。"""
    factory = _session_factory()
    _persist_message(factory, "newer", "sales-1", 20, "相同正文", chat_type="single")
    _persist_message(factory, "other-sales", "sales-2", 1, "相同正文", chat_type="single")
    _persist_message(factory, "other-chat", "sales-1", 2, "相同正文", chat_type="group")
    _persist_message(factory, "current", "sales-1", 15, "回复", quote_text="相同正文")
    with factory.begin() as session:
        current = session.get(IncomingMessage, "current")
        assert current is not None
        result = resolve_message_quote(session, current)
        assert result is not None
        assert result.resolution.resolution_status == "not_found"
        assert result.resolution.candidate_count == 0


def test_quote_chat_id_requires_exact_match_when_current_chat_id_exists() -> None:
    """验证当前消息存在 chatid 时不同 chatid 或缺失 chatid 都不匹配。"""
    factory = _session_factory()
    _persist_message(factory, "same", "sales-1", 1, "预算30万", chat_id="chat-a")
    _persist_message(factory, "different", "sales-1", 2, "预算30万", chat_id="chat-b")
    _persist_message(
        factory, "current", "sales-1", 3, "回复", chat_id="chat-a", quote_text="预算30万"
    )
    with factory.begin() as session:
        current = session.get(IncomingMessage, "current")
        assert current is not None
        result = resolve_message_quote(session, current)
        assert result is not None
        assert result.resolution.candidate_count == 1
        assert result.resolution.quoted_source_message_id == "same"


def test_duplicate_exact_quote_is_ambiguous_and_never_latest() -> None:
    """验证重复正文返回两个候选，不默认选最新或 sequence 最近的一条。"""
    factory = _session_factory()
    _persist_message(factory, "first", "sales-1", 1, "预算30万")
    _persist_message(factory, "second", "sales-1", 2, "预算30万")
    _persist_message(factory, "current", "sales-1", 3, "回复", quote_text="预算30万")
    with factory.begin() as session:
        current = session.get(IncomingMessage, "current")
        assert current is not None
        result = resolve_message_quote(session, current)
        assert result is not None
        assert result.resolution.resolution_status == "ambiguous"
        assert result.resolution.candidate_count == 2
        assert result.resolution.quoted_source_message_id is None


def test_duplicate_exact_quote_fails_closed_in_full_routing() -> None:
    """验证重复正文在完整 Worker 路由中也不会选择任一 Lead。"""
    factory = _session_factory()
    first_id = _persist_message(factory, "first", "sales-1", 1, "客户：甲公司")
    _persist_message(factory, "second", "sales-1", 2, "客户：甲公司")
    _persist_message(
        factory, "current", "sales-1", 3, "预算30万", quote_text="客户：甲公司"
    )
    service = FirstTextLeadWorkspaceService(factory, _adapter())
    service.consume(first_id)
    with factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(LeadMessageResolution.message_id == "current")
        )
        quote = session.scalar(select(MessageQuoteResolution))
    # 首条消息完成后会按既有顺序递归消费后续事件；这里断言当前事件已经 fail closed。
    assert quote is not None and quote.resolution_status == "ambiguous"
    assert resolution is not None and resolution.lead_id is None


def test_quote_not_found_fails_closed_without_active_context_fallback() -> None:
    """验证 quote 未找到时当前消息不会偷偷继承 active context。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 1, "客户：甲公司")
    service = FirstTextLeadWorkspaceService(factory, _adapter())
    source = service.consume(source_id)
    current_id = _persist_message(factory, "current", "sales-1", 2, "预算30万", quote_text="不存在")
    result = service.consume(current_id)
    assert result.status is LeadProcessingStatus.QUOTE_UNRESOLVED
    assert result.lead_id is None
    with factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(LeadMessageResolution.message_id == "current")
        )
        assert resolution is not None
        assert resolution.status == "unassigned"
        assert resolution.lead_id is None
        quote = session.scalar(select(MessageQuoteResolution))
        assert quote is not None and quote.resolution_status == "not_found"
    assert source.lead_id is not None


def test_quote_waits_when_source_has_no_resolution_and_outbox_is_pending() -> None:
    """验证 source 尚未形成归属事实时只等待，不创建 Lead 或待归属结论。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 10, "客户：甲公司")
    current_id = _persist_message(
        factory,
        "current",
        "sales-1",
        15,
        "预算30万",
        quote_text="客户：甲公司",
    )
    result = FirstTextLeadWorkspaceService(factory, _adapter()).consume(current_id)
    assert result.status is LeadProcessingStatus.WAITING_FOR_PREVIOUS
    with factory() as session:
        source_event = session.get(OutboxEvent, source_id)
        current_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "current")
        )
        assert source_event is not None and source_event.status == "pending"
        assert current_event is not None and current_event.status == "pending"
        assert session.scalars(select(Lead)).all() == []
        assert session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "current"
            )
        ) is None
        quote = session.scalar(select(MessageQuoteResolution))
        assert quote is not None and quote.resolution_status == "resolved"


def test_claimed_reply_releases_worker_lease_when_quote_source_is_pending() -> None:
    """验证真实 Worker claim 等待 source 后释放 processing lease 并保留 quote 关系。"""
    factory = _session_factory()
    _persist_message(factory, "source", "sales-1", 10, "客户：甲公司")
    current_id = _persist_message(
        factory, "current", "sales-1", 15, "预算30万", quote_text="客户：甲公司"
    )
    with factory.begin() as session:
        current_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "current")
        )
        assert current_event is not None
        current_event.status = "processing"
        current_event.processing_started_at = utc_now()

    result = FirstTextLeadWorkspaceService(factory, _adapter()).consume(
        current_id, claimed_for_processing=True
    )

    assert result.status is LeadProcessingStatus.WAITING_FOR_PREVIOUS
    with factory() as session:
        current_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "current")
        )
        quote = session.scalar(select(MessageQuoteResolution))
        assert current_event is not None
        assert current_event.status == "pending"
        assert current_event.processing_started_at is None
        assert session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "current"
            )
        ) is None
        assert session.scalars(select(Lead)).all() == []
        assert quote is not None
        assert quote.resolution_status == "resolved"
        assert quote.quoted_source_message_id == "source"


def test_quote_waits_when_source_resolution_is_processing_without_lead() -> None:
    """验证 processing 且 lead_id 为空时不能伪装成 unassigned recovery。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 10, "客户：甲公司")
    current_id = _persist_message(
        factory,
        "current",
        "sales-1",
        15,
        "预算30万",
        quote_text="客户：甲公司",
    )
    with factory.begin() as session:
        source_event = session.get(OutboxEvent, source_id)
        assert source_event is not None
        source_event.status = "processing"
        session.add(
            LeadMessageResolution(
                message_id="source", segment_index=0, status="processing"
            )
        )
    result = FirstTextLeadWorkspaceService(factory, _adapter()).consume(current_id)
    assert result.status is LeadProcessingStatus.WAITING_FOR_PREVIOUS
    with factory() as session:
        assert session.scalars(select(Lead)).all() == []
        current_resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "current"
            )
        )
        assert current_resolution is None


def test_claimed_reply_waits_then_claims_again_and_recovers_explicit_unassigned_source() -> None:
    """验证真实 Worker 两次 claim 间 source 变为 unassigned 后只 recovery 一次。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 10, "客户：甲公司")
    current_id = _persist_message(
        factory,
        "current",
        "sales-1",
        15,
        "预算30万",
        quote_text="客户：甲公司",
    )
    service = FirstTextLeadWorkspaceService(factory, _adapter())
    with factory.begin() as session:
        current_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "current")
        )
        assert current_event is not None
        current_event.status = "processing"
        current_event.processing_started_at = utc_now()
    first = service.consume(current_id, claimed_for_processing=True)
    assert first.status is LeadProcessingStatus.WAITING_FOR_PREVIOUS
    with factory.begin() as session:
        source_event = session.get(OutboxEvent, source_id)
        assert source_event is not None
        source_event.status = "succeeded"
        session.add(
            LeadMessageResolution(
                message_id="source", segment_index=0, status="unassigned"
            )
        )
        current_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "current")
        )
        assert current_event is not None and current_event.status == "pending"
        current_event.status = "processing"
        current_event.processing_started_at = utc_now()
    second = service.consume(current_id, claimed_for_processing=True)
    assert second.lead_id is not None
    with factory() as session:
        assert session.scalar(select(func.count(Lead.id))) == 1
        assert session.scalar(select(func.count(MessageQuoteResolution.id))) == 1
        assert session.scalar(
            select(func.count(BusinessAuditEvent.id)).where(
                BusinessAuditEvent.event_type == "quoted_unassigned_message_recovered"
            )
        ) == 1
        resolutions = session.scalars(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id.in_(["source", "current"])
            )
        ).all()
        context = session.get(SalesLeadContext, "sales-1")
        assert {item.lead_id for item in resolutions} == {second.lead_id}
        assert context is not None and context.last_message_sequence == 15


@pytest.mark.parametrize(
    "source_status",
    [
        pytest.param("succeeded", id="succeeded-without-resolution"),
        pytest.param("ignored", id="ignored"),
        pytest.param("failed_pending_review", id="failed-pending-review"),
    ],
)
def test_terminal_quote_source_without_identity_fails_closed(source_status: str) -> None:
    """验证终态 source 无 Lead identity 时给出 SOURCE_UNROUTABLE，不等待或借用 context。"""
    factory = _session_factory()
    _persist_message(
        factory, "context-source", "sales-1", 1, "客户：上下文公司"
    )
    _persist_message(factory, "source", "sales-1", 10, "客户：目标公司")
    current_id = _persist_message(
        factory, "current", "sales-1", 15, "预算30万", quote_text="客户：目标公司"
    )
    with factory.begin() as session:
        context_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "context-source")
        )
        source_event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "source")
        )
        assert context_event is not None and source_event is not None
        context_event.status = "succeeded"
        source_event.status = source_status
        context_lead = Lead(
            source_message_id="context-source",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            field_values={"线索名称": "上下文公司", "线索来源": "展会"},
        )
        session.add(context_lead)
        session.flush()
        session.add(
            LeadMessageResolution(
                message_id="context-source",
                segment_index=0,
                lead_id=context_lead.id,
                status="assigned",
            )
        )
        session.add(
            SalesLeadContext(
                sales_user_id="sales-1",
                lead_id=context_lead.id,
                last_message_received_at=utc_now(),
                last_message_sequence=1,
            )
        )

    result = FirstTextLeadWorkspaceService(factory, _adapter()).consume(current_id)

    assert result.status is LeadProcessingStatus.QUOTE_UNRESOLVED
    with factory() as session:
        quote = session.scalar(
            select(MessageQuoteResolution).where(
                MessageQuoteResolution.current_message_id == "current"
            )
        )
        current_resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "current"
            )
        )
        assert quote is not None
        assert quote.resolution_status == "conflict"
        assert quote.quoted_source_message_id == "source"
        assert quote.conflict_code == "QUOTE_SOURCE_UNROUTABLE"
        assert current_resolution is not None
        assert current_resolution.status == "unassigned"
        assert current_resolution.lead_id is None
        assert session.scalar(select(func.count(Lead.id))) == 1


def test_quote_recovery_calls_ai_for_each_segment_and_preserves_field_provenance() -> None:
    """验证 deterministic 字段不阻止两个 segment 的 AI 补充，且来源不串到 source。"""
    factory = _session_factory()
    source_id = _persist_message(
        factory,
        "source",
        "sales-1",
        10,
        "客户：QA引用测试公司；联系人：张经理；需要视觉检测",
    )
    current_id = _persist_message(
        factory,
        "current",
        "sales-1",
        15,
        "手机号：13800138000；预算35万；预计年底",
        quote_text="客户：QA引用测试公司；联系人：张经理；需要视觉检测",
    )
    with factory.begin() as session:
        source_event = session.get(OutboxEvent, source_id)
        assert source_event is not None
        source_event.status = "succeeded"
        session.add(
            LeadMessageResolution(
                message_id="source", segment_index=0, status="unassigned"
            )
        )
    responses = [
        json.dumps(
            {
                "intent": "UPDATE_LEAD",
                "customer_reference": {},
                "crm_fields": {},
                "enrichment": {"客户需求/痛点": "需要视觉检测"},
                "confidence_by_field": {},
                "conflicts": [],
                "warnings": [],
            },
            ensure_ascii=False,
        ),
        json.dumps(
            {
                "intent": "UPDATE_LEAD",
                "customer_reference": {},
                "crm_fields": {},
                "enrichment": {"预算": "35万", "特殊要求": "预计年底"},
                "confidence_by_field": {},
                "conflicts": [],
                "warnings": [],
            },
            ensure_ascii=False,
        ),
    ]
    provider = MockLLMProvider(responses)
    service = FirstTextLeadWorkspaceService(
        factory,
        _adapter(),
        ai_gateway=AIGateway(provider),
    )
    result = service.consume(current_id)
    assert result.lead_id is not None
    assert len(provider.requests) == 2
    with factory() as session:
        lead = session.get(Lead, result.lead_id)
        assert lead is not None
        assert lead.enrichment_values == {
            "客户需求/痛点": "需要视觉检测",
            "预算": "35万",
            "特殊要求": "预计年底",
        }
        provenances = session.scalars(
            select(LeadFieldProvenance).where(LeadFieldProvenance.lead_id == lead.id)
        ).all()
        provenance_by_field = {item.field_name: item.source_message_id for item in provenances}
        assert provenance_by_field["线索名称"] == "source"
        assert provenance_by_field["联系人"] == "source"
        assert provenance_by_field["手机"] == "current"


def test_assigned_quote_inherits_same_lead_and_advances_context_to_reply() -> None:
    """验证唯一 assigned source 直接归属同一 Lead，context 使用 reply sequence。"""
    factory = _session_factory()
    service = FirstTextLeadWorkspaceService(factory, _adapter())
    source_id = _persist_message(factory, "source", "sales-1", 10, "客户：甲公司")
    source = service.consume(source_id)
    current_id = _persist_message(
        factory, "current", "sales-1", 15, "预算30万", quote_text="客户：甲公司"
    )
    current = service.consume(current_id)
    assert source.lead_id is not None
    assert current.lead_id == source.lead_id
    with factory() as session:
        resolutions = session.scalars(
            select(LeadMessageResolution)
            .where(LeadMessageResolution.message_id.in_(["source", "current"]))
            .order_by(LeadMessageResolution.message_id)
        ).all()
        context = session.get(SalesLeadContext, "sales-1")
    assert {item.lead_id for item in resolutions} == {source.lead_id}
    assert context is not None and context.last_message_sequence == 15


def test_processing_quote_inherits_local_lead_when_source_event_failed_pending_review() -> None:
    """验证 Outbox 已失败待审但本地 processing resolution 有 Lead 时仍可继承。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 10, "预算30万")
    current_id = _persist_message(factory, "current", "sales-1", 15, "补充", quote_text="预算30万")
    with factory.begin() as session:
        lead = Lead(
            source_message_id="source",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-1",
            field_values={"线索名称": "甲公司", "线索来源": "展会"},
        )
        session.add(lead)
        session.flush()
        session.add(
            LeadMessageResolution(
                message_id="source", segment_index=0, lead_id=lead.id, status="processing"
            )
        )
        session.add(
            SmartTableSync(
                lead_id=lead.id,
                source_message_id="source",
                status="failed_pending_review",
                error_summary="SmartTableProjectionFailure",
            )
        )
        source_event = session.get(OutboxEvent, source_id)
        assert source_event is not None
        source_event.status = "failed_pending_review"
    # source 的本地 Lead identity 处于 processing，当前 reply 仍应能够直接继承。
    del source_id
    result = FirstTextLeadWorkspaceService(factory, _adapter()).consume(current_id)
    assert result.lead_id is not None
    with factory() as session:
        current_resolution = session.scalar(
            select(LeadMessageResolution).where(LeadMessageResolution.message_id == "current")
        )
        assert current_resolution is not None
        assert current_resolution.lead_id == result.lead_id


def test_unassigned_quote_recovery_assigns_source_and_reply_once() -> None:
    """验证 unassigned source 通过双片段恢复创建一条 Lead 并保留恢复审计。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 10, "客户：甲公司")
    current_id = _persist_message(
        factory, "current", "sales-1", 15, "预算30万", quote_text="客户：甲公司"
    )
    with factory.begin() as session:
        source_event = session.get(OutboxEvent, source_id)
        assert source_event is not None
        source_event.status = "succeeded"
        session.add(
            LeadMessageResolution(message_id="source", segment_index=0, status="unassigned")
        )
    service = FirstTextLeadWorkspaceService(factory, _adapter())
    result = service.consume(current_id)
    assert result.lead_id is not None
    with factory() as session:
        resolutions = session.scalars(
            select(LeadMessageResolution)
            .where(LeadMessageResolution.message_id.in_(["source", "current"]))
            .order_by(LeadMessageResolution.message_id)
        ).all()
        audits = session.scalars(
            select(BusinessAuditEvent).where(
                BusinessAuditEvent.event_type == "quoted_unassigned_message_recovered"
            )
        ).all()
        context = session.get(SalesLeadContext, "sales-1")
    assert {item.lead_id for item in resolutions} == {result.lead_id}
    assert {item.status for item in resolutions} == {"assigned"}
    assert len(audits) == 1
    assert audits[0].details["previous_source_status"] == "unassigned"
    assert audits[0].details["new_source_status"] == "assigned"
    assert context is not None and context.last_message_sequence == 15


def test_quote_identity_conflict_fails_closed_without_creating_second_lead() -> None:
    """验证可靠公司 B 与 quote Lead A 冲突时不选 A、不创建 B。"""
    factory = _session_factory()
    service = FirstTextLeadWorkspaceService(factory, _adapter())
    source_id = _persist_message(factory, "source", "sales-1", 1, "客户：甲公司")
    source = service.consume(source_id)
    current_id = _persist_message(
        factory,
        "current",
        "sales-1",
        2,
        "客户：乙公司；预算30万",
        quote_text="客户：甲公司",
    )
    result = service.consume(current_id)
    assert result.status is LeadProcessingStatus.QUOTE_UNRESOLVED
    with factory() as session:
        leads = session.scalars(select(Lead)).all()
        current_resolution = session.scalar(
            select(LeadMessageResolution).where(LeadMessageResolution.message_id == "current")
        )
        quote = session.scalar(select(MessageQuoteResolution))
    assert len(leads) == 1 and leads[0].id == source.lead_id
    assert current_resolution is not None and current_resolution.lead_id is None
    assert quote is not None
    assert quote.conflict_code == "QUOTE_IDENTITY_CONFLICT"
    assert quote.resolution_status == "conflict"


def test_quote_target_ambiguity_from_multiple_source_leads_fails_closed() -> None:
    """验证 source 多个不同 lead_id 时不按 segment 或顺序猜目标。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 1, "客户甲和客户乙")
    current_id = _persist_message(
        factory, "current", "sales-1", 2, "补充", quote_text="客户甲和客户乙"
    )
    with factory.begin() as session:
        source_event = session.get(OutboxEvent, source_id)
        assert source_event is not None
        source_event.status = "succeeded"
        leads = []
        for name in ("甲公司", "乙公司"):
            lead = Lead(
                source_message_id=None,
                original_capturing_sales_user_id="sales-1",
                smart_table_owner_user_id="sales-1",
                field_values={"线索名称": name, "线索来源": "展会"},
            )
            session.add(lead)
            session.flush()
            leads.append(lead)
        session.add_all(
            [
                LeadMessageResolution(
                    message_id="source", segment_index=0, lead_id=leads[0].id, status="assigned"
                ),
                LeadMessageResolution(
                    message_id="source", segment_index=1, lead_id=leads[1].id, status="assigned"
                ),
            ]
        )
    result = FirstTextLeadWorkspaceService(factory, _adapter()).consume(current_id)
    assert result.status is LeadProcessingStatus.QUOTE_UNRESOLVED
    with factory() as session:
        current_resolution = session.scalar(
            select(LeadMessageResolution).where(LeadMessageResolution.message_id == "current")
        )
        quote = session.scalar(select(MessageQuoteResolution))
    assert current_resolution is not None and current_resolution.lead_id is None
    assert quote is not None and quote.conflict_code == "QUOTE_TARGET_AMBIGUOUS"


def test_quote_does_not_bypass_lead_ownership() -> None:
    """验证 source Lead 转交给其他销售后，原销售 quote 仍 fail closed。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 1, "客户：甲公司")
    current_id = _persist_message(
        factory, "current", "sales-1", 2, "补充", quote_text="客户：甲公司"
    )
    with factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-2", is_authorized=True))
        lead = Lead(
            source_message_id="source",
            original_capturing_sales_user_id="sales-1",
            smart_table_owner_user_id="sales-2",
            field_values={"线索名称": "甲公司", "线索来源": "展会"},
        )
        session.add(lead)
        session.flush()
        session.add(
            LeadMessageResolution(
                message_id="source", segment_index=0, lead_id=lead.id, status="assigned"
            )
        )
        source_event = session.get(OutboxEvent, source_id)
        assert source_event is not None
        source_event.status = "succeeded"
    result = FirstTextLeadWorkspaceService(factory, _adapter()).consume(current_id)
    assert result.status is LeadProcessingStatus.QUOTE_UNRESOLVED
    with factory() as session:
        current_resolution = session.scalar(
            select(LeadMessageResolution).where(LeadMessageResolution.message_id == "current")
        )
    assert current_resolution is not None and current_resolution.lead_id is None


def test_ai_hallucinated_company_does_not_create_quote_identity_conflict() -> None:
    """验证当前消息没有服务器公司证据时，AI 虚构公司不改变 quote 目标。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 1, "客户：甲公司")
    service = FirstTextLeadWorkspaceService(factory, _adapter())
    source = service.consume(source_id)
    current_id = _persist_message(
        factory, "current", "sales-1", 2, "预算30万", quote_text="客户：甲公司"
    )
    fake_company_response = json.dumps(
        {
            "intent": "UPDATE_LEAD",
            "customer_reference": {},
            "crm_fields": {"线索名称": "乙公司"},
            "enrichment": {},
            "confidence_by_field": {"线索名称": 0.99},
            "conflicts": [],
            "warnings": [],
        },
        ensure_ascii=False,
    )
    service = FirstTextLeadWorkspaceService(
        factory,
        _adapter(),
        ai_gateway=AIGateway(MockLLMProvider([fake_company_response])),
    )
    result = service.consume(current_id)
    assert source.lead_id is not None
    assert result.lead_id == source.lead_id
    with factory() as session:
        assert session.scalar(select(func.count(Lead.id))) == 1


def test_quote_retry_is_idempotent_for_resolution_recovery_lead_and_audit() -> None:
    """验证重复消费 recovery reply 不重复创建 quote、Lead、归属或审计。"""
    factory = _session_factory()
    source_id = _persist_message(factory, "source", "sales-1", 1, "客户：甲公司")
    current_id = _persist_message(
        factory, "current", "sales-1", 2, "预算30万", quote_text="客户：甲公司"
    )
    with factory.begin() as session:
        event = session.get(OutboxEvent, source_id)
        assert event is not None
        event.status = "succeeded"
        session.add(
            LeadMessageResolution(message_id="source", segment_index=0, status="unassigned")
        )
    service = FirstTextLeadWorkspaceService(factory, _adapter())
    first = service.consume(current_id)
    second = service.consume(current_id)
    assert second.status is LeadProcessingStatus.ALREADY_PROCESSED
    with factory() as session:
        assert session.scalar(select(func.count(MessageQuoteResolution.id))) == 1
        assert session.scalar(select(func.count(Lead.id))) == 1
        assert session.scalar(
            select(func.count(BusinessAuditEvent.id)).where(
                BusinessAuditEvent.event_type == "quoted_unassigned_message_recovered"
            )
        ) == 1
        resolutions = session.scalars(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "current"
            )
        ).all()
    assert first.lead_id is not None
    assert len(resolutions) == 1
