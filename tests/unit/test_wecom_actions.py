"""T18 企业微信确定性卡片动作的契约与幂等测试。"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Generator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import get_settings
from app.crm.adapter import CRMSearchResult
from app.crm.commands import (
    is_explicit_submission_request,
    parse_company_submission_request,
    parse_crm_submission_command,
)
from app.crm.mock import MockCRMAdapter
from app.crm.sop import SopCRMError
from app.leads.discard import LeadDiscardService
from app.leads.models import (
    CrmSyncRecord,
    Lead,
    LeadFieldProvenance,
    LeadMessageResolution,
    MessageReassignmentAudit,
    UserConfirmationEvent,
)
from app.leads.service import LeadReassignmentService
from app.messaging.models import (
    Base,
    IncomingMessage,
    NotificationRecord,
    SalesAuthorization,
    WecomAction,
    WecomActionOutbox,
    WecomCallbackDelivery,
    utc_now,
)
from app.notifications.outbound import WecomOutboundNotificationSender
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema
from app.wecom_bot.actions import (
    CARD_EVENT_KEY_CRM_BATCH_SUBMISSION,
    CARD_EVENT_KEY_CRM_COMPANY_CONFIRM,
    CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE,
    CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
    CARD_EVENT_KEY_DISCARD_CONFIRM,
    CARD_EVENT_KEY_REASSIGN_CONFIRM,
    CallbackParseError,
    DeterministicWecomActionExecutor,
    InvalidActionTransition,
    StaleActionClaim,
    TemplateCardCallbackParser,
    WecomActionService,
    WecomActionStatus,
    _transition_action,
    build_action_card,
    build_batch_submission_markdown,
    build_preview_markdown,
    parse_deterministic_action_command,
)
from app.wecom_bot.callback import WecomTemplateCardCallbackHandler


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供 T18 单元测试使用的隔离 SQLAlchemy 会话工厂。"""

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


def _fixture_frame() -> dict[str, object]:
    """读取脱敏真实契约 fixture，避免测试自行发明未验证字段。"""

    fixture_path = Path(__file__).parents[1] / "fixtures" / "wecom_template_card_event.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _authorize(session_factory: sessionmaker[Session], user_id: str = "sales-a") -> None:
    """在测试数据库中创建可点击卡片的销售授权事实。"""

    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id=user_id, is_authorized=True, is_active=True))


def _service(session_factory: sessionmaker[Session]) -> WecomActionService:
    """创建启用卡片回调能力的动作服务。"""

    return WecomActionService(session_factory, card_callback_ready=True)


def _frame_for_action(
    action: WecomAction, *, msgid: str, actor: str = "sales-a"
) -> dict[str, object]:
    """根据服务端 action 生成仅用于测试 transport 的 callback 帧。"""

    frame = _fixture_frame()
    frame["body"]["msgid"] = msgid
    frame["body"]["from"]["userid"] = actor
    frame["body"]["event"]["template_card_event"]["task_id"] = action.task_id
    frame["body"]["event"]["template_card_event"]["event_key"] = action.expected_action_key
    return frame


def _seed_confirmable_lead(
    session_factory: sessionmaker[Session], adapter: MockSmartTableAdapter
) -> str:
    """创建带 CRM 必填 AI待确认 字段的可审核线索。"""

    record = adapter.create_record(
        {
            "线索名称": "卡片公司",
            "业务线": "协作机器人",
            "手机": "13800000000",
            "负责人": "sales-a",
            "AI待确认": ["业务线"],
        },
        actor=SmartTableActor.ROBOT,
    )
    with session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-action",
                sales_user_id="sales-a",
                sequence=1,
                raw_payload={},
            )
        )
        lead = Lead(
            source_message_id="message-action",
            original_capturing_sales_user_id="sales-a",
            smart_table_owner_user_id="sales-a",
            smart_table_record_id=record.record_id,
            lifecycle_state="pending_create",
            field_values={
                "线索名称": "卡片公司",
                "业务线": "协作机器人",
                "手机": "13800000000",
            },
        )
        session.add(lead)
        session.flush()
        session.add(
            LeadFieldProvenance(
                lead_id=lead.id,
                source_message_id="message-action",
                field_name="业务线",
                value="协作机器人",
                last_ai_synced_value="协作机器人",
            )
        )
        return lead.id


def _seed_batch_leads(session_factory: sessionmaker[Session], count: int = 3) -> tuple[str, ...]:
    """创建供批量候选回调测试使用的同销售待提交线索。"""

    with session_factory.begin() as session:
        if session.get(SalesAuthorization, "sales-a") is None:
            session.add(
                SalesAuthorization(wecom_user_id="sales-a", is_authorized=True, is_active=True)
            )
        leads: list[Lead] = []
        for index in range(count):
            message_id = f"batch-message-{index}"
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id="sales-a",
                    sequence=index + 1,
                    raw_payload={},
                )
            )
            leads.append(
                Lead(
                    id=f"batch-lead-{index}",
                    source_message_id=message_id,
                    original_capturing_sales_user_id="sales-a",
                    smart_table_owner_user_id="sales-a",
                    lifecycle_state="pending_create",
                    field_values={"线索名称": "同名公司", "提交状态": "未提交"},
                )
            )
        session.add_all(leads)
        return tuple(lead.id for lead in leads)


def _seed_today_submission_leads(
    session_factory: sessionmaker[Session],
    adapter: MockSmartTableAdapter,
    count: int = 2,
) -> tuple[str, ...]:
    """创建具备 CRM 必填快照和可信负责人显示名的 TODAY 候选。"""

    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="sales-a",
                crm_user_id="crm-sales-a",
                is_authorized=True,
                is_active=True,
            )
        )
        lead_ids: list[str] = []
        for index in range(count):
            message_id = f"today-message-{index}"
            record = adapter.create_record(
                {
                    "负责人": "sales-a",
                    "创建人": "sales-a",
                    "线索名称": f"TODAY测试公司-{index}",
                    "业务线": "协作机器人",
                    "线索来源": "展会",
                    "联系人": "王工",
                    "职务": "项目经理",
                    "沟通方式": "微信",
                    "手机": f"1380000000{index}",
                    "客户行业": "机械加工",
                    "备注": "已确认机器人项目需求，正在评估方案和预算，销售需继续跟进。",
                    "提交状态": "未提交",
                },
                actor=SmartTableActor.ROBOT,
                member_names={"负责人": "测试销售"},
            )
            session.add(
                IncomingMessage(
                    message_id=message_id,
                    sales_user_id="sales-a",
                    sequence=index + 1,
                    raw_payload={},
                )
            )
            lead_id = f"today-lead-{index}"
            session.add(
                Lead(
                    id=lead_id,
                    source_message_id=message_id,
                    original_capturing_sales_user_id="sales-a",
                    smart_table_owner_user_id="sales-a",
                    smart_table_record_id=record.record_id,
                    lifecycle_state="pending_create",
                    standard_company_name=f"TODAY测试公司-{index}",
                    field_values=dict(record.fields),
                )
            )
            lead_ids.append(lead_id)
        return tuple(lead_ids)


def _batch_frame_for_action(
    action: WecomAction, selected: tuple[str, ...], *, msgid: str
) -> dict[str, object]:
    """构造带服务端冻结候选选择项的批量 callback 帧。"""

    frame = _frame_for_action(action, msgid=msgid)
    card_event = frame["body"]["event"]["template_card_event"]  # type: ignore[index]
    card_event["card_type"] = "vote_interaction"  # type: ignore[index]
    card_event["selected_items"] = {  # type: ignore[index]
        "selected_item": [
            {
                "question_key": "crm_submission_candidates",
                "option_ids": {"option_id": list(selected)},
            }
        ]
    }
    return frame


def _issue_today_action(
    service: WecomActionService, lead_ids: tuple[str, ...], *, message_id: str
) -> WecomAction:
    """发行由服务端冻结目标的 TODAY 候选动作。"""

    return service.issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id=message_id,
        command_text="提交今天的线索",
        candidates=tuple(
            {"lead_id": lead_id, "company_name": f"TODAY测试公司-{index}"}
            for index, lead_id in enumerate(lead_ids)
        ),
    )


def _seed_reassignment_case(session_factory: sessionmaker[Session]) -> str:
    """创建当前销售可将消息分段归属到目标线索的最小事实集。"""

    with session_factory.begin() as session:
        session.add_all(
            [
                IncomingMessage(
                    message_id="message-source",
                    sales_user_id="sales-a",
                    sequence=1,
                    raw_payload={},
                ),
                IncomingMessage(
                    message_id="message-target",
                    sales_user_id="sales-a",
                    sequence=2,
                    raw_payload={},
                ),
            ]
        )
        source = Lead(
            id="lead-source",
            source_message_id="message-source",
            original_capturing_sales_user_id="sales-a",
            smart_table_owner_user_id="sales-a",
            lifecycle_state="temporary",
            field_values={"线索名称": "来源公司"},
        )
        target = Lead(
            id="lead-target",
            source_message_id="message-target",
            original_capturing_sales_user_id="sales-a",
            smart_table_owner_user_id="sales-a",
            lifecycle_state="temporary",
            field_values={"线索名称": "目标公司"},
        )
        session.add_all([source, target])
        session.flush()
        session.add(
            LeadMessageResolution(
                message_id="message-source",
                segment_index=0,
                lead_id=source.id,
                status="assigned",
            )
        )
    return "lead-target"


def test_real_template_card_fixture_uses_only_verified_contract() -> None:
    """验证正式 fixture 的解析路径只依赖已冻结的字段。"""

    parsed = TemplateCardCallbackParser.parse(_fixture_frame())

    assert parsed.actor_user_id == "sales-a"
    assert parsed.event_key == CARD_EVENT_KEY_CRM_FIELD_CONFIRM
    assert parsed.task_id == "task-fixture-001"


def test_duplicate_card_supports_batch_selection_and_two_decisions() -> None:
    """验证重复线索卡片包含多选项及继续、停止两个按钮。"""
    card = build_action_card(
        task_id="task-duplicate",
        event_key=CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE,
        title="CRM 重复线索确认",
        description="当前系统有线索A的信息，请问是进行覆盖还是停止提交",
        duplicate_leads=[
            {"lead_id": "lead-a", "company_name": "线索A", "crm_lead_id": "crm-a"},
            {"lead_id": "lead-b", "company_name": "线索B", "crm_lead_id": "crm-b"},
        ],
    )
    assert card["checkbox"]["mode"] == 1  # type: ignore[index]
    assert card["submit_button"]["text"] == "继续提交"  # type: ignore[index]
    assert card["action_menu"]["action_list"][0]["text"] == "停止提交"  # type: ignore[index]

    frame = _fixture_frame()
    card_event = frame["body"]["event"]["template_card_event"]
    card_event["card_type"] = "vote_interaction"
    card_event["event_key"] = CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE
    card_event["task_id"] = "task-duplicate"
    card_event["selected_items"] = {
        "selected_item": [
            {"question_key": "crm_duplicate_leads", "option_ids": {"option_id": ["lead-a"]}}
        ]
    }
    parsed = TemplateCardCallbackParser.parse(frame)
    assert parsed.selected_option_ids == ("lead-a",)
    assert parsed.provider_msgid == "provider-msg-001"
    assert parsed.req_id == "request-001"


def test_malformed_callback_fields_fail_closed() -> None:
    """验证缺失或非法的 callback 标识不会进入业务处理。"""

    frame = _fixture_frame()
    body = frame["body"]
    assert isinstance(body, dict)
    body["msgid"] = "bad\nmsgid"

    with pytest.raises(CallbackParseError):
        TemplateCardCallbackParser.parse(frame)


@pytest.mark.parametrize(
    "card_type",
    (None, "unknown_card"),
)
def test_missing_or_unknown_card_type_fails_closed(card_type: str | None) -> None:
    """验证 callback 只接受真实已验证的 button_interaction 卡片类型。"""

    frame = _fixture_frame()
    card_event = frame["body"]["event"]["template_card_event"]
    if card_type is None:
        del card_event["card_type"]
    else:
        card_event["card_type"] = card_type
    with pytest.raises(CallbackParseError):
        TemplateCardCallbackParser.parse(frame)


def test_t18_machine_commands_are_exact_and_not_natural_language() -> None:
    """验证废弃/重归属入口只接受固定 machine command。"""

    discard = parse_deterministic_action_command("t18.discard:lead-1")
    reassign = parse_deterministic_action_command("t18.reassign:message-1:0:lead-2")
    assert discard is not None and discard.target_id == "lead-1"
    assert reassign is not None and reassign.message_id == "message-1"
    assert parse_deterministic_action_command("请帮我废弃 lead-1") is None
    assert parse_deterministic_action_command("t18.discard:lead-1:extra") is None


def test_action_state_transition_guard_rejects_terminal_rewrites() -> None:
    """验证集中状态图允许处理中的拒绝，并阻止终态复活或回退。"""

    processing = WecomAction(status=WecomActionStatus.PROCESSING.value)
    _transition_action(processing, WecomActionStatus.DENIED.value)
    assert processing.status == WecomActionStatus.DENIED.value

    for current, target in (
        (WecomActionStatus.DENIED.value, WecomActionStatus.SUCCEEDED.value),
        (WecomActionStatus.SUCCEEDED.value, WecomActionStatus.FAILED.value),
        (WecomActionStatus.EXPIRED.value, WecomActionStatus.PROCESSING.value),
    ):
        action = WecomAction(status=current)
        with pytest.raises(InvalidActionTransition):
            _transition_action(action, target)
        assert action.status == current


def test_action_context_does_not_persist_field_value_or_contact_pii(
    session_factory: sessionmaker[Session],
) -> None:
    """验证动作 context 只保留字段确认值的不可逆摘要。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_field_confirmation_action(
        actor_user_id="sales-a",
        lead_id="lead-pii",
        field_names=("邮箱",),
        command_text="提交今天的线索",
        request_message_id="message-pii",
        field_values={"邮箱": "customer@example.com"},
    )

    stored_values = action.context["field_values"]
    assert isinstance(stored_values, dict)
    assert "customer@example.com" not in str(action.context)
    assert str(stored_values["邮箱"]).startswith("sha256:")


@pytest.mark.parametrize(
    "field_path",
    ("req_id", "task_id", "event_key"),
)
def test_malformed_request_task_or_event_identifier_fails_closed(field_path: str) -> None:
    """验证 req_id、task_id 和 event_key 的换行输入不能进入 callback 解析。"""

    frame = _fixture_frame()
    if field_path == "req_id":
        frame["headers"]["req_id"] = "req\nlog-injection"
    else:
        frame["body"]["event"]["template_card_event"][field_path] = "bad\nidentifier"

    with pytest.raises(CallbackParseError):
        TemplateCardCallbackParser.parse(frame)


def test_unknown_event_key_has_no_business_side_effect(
    session_factory: sessionmaker[Session],
) -> None:
    """验证未知 event_key 被拒绝且不创建 action execution。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_action(
        actor_user_id="sales-a",
        action_type="crm_field_confirmation",
        target_type="lead",
        target_id="lead-1",
        expected_action_key=CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
        context={"field_names": ["业务线"]},
        title="确认字段",
        description="请确认字段",
    )
    frame = _fixture_frame()
    event = frame["body"]["event"]
    assert isinstance(event, dict)
    card_event = event["template_card_event"]
    assert isinstance(card_event, dict)
    card_event["event_key"] = "unknown.event.key"
    card_event["task_id"] = action.task_id
    frame["body"]["from"]["userid"] = "sales-a"

    result = _service(session_factory).claim_callback(frame)

    assert result.code == "unknown_event_key"
    assert result.should_update_card is False
    with session_factory() as session:
        stored = session.get(WecomAction, action.id)
        assert stored is not None and stored.status == "pending"
        assert session.scalars(select(WecomActionOutbox)).all() == []


def test_unknown_task_and_actor_mismatch_have_no_domain_execution(
    session_factory: sessionmaker[Session],
) -> None:
    """验证未知 task 与跨用户点击均 fail closed。"""

    _authorize(session_factory, "sales-a")
    _authorize(session_factory, "sales-b")
    frame = _fixture_frame()
    card_event = frame["body"]["event"]["template_card_event"]
    card_event["task_id"] = "task-does-not-exist"
    unknown_task = _service(session_factory).claim_callback(frame)
    assert unknown_task.code == "unknown_task"
    assert unknown_task.should_update_card is False

    action = _service(session_factory).issue_action(
        actor_user_id="sales-a",
        action_type="lead_discard_confirmation",
        target_type="lead",
        target_id="lead-1",
        expected_action_key=CARD_EVENT_KEY_DISCARD_CONFIRM,
        context={"reason": "用户明确废弃"},
        title="确认废弃",
        description="请确认",
    )
    card_event["task_id"] = action.task_id
    card_event["event_key"] = CARD_EVENT_KEY_DISCARD_CONFIRM
    frame["body"]["from"]["userid"] = "sales-b"
    frame["body"]["msgid"] = "provider-msg-002"

    result = _service(session_factory).claim_callback(frame)

    assert result.code == "actor_mismatch"
    with session_factory() as session:
        stored = session.get(WecomAction, action.id)
        assert stored is not None and stored.status == "denied"
        assert session.scalars(select(WecomActionOutbox)).all() == []


def test_field_confirmation_owner_is_checked_before_callback_claim(
    session_factory: sessionmaker[Session],
) -> None:
    """验证字段确认卡在 callback claim 前发现负责人变化时不创建执行 outbox。"""

    _authorize(session_factory, "sales-a")
    _authorize(session_factory, "sales-b")
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _seed_confirmable_lead(session_factory, adapter)
    action = _service(session_factory).issue_field_confirmation_action(
        actor_user_id="sales-a",
        lead_id=lead_id,
        field_names=("业务线",),
        command_text="提交今天的线索",
        request_message_id="message-owner-precheck",
    )
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_id)
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-b"

    result = _service(session_factory).claim_callback(
        _frame_for_action(action, msgid="provider-owner-mismatch", actor="sales-a")
    )

    assert result.code == "owner_mismatch"
    with session_factory() as session:
        stored = session.get(WecomAction, action.id)
        assert stored is not None and stored.status == "denied"
        assert session.scalars(select(WecomActionOutbox)).all() == []


def test_disabled_callback_capability_does_not_claim_existing_action(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 readiness 关闭时旧卡 callback 也不会创建执行 Outbox。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-disabled", reason="能力关闭测试"
    )
    result = WecomActionService(session_factory, card_callback_ready=False).claim_callback(
        _frame_for_action(action, msgid="provider-msg-disabled")
    )

    assert result.code == "card_callback_unavailable"
    assert result.should_update_card is False
    with session_factory() as session:
        stored = session.get(WecomAction, action.id)
        assert stored is not None and stored.status == "pending"
        assert session.scalars(select(WecomActionOutbox)).all() == []


def test_expired_action_does_not_create_execution(session_factory: sessionmaker[Session]) -> None:
    """验证过期卡片不会被重新激活。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_action(
        actor_user_id="sales-a",
        action_type="lead_reassignment_confirmation",
        target_type="lead",
        target_id="lead-1",
        expected_action_key=CARD_EVENT_KEY_REASSIGN_CONFIRM,
        context={"message_id": "message-1", "segment_index": 0, "reason": "修正归属"},
        title="确认归属",
        description="请确认",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    frame = _fixture_frame()
    frame["body"]["from"]["userid"] = "sales-a"
    frame["body"]["event"]["template_card_event"]["task_id"] = action.task_id
    frame["body"]["event"]["template_card_event"]["event_key"] = CARD_EVENT_KEY_REASSIGN_CONFIRM

    result = _service(session_factory).claim_callback(frame)

    assert result.code == "expired"
    with session_factory() as session:
        assert session.scalars(select(WecomActionOutbox)).all() == []


def test_submission_command_aliases_normalize_to_confirmed_intents() -> None:
    """验证常见自然说法归一化到既有确认卡命令，不绕过服务端选择。"""

    assert parse_crm_submission_command("提交今天的线索") == "提交今天的线索"
    assert parse_crm_submission_command("提交我所有线索") == "提交我所有线索"
    assert parse_crm_submission_command("帮我提交放弃提交的线索") == "帮我提交放弃提交的线索"
    assert parse_crm_submission_command("提交我的更新") == "提交我的更新"
    assert parse_crm_submission_command("提交我所有的线索") == "提交我所有线索"
    assert parse_crm_submission_command("请提交我所有线索") is None
    assert parse_crm_submission_command("帮我提交今天的线索") == "提交今天的线索"
    assert parse_crm_submission_command("提交今天的线索。") == "提交今天的线索"
    assert parse_crm_submission_command("请提交我的更新") is None
    assert parse_crm_submission_command("提交我所有线索？") is None
    assert is_explicit_submission_request("请帮我提交今天的线索") is True
    assert is_explicit_submission_request("请提交所有线索") is True
    assert is_explicit_submission_request("帮我提交遨博这条线索") is True
    assert is_explicit_submission_request("今天的线索提交了吗？") is False
    assert is_explicit_submission_request("这个客户之前提交过吗？") is False
    assert is_explicit_submission_request("不要提交今天的线索") is False
    assert is_explicit_submission_request("先别提交我的更新") is False
    assert is_explicit_submission_request("暂时不提交所有线索") is False
    assert is_explicit_submission_request("我不想提交这条线索") is False
    assert parse_crm_submission_command("不要提交今天的线索") is None


def test_company_submission_request_is_strict_and_returns_exact_company_name() -> None:
    """验证公司提交句式只提取明确公司名，不放宽为自然语言意图。"""

    text = "请帮我提交上海世界纵横智能科技有限公司这条线索"
    assert parse_company_submission_request(text) == "上海世界纵横智能科技有限公司"
    assert parse_crm_submission_command(text) == text
    assert parse_company_submission_request("帮我提交上海世界纵横智能科技有限公司") is None
    assert parse_company_submission_request("请帮我提交上海世界纵横智能科技有限公司。") is None


def test_incomplete_retry_routing_keeps_abandoned_priority_and_guards() -> None:
    """验证待完善重提不抢占放弃/更新命令，疑问和否定不授权提交。"""
    assert parse_crm_submission_command("重新提交") is None
    assert parse_crm_submission_command("重新提交放弃提交的线索") == "帮我提交放弃提交的线索"
    assert parse_crm_submission_command("提交我的更新") == "提交我的更新"
    assert not is_explicit_submission_request("这些待完善线索可以重新提交吗？")
    assert not is_explicit_submission_request("先不要重新提交")


def test_company_submission_confirmation_card_contains_preview_fields(
    session_factory: sessionmaker[Session],
) -> None:
    """验证单条公司确认动作把全字段放入卡片，而不是放入客户端目标参数。"""

    _authorize(session_factory)
    service = _service(session_factory)
    action = service.issue_company_submission_confirmation_action(
        actor_user_id="sales-a",
        lead_id="lead-preview",
        request_message_id="message-preview",
        company_name="预览公司",
        field_values={"线索名称": "预览公司", "备注": "销售确认"},
    )

    with session_factory() as session:
        notices = session.scalars(select(NotificationRecord)).all()
    assert action.action_type == "crm_company_submission_confirmation"
    assert len(notices) == 2
    card_notice = next(
        notice for notice in notices if notice.notification_type == "wecom_action_card"
    )
    preview_notice = next(
        notice for notice in notices if notice.notification_type == "wecom_action_preview"
    )
    assert len(card_notice.payload["template_card"]["horizontal_content_list"]) == 3
    assert "备注" in preview_notice.payload["markdown"]["content"]


def test_preview_card_and_markdown_fit_wecom_display_limits() -> None:
    """验证单条卡片和 Markdown 只显示固定十字段，三组且长值截断。"""

    names = (
        "业务线", "线索名称", "线索来源", "联系人", "职务", "沟通方式",
        "手机", "备注", "客户行业", "提交状态",
    )
    preview = dict.fromkeys(names, "已填写")
    preview.update({"线索名称": "长名称" * 1000, "备注": "长备注" * 1000, "职务": "  "})
    extras = ("电话", "邮箱", "客户级别", "工艺", "下次联系时间", "AI待确认", "负责人")
    preview.update(dict.fromkeys(extras, "不应展示"))
    original = dict(preview)
    card = build_action_card(
        task_id="task-preview-limit",
        event_key="crm.company_submission.confirm",
        title="确认提交线索",
        description="请核对全部字段",
        preview_fields=preview,
    )
    rows = card["horizontal_content_list"]
    assert isinstance(rows, list)
    assert len(rows) == 3
    rendered = "".join(str(row) for row in rows)
    details = build_preview_markdown(preview)
    field_lines = details.splitlines()[1:4]
    assert [name for line in field_lines for name in names if name + "：" in line] == list(names)
    assert "职务：**未填写**" in details
    assert "…" in details and len(details.encode("utf-8")) < 4096
    assert all(name not in details and name not in rendered for name in extras)
    assert all(name + "：" in rendered for name in names)
    assert preview == original


def test_company_submission_candidate_card_uses_single_selection() -> None:
    """验证同名候选卡使用单选并固定服务端确认 action key。"""

    card = build_action_card(
        task_id="task-candidates",
        event_key="crm.company_submission.confirm",
        title="选择要提交的线索",
        description="存在多个候选",
        selection_options=[
            {"lead_id": "lead-a", "company_name": "候选一"},
            {"lead_id": "lead-b", "company_name": "候选二"},
        ],
        selection_key="crm.company_submission.confirm",
    )

    assert card["card_type"] == "vote_interaction"
    assert card["checkbox"]["mode"] == 0  # type: ignore[index]
    assert card["submit_button"]["key"] == "crm.company_submission.confirm"  # type: ignore[index]


def test_batch_preview_long_values_keep_three_groups_and_bounded_chunks() -> None:
    """验证二十条超长候选仅展示十字段，缺项不改真实状态且每片不超过平台字节限制。"""
    names = (
        "业务线", "线索名称", "线索来源", "联系人", "职务", "沟通方式",
        "手机", "备注", "客户行业", "提交状态",
    )
    fields = dict.fromkeys(names, "*超长内容*" * 1000)
    fields.update({"提交状态": "未提交", "手机": [], "邮箱": "不应显示", "AI待确认": ["职务"]})
    candidates = [
        {
            "lead_id": f"lead-{index}", "company_name": "很长公司名称" * 1000,
            "field_values": dict(fields), "missing_fields": ["手机"],
        }
        for index in range(20)
    ]
    chunks = build_batch_submission_markdown(candidates, page=1, page_count=1)
    assert len(chunks) > 1 and all(len(chunk.encode("utf-8")) <= 4096 for chunk in chunks)
    content = "\n".join(chunks)
    assert all(content.count(name + "：") == 20 for name in names)
    assert "邮箱" not in content and "AI待确认" not in content
    assert content.count("提交状态：未提交") == 20
    assert content.count("手机：**未填写**") == 20
    assert content.count("- 缺少：手机") == 20
    for block in content.split("【")[1:]:
        rows = [
            line for line in block.splitlines()
            if line.startswith(("业务线：", "联系人：", "手机："))
        ]
        assert len(rows) == 3
    assert fields["备注"] == "*超长内容*" * 1000 and fields["提交状态"] == "未提交"


def test_batch_submission_card_allows_multi_selection() -> None:
    """验证“提交我所有线索”候选卡支持勾选多条线索。"""

    card = build_action_card(
        task_id="task-batch-candidates",
        event_key=CARD_EVENT_KEY_CRM_BATCH_SUBMISSION,
        title="选择要提交的线索",
        description="请勾选需要提交的线索",
        selection_options=[
            {
                "lead_id": "lead-a",
                "company_name": "候选一",
                "display_text": "候选一｜王工｜2026-09-29",
            },
            {
                "lead_id": "lead-b",
                "company_name": "候选二",
                "display_text": "候选二｜李工｜2026-09-28",
            },
        ],
        selection_key=CARD_EVENT_KEY_CRM_BATCH_SUBMISSION,
    )

    assert card["checkbox"]["mode"] == 1  # type: ignore[index]
    assert card["submit_button"]["key"] == CARD_EVENT_KEY_CRM_BATCH_SUBMISSION  # type: ignore[index]
    options = card["checkbox"]["option_list"]  # type: ignore[index]
    assert options[0]["text"] != options[1]["text"]  # type: ignore[index]
    assert [option["text"] for option in options] == ["1. 候选一", "2. 候选二"]  # type: ignore[index]


def test_batch_markdown_and_checkbox_share_frozen_page_order() -> None:
    """验证批量 Markdown 完整字段与卡片短选项按同一页序号对应。"""

    candidates = [
        {
            "lead_id": "lead-a",
            "company_name": "候选一",
            "display_text": "候选一｜王工｜2026-09-30",
            "field_values": {"业务线": "协作机器人", "联系人": "王工", "AI待确认": ["职务"]},
            "missing_fields": ["职务"],
        },
        {
            "lead_id": "lead-b",
            "company_name": "候选二",
            "display_text": "候选二｜李工｜2026-09-30",
            "field_values": {"业务线": "车载机器人", "联系人": "李工"},
        },
    ]
    markdown = build_batch_submission_markdown(candidates, page=1, page_count=1)
    card = build_action_card(
        task_id="task-batch-details",
        event_key=CARD_EVENT_KEY_CRM_BATCH_SUBMISSION,
        title="选择要提交的线索",
        description="请勾选需要提交的线索",
        selection_options=[
            {"lead_id": item["lead_id"], "company_name": item["company_name"]}
            for item in candidates
        ],
        selection_key=CARD_EVENT_KEY_CRM_BATCH_SUBMISSION,
    )

    assert len(markdown) == 1
    assert "【1】候选一｜王工｜2026-09-30" in markdown[0]
    assert "【2】候选二｜李工｜2026-09-30" in markdown[0]
    assert "AI待确认" not in markdown[0]
    assert "提交状态：**未填写**" in markdown[0]
    assert "- 缺少：职务" in markdown[0]
    options = card["checkbox"]["option_list"]  # type: ignore[index]
    assert [option["id"] for option in options] == ["lead-a", "lead-b"]  # type: ignore[index]
    assert [option["text"] for option in options] == ["1. 候选一", "2. 候选二"]  # type: ignore[index]


def test_batch_submission_action_accepts_single_candidate_and_freezes_context(
    session_factory: sessionmaker[Session],
) -> None:
    """验证只有一条未提交线索时仍发行可选择的批量卡。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-batch-card",
        command_text="提交我所有线索",
        candidates=({"lead_id": "lead-a", "company_name": "候选一"},),
    )

    assert action.context["candidate_leads"] == [{"lead_id": "lead-a", "company_name": "候选一"}]
    assert action.context["page"] == 1
    assert action.context["page_count"] == 1
    with session_factory() as session:
        notice = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "wecom_action_card"
            )
        )
    assert notice is not None
    assert notice.payload["template_card"]["checkbox"]["mode"] == 1  # type: ignore[index]


def test_batch_submission_action_freezes_page_for_more_than_one_page(
    session_factory: sessionmaker[Session],
) -> None:
    """验证超过 20 条候选时服务端动作可冻结独立分页。"""

    _authorize(session_factory)
    candidates = tuple(
        {"lead_id": f"lead-{index}", "company_name": f"公司{index}"} for index in range(20)
    )
    action = _service(session_factory).issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-page",
        command_text="提交我所有线索",
        candidates=candidates,
        page=2,
        page_count=3,
    )

    assert action.context["page"] == 2
    assert action.context["page_count"] == 3
    with session_factory() as session:
        notice = session.scalar(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "wecom_action_card"
            )
        )
    assert notice is not None
    assert "第 2/3 批" in notice.content


def test_batch_callback_accepts_only_selected_frozen_subset(
    session_factory: sessionmaker[Session],
) -> None:
    """验证批量回调只认服务端冻结候选中的已勾选子集。"""

    lead_ids = _seed_batch_leads(session_factory)
    action = _service(session_factory).issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-subset",
        command_text="提交我所有线索",
        candidates=tuple({"lead_id": lead_id, "company_name": "同名公司"} for lead_id in lead_ids),
    )
    result = _service(session_factory).claim_callback(
        _batch_frame_for_action(action, lead_ids[:2], msgid="provider-subset")
    )

    assert result.code == "claimed"
    with session_factory() as session:
        stored = session.get(WecomAction, action.id)
        assert stored is not None
        assert stored.context["selected_lead_ids"] == list(lead_ids[:2])
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_batch_callback_accepts_temporary_unsubmitted_candidate(
    session_factory: sessionmaker[Session],
) -> None:
    """验证“全部未提交”卡片可确认资料不完整的 temporary 线索。"""

    lead_ids = _seed_batch_leads(session_factory, count=1)
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_ids[0])
        assert lead is not None
        lead.lifecycle_state = "temporary"
    action = _service(session_factory).issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-temporary-candidate",
        command_text="提交我所有线索",
        candidates=({"lead_id": lead_ids[0], "company_name": "同名公司"},),
    )

    result = _service(session_factory).claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid="provider-temporary-candidate")
    )

    assert result.code == "claimed"


def test_batch_callback_rejects_injected_option_id(
    session_factory: sessionmaker[Session],
) -> None:
    """验证客户端注入未发行 option id 时不创建执行 outbox。"""

    lead_ids = _seed_batch_leads(session_factory)
    action = _service(session_factory).issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-injected-option",
        command_text="提交我所有线索",
        candidates=tuple({"lead_id": lead_id, "company_name": "同名公司"} for lead_id in lead_ids),
    )
    result = _service(session_factory).claim_callback(
        _batch_frame_for_action(action, ("not-frozen-lead",), msgid="provider-injected")
    )

    assert result.code == "selection_mismatch"
    with session_factory() as session:
        assert session.scalars(select(WecomActionOutbox)).all() == []


def test_batch_callback_rechecks_owner_transfer_and_status_change(
    session_factory: sessionmaker[Session],
) -> None:
    """验证卡片发行后负责人转移或生命周期变化都会在回调前拒绝。"""

    lead_ids = _seed_batch_leads(session_factory, count=2)
    _authorize(session_factory, "sales-b")
    action = _service(session_factory).issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-owner-change",
        command_text="提交我所有线索",
        candidates=tuple({"lead_id": lead_id, "company_name": "同名公司"} for lead_id in lead_ids),
    )
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_ids[0])
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-b"
        lead.lifecycle_state = "synced"

    result = _service(session_factory).claim_callback(
        _batch_frame_for_action(action, (lead_ids[0],), msgid="provider-owner-change")
    )

    assert result.code == "claimed"
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_batch_callback_replay_is_idempotent(
    session_factory: sessionmaker[Session],
) -> None:
    """验证同一批量卡重复投递不会产生第二次执行。"""

    lead_ids = _seed_batch_leads(session_factory, count=1)
    action = _service(session_factory).issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-replay",
        command_text="提交我所有线索",
        candidates=({"lead_id": lead_ids[0], "company_name": "同名公司"},),
    )
    service = _service(session_factory)
    first = service.claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid="provider-replay-1")
    )
    duplicate = service.claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid="provider-replay-1")
    )
    replay = service.claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid="provider-replay-2")
    )

    assert first.code == "claimed"
    assert duplicate.code == "duplicate_delivery"
    assert replay.code == "action_processing"
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_today_callback_claim_executor_submits_only_selected_lead(
    session_factory: sessionmaker[Session],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 TODAY callback 经持久化 claim 和 executor 后仅提交勾选线索一次。"""

    import app.crm.service as crm_service

    employee_path = tmp_path / "employee.csv"
    employee_path.write_text("id,name,nickname\ncrm-sales-a,测试销售,测试\n", encoding="utf-8")
    settings = get_settings().model_copy(update={"employee_directory_path": str(employee_path)})
    monkeypatch.setattr(crm_service, "get_settings", lambda: settings)
    monkeypatch.setattr(crm_service, "_TEST_EMPLOYEE_DIRECTORY_PATH", employee_path, raising=False)

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_ids = _seed_today_submission_leads(session_factory, adapter)
    action_service = _service(session_factory)
    action = _issue_today_action(action_service, lead_ids, message_id="today-selection")
    claim = action_service.claim_callback(
        _batch_frame_for_action(action, (lead_ids[0],), msgid="today-provider-1")
    )
    replay = action_service.claim_callback(
        _batch_frame_for_action(action, (lead_ids[0],), msgid="today-provider-2")
    )
    crm = MockCRMAdapter()
    executor = DeterministicWecomActionExecutor(session_factory, adapter, crm, action_service)
    execution = action_service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert replay.code == "action_processing"
    assert claim.should_update_card is True
    assert replay.should_update_card is False
    assert action.action_type == "crm_batch_submission"
    card = claim.response_card()
    assert card["card_type"] == "vote_interaction"
    checkbox = card["checkbox"]
    assert checkbox["mode"] == 1  # type: ignore[index]
    assert checkbox["disable"] is True  # type: ignore[index]
    assert checkbox["option_list"] == [  # type: ignore[index]
        {
            "id": lead_ids[0],
            "text": "1. TODAY测试公司-0",
            "is_checked": True,
        },
        {
            "id": lead_ids[1],
            "text": "2. TODAY测试公司-1",
            "is_checked": False,
        },
    ]
    assert card["replace_text"] == "已确认选择"
    assert execution.executed is True
    replay_execution = action_service.execute_action(action.id, executor)
    assert replay_execution.executed is False
    assert crm.search_calls == 1 and crm.calls == 1 and crm.update_calls == 0
    assert [payload["name"] for payload in crm.payloads] == ["TODAY测试公司-0"]
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_duplicate_response_message_never_enters_result_notification(
    session_factory: sessionmaker[Session],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 CRM 重复原文不会进入动作结果通知，且缺少 ID 时不创建 CRM 线索。"""
    import app.crm.service as crm_service

    employee_path = tmp_path / "employee.csv"
    employee_path.write_text("id,name,nickname\ncrm-sales-a,测试销售,测试\n", encoding="utf-8")
    settings = get_settings().model_copy(update={"employee_directory_path": str(employee_path)})
    monkeypatch.setattr(crm_service, "get_settings", lambda: settings)
    monkeypatch.setattr(crm_service, "_TEST_EMPLOYEE_DIRECTORY_PATH", employee_path, raising=False)

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_ids = _seed_today_submission_leads(session_factory, adapter)
    action_service = _service(session_factory)
    action = _issue_today_action(action_service, lead_ids, message_id="duplicate-message-safe")
    claim = action_service.claim_callback(
        _batch_frame_for_action(action, (lead_ids[0],), msgid="duplicate-message-safe-1")
    )

    class DuplicateTargetCRM(MockCRMAdapter):
        """返回安全分类，但异常正文模拟远端敏感文案。"""

        def search_by_company_name(
            self, payload: Mapping[str, object] | str
        ) -> tuple[CRMSearchResult, ...]:
            """阻止测试走 create，并验证远端正文不被结果路径读取。"""
            del payload
            self.search_calls += 1
            raise SopCRMError(
                "PRIVATE RAW SOP MESSAGE",
                category="duplicate_target_unavailable",
                sub_code="duplicate_detected_without_lead_id",
                duplicate_entity_type="lead",
            )

    crm = DuplicateTargetCRM()
    executor = DeterministicWecomActionExecutor(session_factory, adapter, crm, action_service)
    execution = action_service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert execution.executed is True
    assert crm.search_calls == 1 and crm.calls == 0
    with session_factory() as session:
        notifications = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "wecom_action_result",
                NotificationRecord.source_message_id == "duplicate-message-safe",
            )
        ).all()
    assert notifications
    assert all("PRIVATE RAW SOP MESSAGE" not in (item.content or "") for item in notifications)
    assert any("CRM 检测到重复线索" in (item.content or "") for item in notifications)


def test_today_callback_defers_temporary_lifecycle_to_worker_revalidation(
    session_factory: sessionmaker[Session],
) -> None:
    """验证冻结候选回调允许 temporary 进入 worker 最终快照复核。"""

    _authorize(session_factory)
    lead_ids = _seed_batch_leads(session_factory, count=1)
    action_service = _service(session_factory)
    action = _issue_today_action(action_service, lead_ids, message_id="today-state-change")
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_ids[0])
        assert lead is not None
        lead.lifecycle_state = "temporary"

    result = action_service.claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid="today-provider-state-change")
    )

    assert result.code == "claimed"
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_incomplete_submission_results_are_fenced_and_persisted_on_action(
    session_factory: sessionmaker[Session],
) -> None:
    """验证批量动作只持久化安全逐条结果，且使用当前 claim token fencing。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_ids = _seed_today_submission_leads(session_factory, adapter, count=1)
    record = next(iter(adapter.get_records()))
    adapter.update_record(record.record_id, {"业务线": ""})
    action_service = _service(session_factory)
    action = _issue_today_action(action_service, lead_ids, message_id="fenced-incomplete-results")
    claim = action_service.claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid="fenced-incomplete-provider")
    )
    crm = MockCRMAdapter()
    executor = DeterministicWecomActionExecutor(session_factory, adapter, crm, action_service)

    execution = action_service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert execution.executed is True
    assert crm.search_calls == 0 and crm.calls == 0
    with session_factory() as session:
        saved = session.get(WecomAction, action.id)
        outbox = session.scalar(
            select(WecomActionOutbox).where(WecomActionOutbox.action_id == action.id)
        )
    assert saved is not None and saved.status == "succeeded"
    assert saved.context["selected_lead_ids"] == [lead_ids[0]]
    assert saved.context["submission_results"] == [
        {
            "lead_id": lead_ids[0],
            "status": "incomplete",
            "reason_code": "missing_required_fields",
            "missing_fields": ["业务线"],
        }
    ]
    assert saved.context["incomplete_selected_lead_ids"] == [lead_ids[0]]
    assert outbox is not None and outbox.status == "succeeded"
    with pytest.raises(StaleActionClaim):
        action_service.record_submission_results(
            action.id,
            "stale-worker-token",
            (
                {
                    "lead_id": lead_ids[0],
                    "status": "created",
                    "reason_code": None,
                    "missing_fields": (),
                },
            ),
        )
    with session_factory() as session:
        saved_after_stale_write = session.get(WecomAction, action.id)
    assert saved_after_stale_write is not None
    assert saved_after_stale_write.context["submission_results"] == saved.context[
        "submission_results"
    ]


def test_unique_company_temporary_incomplete_is_confirmable_and_result_persisted(
    session_factory: sessionmaker[Session],
) -> None:
    """验证唯一指定线索的不完整 temporary 可确认，动作成功但 CRM 零调用。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_ids = _seed_today_submission_leads(session_factory, adapter, count=1)
    record = next(iter(adapter.get_records()))
    adapter.update_record(record.record_id, {"职务": ""})
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_ids[0])
        assert lead is not None
        lead.lifecycle_state = "temporary"
    action_service = _service(session_factory)
    action = action_service.issue_company_submission_confirmation_action(
        actor_user_id="sales-a",
        lead_id=lead_ids[0],
        request_message_id="single-incomplete-request",
        company_name="TODAY测试公司-0",
        display_text="TODAY测试公司-0｜王工｜日期",
        field_values={"线索名称": "TODAY测试公司-0"},
    )
    claim = action_service.claim_callback(
        _frame_for_action(action, msgid="single-incomplete-provider")
    )
    crm = MockCRMAdapter()
    executor = DeterministicWecomActionExecutor(session_factory, adapter, crm, action_service)

    execution = action_service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert execution.executed is True
    assert "缺少「职务」" in execution.summary
    assert crm.search_calls == 0 and crm.calls == 0
    with session_factory() as session:
        saved = session.get(WecomAction, action.id)
    assert saved is not None and saved.status == "succeeded"
    assert saved.context["selected_lead_ids"] == [lead_ids[0]]
    assert saved.context["submission_results"][0]["status"] == "incomplete"


def test_multiple_company_candidates_keep_incomplete_temporary_selectable(
    session_factory: sessionmaker[Session],
) -> None:
    """验证指定线索多候选仍能选择不完整 temporary，且未选项不进入结果。"""
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_ids = _seed_today_submission_leads(session_factory, adapter, count=2)
    records = adapter.get_records()
    adapter.update_record(records[0].record_id, {"沟通方式": ""})
    with session_factory.begin() as session:
        for lead_id in lead_ids:
            lead = session.get(Lead, lead_id)
            assert lead is not None
            lead.lifecycle_state = "temporary"
    action_service = _service(session_factory)
    action = action_service.issue_company_candidate_confirmation_action(
        actor_user_id="sales-a",
        request_message_id="multi-single-incomplete-request",
        company_name="TODAY测试公司",
        candidates=(
            {
                "lead_id": lead_ids[0],
                "company_name": "候选公司一",
                "display_text": "候选公司一｜王工",
            },
            {
                "lead_id": lead_ids[1],
                "company_name": "候选公司二",
                "display_text": "候选公司二｜王工",
            },
        ),
    )
    claim = action_service.claim_callback(
        _batch_frame_for_action(
            action, (lead_ids[0],), msgid="multi-single-incomplete-provider"
        )
    )
    crm = MockCRMAdapter()
    executor = DeterministicWecomActionExecutor(session_factory, adapter, crm, action_service)

    execution = action_service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert execution.executed is True
    assert "缺少「沟通方式」" in execution.summary
    assert crm.search_calls == 0 and crm.calls == 0
    with session_factory() as session:
        saved = session.get(WecomAction, action.id)
    assert saved is not None
    assert saved.context["selected_lead_ids"] == [lead_ids[0]]
    assert [item["lead_id"] for item in saved.context["submission_results"]] == [lead_ids[0]]


def test_today_callback_defers_owner_transfer_to_worker_revalidation(
    session_factory: sessionmaker[Session],
) -> None:
    """验证负责人变更不使已接受选择静默消失，留给 worker 逐条复核。"""

    _authorize(session_factory)
    lead_ids = _seed_batch_leads(session_factory, count=1)
    action_service = _service(session_factory)
    action = _issue_today_action(action_service, lead_ids, message_id="today-owner-change")
    with session_factory.begin() as session:
        lead = session.get(Lead, lead_ids[0])
        assert lead is not None
        lead.smart_table_owner_user_id = "sales-b"

    result = action_service.claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid="today-provider-owner-change")
    )

    assert result.code == "claimed"
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


@pytest.mark.parametrize("sync_status", ("succeeded", "abandoned"))
def test_today_callback_defers_terminal_create_status_to_worker_revalidation(
    session_factory: sessionmaker[Session], sync_status: str
) -> None:
    """验证 CRM 状态变化后 callback 仍由 worker 形成逐条未提交结果。"""

    _authorize(session_factory)
    lead_ids = _seed_batch_leads(session_factory, count=1)
    action_service = _service(session_factory)
    action = _issue_today_action(
        action_service, lead_ids, message_id=f"today-terminal-{sync_status}"
    )
    with session_factory.begin() as session:
        session.add(
            CrmSyncRecord(
                lead_id=lead_ids[0],
                operation="create",
                generation=1,
                smart_table_record_id="record-terminal",
                idempotency_key=f"crm:create:{lead_ids[0]}",
                canonical_payload={},
                snapshot_hash="a" * 64,
                request_message_id=f"terminal-{sync_status}",
                submitting_sales_user_id="sales-a",
                submitting_crm_user_id="crm-a",
                crm_lead_id="crm-A" if sync_status == "succeeded" else None,
                status=sync_status,
                completed_at=utc_now(),
            )
        )

    result = action_service.claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid=f"today-provider-terminal-{sync_status}")
    )

    assert result.code == "claimed"
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_today_callback_rejects_injected_lead_id_and_unknown_context(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 TODAY callback 拒绝客户端注入目标和被篡改的服务端命令上下文。"""

    _authorize(session_factory)
    lead_ids = _seed_batch_leads(session_factory, count=1)
    action_service = _service(session_factory)
    action = _issue_today_action(action_service, lead_ids, message_id="today-injected")
    injected = action_service.claim_callback(
        _batch_frame_for_action(action, ("injected-lead",), msgid="today-provider-injected")
    )
    assert injected.code == "selection_mismatch"

    next_action = _issue_today_action(action_service, lead_ids, message_id="today-invalid-context")
    with session_factory.begin() as session:
        stored = session.get(WecomAction, next_action.id)
        assert stored is not None
        stored.context = {**stored.context, "command_text": "unknown command"}
    invalid = action_service.claim_callback(
        _batch_frame_for_action(next_action, lead_ids, msgid="today-provider-invalid-context")
    )

    assert invalid.code == "invalid_action_context"
    with session_factory() as session:
        assert session.scalars(select(WecomActionOutbox)).all() == []


def test_abandoned_only_card_defers_stale_candidate_to_worker(
    session_factory: sessionmaker[Session],
) -> None:
    """验证放弃提交候选失效时由 worker 逐条拒绝而不是静默丢项。"""

    lead_ids = _seed_batch_leads(session_factory, count=1)
    action = _service(session_factory).issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-abandoned-only",
        command_text="帮我提交放弃提交的线索",
        candidates=({"lead_id": lead_ids[0], "company_name": "同名公司"},),
    )
    result = _service(session_factory).claim_callback(
        _batch_frame_for_action(action, lead_ids, msgid="provider-abandoned-only")
    )

    assert result.code == "claimed"
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_duplicate_provider_msgid_and_different_msgid_claim_once(
    session_factory: sessionmaker[Session],
) -> None:
    """验证传输重复和同 action 的不同 msgid 都只产生一次执行 claim。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-1", reason="明确废弃"
    )
    service = _service(session_factory)
    first = service.claim_callback(_frame_for_action(action, msgid="provider-msg-100"))
    duplicate = service.claim_callback(_frame_for_action(action, msgid="provider-msg-100"))
    replay = service.claim_callback(_frame_for_action(action, msgid="provider-msg-101"))
    calls = 0

    def executor(snapshot: object) -> tuple[str, str]:
        """记录唯一的 domain execution 调用。"""

        nonlocal calls
        calls += 1
        return "discarded", "已废弃"

    completed = service.execute_action(first.action_id or "", executor)

    assert first.code == "claimed"
    assert duplicate.code == "duplicate_delivery"
    assert replay.code == "action_processing"
    assert completed.executed is True
    assert calls == 1
    with session_factory.begin() as session:
        delivery = session.scalar(
            select(WecomCallbackDelivery).where(WecomCallbackDelivery.action_id == action.id)
        )
        assert delivery is not None and delivery.processing_status == "completed"
        stored = session.get(WecomAction, action.id)
        assert stored is not None
        stored.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    assert service.claim_callback(_frame_for_action(action, msgid="provider-msg-103")).code == (
        "action_succeeded"
    )
    with session_factory() as session:
        stored = session.get(WecomAction, action.id)
        assert stored is not None and stored.status == "succeeded"
    assert service.claim_callback(_frame_for_action(action, msgid="provider-msg-102")).code == (
        "action_succeeded"
    )


def test_callback_response_failure_does_not_repeat_business_action(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 update_template_card 传输失败时，后续 callback 仍不会重复 domain action。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-2", reason="重复客户"
    )
    handler = WecomTemplateCardCallbackHandler(_service(session_factory))
    frame = _frame_for_action(action, msgid="provider-msg-200")
    updates = 0

    async def failing_update(frame_value: object, card: dict[str, object]) -> None:
        """模拟唯一 callback response 传输失败。"""

        nonlocal updates
        del frame_value, card
        updates += 1
        raise ConnectionError("expired callback frame")

    asyncio.run(handler.handle(frame, failing_update))
    second = _frame_for_action(action, msgid="provider-msg-201")
    asyncio.run(handler.handle(second, failing_update))

    calls = 0

    def executor(snapshot: object) -> tuple[str, str]:
        """记录 callback response 失败后的唯一 domain 调用。"""

        nonlocal calls
        calls += 1
        return "discarded", "已废弃"

    service = _service(session_factory)
    service.execute_action(action.id, executor)

    assert updates == 1
    assert calls == 1
    with session_factory() as session:
        failed_delivery = session.scalar(
            select(WecomCallbackDelivery).where(
                WecomCallbackDelivery.provider_msgid == "provider-msg-200"
            )
        )
        replay_delivery = session.scalar(
            select(WecomCallbackDelivery).where(
                WecomCallbackDelivery.provider_msgid == "provider-msg-201"
            )
        )
        assert failed_delivery is not None
        assert failed_delivery.transport_stage == "callback_card_update"
        assert failed_delivery.transport_status == "failed"
        assert replay_delivery is not None and replay_delivery.transport_status is None


def test_callback_nonzero_sdk_ack_is_transport_failure_without_ack_text(
    session_factory: sessionmaker[Session], caplog: pytest.LogCaptureFixture
) -> None:
    """验证 SDK 非零 ACK 被记为失败，只保存错误码而不保存 errmsg。"""
    _authorize(session_factory)
    action = _service(session_factory).issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-ack-error", reason="ACK 测试"
    )
    handler = WecomTemplateCardCallbackHandler(_service(session_factory))

    async def rejected_ack(frame_value: object, card: dict[str, object]) -> dict[str, object]:
        """模拟 SDK 对非零 ACK 抛出异常，且 errmsg 含敏感正文。"""
        del frame_value, card
        raise RuntimeError(
            "Reply ack error: reqId=DO_NOT_LOG_REQID, errcode=42045, "
            "errmsg=PRIVATE RESPONSE TOKEN"
        )

    frame = _frame_for_action(action, msgid="provider-msg-ack-error")
    asyncio.run(handler.handle(frame, rejected_ack))

    with session_factory() as session:
        delivery = session.scalar(
            select(WecomCallbackDelivery).where(
                WecomCallbackDelivery.provider_msgid == "provider-msg-ack-error"
            )
        )
    assert delivery is not None
    assert delivery.transport_status == "failed"
    assert delivery.transport_failure_code == "sdk_ack_errcode_42045"
    assert delivery.transport_failure_summary == "企业微信 SDK 拒绝卡片更新（错误码 42045）"
    assert "PRIVATE RESPONSE" not in str(delivery.transport_failure_summary)
    assert "DO_NOT_LOG_REQID" not in str(delivery.transport_failure_summary)
    assert "PRIVATE RESPONSE" not in caplog.text
    assert "DO_NOT_LOG_REQID" not in caplog.text


def test_callback_response_success_freezes_card_and_saves_transport_evidence(
    session_factory: sessionmaker[Session], caplog: pytest.LogCaptureFixture
) -> None:
    """验证首次合法点击只更新一次卡片，并保存成功 transport evidence。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-success", reason="确认"
    )
    handler = WecomTemplateCardCallbackHandler(_service(session_factory))
    frame = _frame_for_action(action, msgid="provider-msg-success")
    updates: list[dict[str, object]] = []

    async def successful_update(
        frame_value: object, card: dict[str, object]
    ) -> dict[str, object]:
        """记录平台收到的唯一卡片冻结响应。"""

        assert frame_value is frame
        assert card["task_id"] == action.task_id
        updates.append(card)
        return {"errcode": 0}

    caplog.set_level("INFO", logger="app.wecom_bot.callback")
    asyncio.run(handler.handle(frame, successful_update))
    asyncio.run(
        handler.handle(_frame_for_action(action, msgid="provider-msg-replay"), successful_update)
    )

    assert len(updates) == 1
    assert updates[0]["card_type"] == "text_notice"
    assert updates[0]["card_action"] == {"type": 0}
    assert updates[0]["main_title"] == {
        "title": "已确认选择",
        "desc": "已受理，后台正在提交，请勿重复操作",
    }
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1
        delivery = session.scalar(
            select(WecomCallbackDelivery).where(
                WecomCallbackDelivery.provider_msgid == "provider-msg-success"
            )
        )
        assert delivery is not None
        assert delivery.action_id == action.id
        assert delivery.transport_stage == "callback_card_update"
        assert delivery.transport_status == "succeeded"
        assert delivery.processed_at is not None
    timing = [
        record for record in caplog.records if record.msg == "wecom_template_card_callback_timing"
    ]
    assert len(timing) == 2
    for metric in (
        "callback_claim_duration_ms",
        "remaining_deadline_ms",
        "callback_total_duration_ms",
    ):
        for record in timing:
            assert type(getattr(record, metric)) is int
            assert getattr(record, metric) >= 0
    assert timing[0].card_update_duration_ms >= 0
    assert timing[1].card_update_duration_ms == 0
    assert "provider-msg" not in caplog.text


def test_batch_callback_freezes_original_vote_card_and_ignores_client_names(
    session_factory: sessionmaker[Session],
) -> None:
    """验证首次批量确认以服务端候选更新同型投票卡，并且 replay 不再更新。"""
    _authorize(session_factory)
    service = _service(session_factory)
    action = service.issue_batch_submission_action(
        actor_user_id="sales-a",
        request_message_id="message-vote-freeze",
        command_text="提交我所有线索",
        candidates=(
            {"lead_id": "lead-a", "company_name": "冻结公司A", "display_text": "服务端名称A"},
            {"lead_id": "lead-b", "company_name": "冻结公司B", "display_text": "服务端名称B"},
        ),
    )
    frame = _batch_frame_for_action(action, ("lead-b",), msgid="provider-vote-freeze")
    body = frame.get("body")
    assert isinstance(body, dict)
    event = body.get("event")
    assert isinstance(event, dict)
    card_event = event.get("template_card_event")
    assert isinstance(card_event, dict)
    selected_items = card_event.get("selected_items")
    assert isinstance(selected_items, dict)
    selected_list = selected_items.get("selected_item")
    assert isinstance(selected_list, list) and isinstance(selected_list[0], dict)
    selected_item = selected_list[0]
    selected_item["company_name"] = "客户端伪造名称"

    updates: list[dict[str, object]] = []
    frames: list[Mapping[str, object]] = []

    async def update_card(
        callback_frame: Mapping[str, object], card: dict[str, object]
    ) -> dict[str, int]:
        """记录单次更新，模拟企微 ACK 成功。"""
        frames.append(callback_frame)
        updates.append(card)
        return {"errcode": 0}

    handler = WecomTemplateCardCallbackHandler(service)
    asyncio.run(handler.handle(frame, update_card))
    replay_frame = _batch_frame_for_action(
        action, ("lead-b",), msgid="provider-vote-freeze-replay"
    )
    asyncio.run(handler.handle(replay_frame, update_card))

    assert len(updates) == 1
    assert frames[0] is frame
    card = updates[0]
    assert card["card_type"] == "vote_interaction"
    assert card["task_id"] == action.task_id
    assert card["main_title"] == {
        "title": "选择要提交的线索",
        "desc": "请勾选需要提交的线索；未勾选的线索不会调用 CRM",
    }
    assert card["checkbox"] == {
        "question_key": "crm_submission_candidates",
        "mode": 1,
        "option_list": [
            {"id": "lead-a", "text": "1. 冻结公司A", "is_checked": False},
            {"id": "lead-b", "text": "2. 冻结公司B", "is_checked": True},
        ],
        "disable": True,
    }
    assert card["submit_button"] == {
        "text": "确认选择",
        "key": CARD_EVENT_KEY_CRM_BATCH_SUBMISSION,
    }
    assert card["replace_text"] == "已确认选择"
    assert "card_action" not in card
    assert "客户端伪造名称" not in json.dumps(card, ensure_ascii=False)
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1
        delivery = session.scalar(
            select(WecomCallbackDelivery).where(
                WecomCallbackDelivery.provider_msgid == "provider-vote-freeze"
            )
        )
        assert delivery is not None and delivery.transport_status == "succeeded"


def test_unique_company_callback_freezes_button_card_once(
    session_factory: sessionmaker[Session],
) -> None:
    """验证唯一指定线索确认卡更新与 replay 幂等。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：卡片结构、回放次数或 Outbox 数量不符时断言失败。
    副作用：只写入测试数据库并调用本地模拟更新函数。
    """
    _authorize(session_factory)
    service = _service(session_factory)
    action = service.issue_company_submission_confirmation_action(
        actor_user_id="sales-a",
        lead_id="lead-company-freeze",
        request_message_id="message-company-freeze",
        company_name="冻结公司",
        field_values={"线索名称": "冻结公司"},
    )
    with session_factory.begin() as session:
        session.add(
            Lead(
                id="lead-company-freeze",
                original_capturing_sales_user_id="sales-a",
                smart_table_owner_user_id="sales-a",
                lifecycle_state="pending_create",
                field_values={},
            )
        )
    frame = _frame_for_action(action, msgid="provider-company-freeze")
    card_event = frame["body"]["event"]["template_card_event"]  # type: ignore[index]
    card_event["company_name"] = "客户端注入名称"
    updates: list[dict[str, object]] = []

    async def update_card(
        callback_frame: Mapping[str, object], card: dict[str, object]
    ) -> dict[str, int]:
        """记录并确认原 callback 对应的卡片更新。

        参数：callback_frame 为原始帧；card 为待更新卡片。
        返回值：模拟企业微信成功 ACK。
        异常：frame 非原对象时断言失败。
        副作用：追加一条更新到测试列表。
        """
        assert callback_frame is frame
        updates.append(card)
        return {"errcode": 0}

    handler = WecomTemplateCardCallbackHandler(service)
    asyncio.run(handler.handle(frame, update_card))
    asyncio.run(
        handler.handle(
            _frame_for_action(action, msgid="provider-company-freeze-replay"), update_card
        )
    )

    assert len(updates) == 1
    assert updates[0] == {
        "card_type": "button_interaction",
        "task_id": action.task_id,
        "main_title": {
            "title": "确认提交线索",
            "desc": "已受理，后台正在提交，请勿重复操作",
        },
        "button_list": [
            {"text": "确认", "style": 1, "key": CARD_EVENT_KEY_CRM_COMPANY_CONFIRM}
        ],
        "replace_text": "已确认提交",
    }
    assert "客户端注入名称" not in json.dumps(updates[0], ensure_ascii=False)
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_company_candidate_callback_freezes_single_vote_selection_once(
    session_factory: sessionmaker[Session],
) -> None:
    """验证指定公司多候选卡冻结选择状态和提交入口。

    参数：session_factory 提供隔离数据库。
    返回值：无。
    异常：卡片结构、回放次数或 Outbox 数量不符时断言失败。
    副作用：只写入测试数据库并调用本地模拟更新函数。
    """
    _authorize(session_factory)
    service = _service(session_factory)
    action = service.issue_company_candidate_confirmation_action(
        actor_user_id="sales-a",
        request_message_id="message-company-candidates",
        company_name="同名公司",
        candidates=(
            {
                "lead_id": "lead-company-a",
                "company_name": "同名公司（候选1）",
                "display_text": "同名公司｜张总｜2026年10月01日",
            },
            {
                "lead_id": "lead-company-b",
                "company_name": "同名公司（候选2）",
                "display_text": "同名公司｜李工｜2026年09月30日",
            },
        ),
    )
    with session_factory.begin() as session:
        session.add_all(
            [
                Lead(
                    id=lead_id,
                    original_capturing_sales_user_id="sales-a",
                    smart_table_owner_user_id="sales-a",
                    lifecycle_state="pending_create",
                    field_values={},
                )
                for lead_id in ("lead-company-a", "lead-company-b")
            ]
        )
    frame = _batch_frame_for_action(
        action, ("lead-company-b",), msgid="provider-company-candidates"
    )
    card_event = frame["body"]["event"]["template_card_event"]  # type: ignore[index]
    card_event["company_name"] = "客户端注入名称"
    updates: list[dict[str, object]] = []

    async def update_card(
        callback_frame: Mapping[str, object], card: dict[str, object]
    ) -> dict[str, int]:
        """记录指定公司候选卡更新并返回 ACK 成功。

        参数：callback_frame 为 callback 帧；card 为候选状态更新体。
        返回值：模拟企业微信成功 ACK。
        异常：frame 非本测试原对象时断言失败。
        副作用：追加一条更新到测试列表。
        """
        assert callback_frame is frame
        updates.append(card)
        return {"errcode": 0}

    handler = WecomTemplateCardCallbackHandler(service)
    asyncio.run(handler.handle(frame, update_card))
    asyncio.run(
        handler.handle(
            _batch_frame_for_action(
                action, ("lead-company-b",), msgid="provider-company-candidates-replay"
            ),
            update_card,
        )
    )

    assert len(updates) == 1
    assert updates[0]["card_type"] == "vote_interaction"
    assert updates[0]["task_id"] == action.task_id
    assert updates[0]["main_title"] == {
        "title": "选择要提交的线索",
        "desc": "公司名称“同名公司”存在多个精确候选，请选择一条",
    }
    assert updates[0]["checkbox"] == {
        "question_key": "crm_submission_candidates",
        "mode": 0,
        "option_list": [
            {"id": "lead-company-a", "text": "1. 同名公司（候选1）", "is_checked": False},
            {"id": "lead-company-b", "text": "2. 同名公司（候选2）", "is_checked": True},
        ],
        "disable": True,
    }
    assert updates[0]["submit_button"] == {
        "text": "确认选择",
        "key": CARD_EVENT_KEY_CRM_COMPANY_CONFIRM,
    }
    assert updates[0]["replace_text"] == "已确认选择"
    assert "客户端注入名称" not in json.dumps(updates[0], ensure_ascii=False)
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_company_submission_result_uses_frozen_label_and_safe_reason(
    session_factory: sessionmaker[Session],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证唯一指定提交结果使用发行时标签和受控重复原因。

    参数：数据库、临时员工目录和 monkeypatch fixture 用于隔离提交依赖。
    返回值：无。
    异常：结果、敏感信息或外部调用次数不符时断言失败。
    副作用：只写入测试数据库、临时 CSV 和模拟 CRM。
    """
    import app.crm.service as crm_service

    employee_path = tmp_path / "employee.csv"
    employee_path.write_text("id,name,nickname\ncrm-sales-a,测试销售,测试\n", encoding="utf-8")
    settings = get_settings().model_copy(update={"employee_directory_path": str(employee_path)})
    monkeypatch.setattr(crm_service, "get_settings", lambda: settings)
    monkeypatch.setattr(crm_service, "_TEST_EMPLOYEE_DIRECTORY_PATH", employee_path, raising=False)

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_ids = _seed_today_submission_leads(session_factory, adapter)
    lead_id = lead_ids[0]
    frozen_label = "服务端快照公司｜张总｜2026年10月01日"
    action_service = _service(session_factory)
    action = action_service.issue_company_submission_confirmation_action(
        actor_user_id="sales-a",
        lead_id=lead_id,
        request_message_id="company-safe-result",
        company_name="服务端快照公司",
        display_text=frozen_label,
        field_values={"线索名称": "服务端快照公司"},
    )
    frame = _frame_for_action(action, msgid="company-safe-result-provider")
    frame["body"]["event"]["template_card_event"]["company_name"] = "客户端注入名称"  # type: ignore[index]
    claim = action_service.claim_callback(frame)
    replay = action_service.claim_callback(
        _frame_for_action(action, msgid="company-safe-result-replay")
    )

    class DuplicateTargetCRM(MockCRMAdapter):
        """模拟重复目标没有可操作 CRM Lead ID 的受控结果。"""

        def search_by_company_name(
            self, payload: Mapping[str, object] | str
        ) -> tuple[CRMSearchResult, ...]:
            """阻止测试走 create，并返回安全 duplicate 分类。"""
            del payload
            self.search_calls += 1
            raise SopCRMError(
                "PRIVATE RAW SOP MESSAGE",
                category="duplicate_target_unavailable",
                sub_code="duplicate_detected_without_lead_id",
                duplicate_entity_type="lead",
            )

    crm = DuplicateTargetCRM()
    executor = DeterministicWecomActionExecutor(
        session_factory, adapter, crm, action_service
    )
    execution = action_service.execute_action(action.id, executor)
    replay_execution = action_service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert replay.code == "action_processing"
    assert execution.executed is True
    assert replay_execution.executed is False
    assert "CRM 提交结果（已选择 1 条）" in execution.summary
    assert f"{frozen_label}：CRM 检测到重复线索" in execution.summary
    assert "客户端注入名称" not in execution.summary
    assert "PRIVATE RAW SOP MESSAGE" not in execution.summary
    assert crm.search_calls == 1 and crm.calls == 0
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_company_candidate_submission_executes_only_selected_lead_and_uses_frozen_label(
    session_factory: sessionmaker[Session],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证指定公司多候选仅提交已选 Lead 并使用冻结标签。

    参数：数据库、临时员工目录和 monkeypatch fixture 用于隔离提交依赖。
    返回值：无。
    异常：结果、CRM 调用次数或 Outbox 数量不符时断言失败。
    副作用：只写入测试数据库、临时 CSV 和模拟 CRM。
    """
    import app.crm.service as crm_service

    employee_path = tmp_path / "employee.csv"
    employee_path.write_text("id,name,nickname\ncrm-sales-a,测试销售,测试\n", encoding="utf-8")
    settings = get_settings().model_copy(update={"employee_directory_path": str(employee_path)})
    monkeypatch.setattr(crm_service, "get_settings", lambda: settings)
    monkeypatch.setattr(crm_service, "_TEST_EMPLOYEE_DIRECTORY_PATH", employee_path, raising=False)

    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_ids = _seed_today_submission_leads(session_factory, adapter)
    labels = (
        "同名公司｜张总｜2026年10月01日",
        "同名公司｜李工｜2026年09月30日",
    )
    action_service = _service(session_factory)
    action = action_service.issue_company_candidate_confirmation_action(
        actor_user_id="sales-a",
        request_message_id="company-candidate-result",
        company_name="同名公司",
        candidates=tuple(
            {
                "lead_id": lead_id,
                "company_name": f"同名公司（候选{index}）",
                "display_text": labels[index - 1],
            }
            for index, lead_id in enumerate(lead_ids, start=1)
        ),
    )
    frame = _batch_frame_for_action(
        action, (lead_ids[1],), msgid="company-candidate-result-provider"
    )
    frame["body"]["event"]["template_card_event"]["company_name"] = "客户端注入名称"  # type: ignore[index]
    claim = action_service.claim_callback(frame)
    replay = action_service.claim_callback(
        _batch_frame_for_action(
            action, (lead_ids[1],), msgid="company-candidate-result-replay"
        )
    )
    crm = MockCRMAdapter()
    executor = DeterministicWecomActionExecutor(
        session_factory, adapter, crm, action_service
    )
    execution = action_service.execute_action(action.id, executor)
    replay_execution = action_service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert replay.code == "action_processing"
    assert execution.executed is True
    assert replay_execution.executed is False
    assert "CRM 提交结果（已选择 1 条）" in execution.summary
    assert f"{labels[1]}" in execution.summary
    assert "客户端注入名称" not in execution.summary
    assert crm.search_calls == 1 and crm.calls == 1 and crm.update_calls == 0
    assert [payload["name"] for payload in crm.payloads] == ["TODAY测试公司-1"]
    with session_factory() as session:
        assert len(session.scalars(select(WecomActionOutbox)).all()) == 1


def test_final_notification_retry_does_not_repeat_domain_action(
    session_factory: sessionmaker[Session],
) -> None:
    """验证最终通知失败重试时只重发通知，不重新调用领域服务。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-notice", reason="通知重试测试"
    )
    service = _service(session_factory)
    service.claim_callback(_frame_for_action(action, msgid="provider-msg-notice"))
    calls = 0

    def executor(snapshot: object) -> tuple[str, str]:
        """记录唯一一次领域动作执行。"""

        nonlocal calls
        calls += 1
        return "discarded", "已废弃"

    service.execute_action(action.id, executor)

    class FlakyClient:
        """首发失败、后续成功的最终通知客户端。"""

        def __init__(self) -> None:
            """初始化发送次数。"""

            self.attempts = 0
            self.failures_remaining = 2

        async def send_message(
            self, userid_or_chatid: str, body: dict[str, object]
        ) -> dict[str, str]:
            """第一次抛出传输错误，后续返回成功回执。"""

            del userid_or_chatid, body
            self.attempts += 1
            if self.failures_remaining:
                self.failures_remaining -= 1
                raise ConnectionError("notification transport failure")
            return {"status": "ok"}

    client = FlakyClient()
    sender = WecomOutboundNotificationSender(session_factory, client)
    assert asyncio.run(sender.send_pending_once()) == 0
    assert asyncio.run(sender.send_pending_once()) == 2
    assert service.execute_action(action.id, executor).code == "already_succeeded"
    assert calls == 1


def test_inactive_actor_is_denied_but_legacy_flag_is_ignored(
    session_factory: sessionmaker[Session],
) -> None:
    """验证卡片发行后停用 actor 会阻止动作，但历史授权字段不再阻断。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-3", reason="停用测试"
    )
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-a")
        assert authorization is not None
        authorization.is_active = False
    assert (
        _service(session_factory)
        .claim_callback(_frame_for_action(action, msgid="provider-msg-300"))
        .code
        == "actor_inactive"
    )

    _authorize(session_factory, "sales-b")
    action_b = _service(session_factory).issue_discard_action(
        actor_user_id="sales-b", lead_id="lead-4", reason="撤销测试"
    )
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-b")
        assert authorization is not None
        authorization.is_authorized = False
    assert (
        _service(session_factory)
        .claim_callback(_frame_for_action(action_b, msgid="provider-msg-301", actor="sales-b"))
        .code
        == "claimed"
    )


def test_field_confirmation_happy_path_reuses_lead_review_service(
    session_factory: sessionmaker[Session],
) -> None:
    """验证字段确认卡只确认当前 pending 字段并写入人工确认事件。"""

    _authorize(session_factory)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _seed_confirmable_lead(session_factory, adapter)
    service = _service(session_factory)
    action = service.issue_field_confirmation_action(
        actor_user_id="sales-a",
        lead_id=lead_id,
        field_names=("业务线",),
        command_text="提交今天的线索",
        request_message_id="message-action",
    )
    claim = service.claim_callback(_frame_for_action(action, msgid="provider-msg-400"))
    executor = DeterministicWecomActionExecutor(session_factory, adapter, MockCRMAdapter())

    result = service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert result.executed is True
    record = adapter.get_record(next(iter(adapter.get_records())).record_id)
    assert record is not None and record.fields["AI待确认"] == []
    with session_factory() as session:
        assert (
            session.scalars(
                select(LeadFieldProvenance).where(
                    LeadFieldProvenance.lead_id == lead_id,
                    LeadFieldProvenance.is_user_confirmed.is_(True),
                )
            ).first()
            is not None
        )


def test_stale_field_confirmation_card_cannot_overwrite_latest_table_state(
    session_factory: sessionmaker[Session],
) -> None:
    """验证旧卡在销售先完成表格确认后不会覆盖当前状态。"""

    _authorize(session_factory)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _seed_confirmable_lead(session_factory, adapter)
    service = _service(session_factory)
    action = service.issue_field_confirmation_action(
        actor_user_id="sales-a",
        lead_id=lead_id,
        field_names=("业务线",),
        command_text="提交今天的线索",
        request_message_id="message-action-stale",
    )
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"AI待确认": [], "业务线": "车载机器人"})
    service.claim_callback(_frame_for_action(action, msgid="provider-msg-401"))
    executor = DeterministicWecomActionExecutor(session_factory, adapter, MockCRMAdapter())

    result = service.execute_action(action.id, executor)

    assert result.executed is True
    current = adapter.get_record(record_id)
    assert current is not None and current.fields["业务线"] == "车载机器人"
    with session_factory() as session:
        assert (
            session.scalars(
                select(LeadFieldProvenance).where(
                    LeadFieldProvenance.lead_id == lead_id,
                    LeadFieldProvenance.is_user_confirmed.is_(True),
                )
            ).first()
            is not None
        )


def test_field_confirmation_remote_success_reconciles_without_blind_table_replay(
    session_factory: sessionmaker[Session],
) -> None:
    """验证远端已清除 AI待确认 而本地 finalize 失败时只读核对并补事实。"""

    _authorize(session_factory)
    adapter = MockSmartTableAdapter(schema=build_required_smart_table_schema())
    lead_id = _seed_confirmable_lead(session_factory, adapter)
    service = _service(session_factory)
    action = service.issue_field_confirmation_action(
        actor_user_id="sales-a",
        lead_id=lead_id,
        field_names=("业务线",),
        command_text="提交今天的线索",
        request_message_id="message-recovery",
        field_values={"业务线": "协作机器人"},
    )
    service.claim_callback(_frame_for_action(action, msgid="provider-msg-recovery"))
    record_id = next(iter(adapter.get_records())).record_id
    adapter.update_record(record_id, {"AI待确认": []})
    with session_factory.begin() as session:
        stored = session.get(WecomAction, action.id)
        outbox = session.scalar(
            select(WecomActionOutbox).where(WecomActionOutbox.action_id == action.id)
        )
        assert stored is not None and outbox is not None
        stored.status = "pending_recovery"
        outbox.status = "failed"
        outbox.remote_effect_status = "unknown"
        outbox.domain_operation_payload = {
            "field_values": {
                "业务线": "sha256:" + hashlib.sha256("协作机器人".encode("utf-8")).hexdigest()
            }
        }
    result = service.reconcile_field_confirmation(action.id, adapter)

    assert result.code == "confirmation_recovered"
    with session_factory() as session:
        assert (
            session.scalars(
                select(UserConfirmationEvent).where(UserConfirmationEvent.lead_id == lead_id)
            ).first()
            is not None
        )
        stored = session.get(WecomAction, action.id)
        assert stored is not None and stored.status == "succeeded"
        outbox = session.scalar(
            select(WecomActionOutbox).where(WecomActionOutbox.action_id == action.id)
        )
        assert outbox is not None and outbox.remote_effect_status == "succeeded"


def test_discard_confirmation_double_click_calls_existing_service_once(
    session_factory: sessionmaker[Session],
) -> None:
    """验证废弃卡重复点击只执行一次 LeadDiscardService 业务副作用。"""

    _authorize(session_factory)
    with session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-discard",
                sales_user_id="sales-a",
                sequence=1,
                raw_payload={},
            )
        )
        session.add(
            Lead(
                id="lead-discard",
                source_message_id="message-discard",
                original_capturing_sales_user_id="sales-a",
                smart_table_owner_user_id="sales-a",
                lifecycle_state="pending_create",
                field_values={"线索名称": "待废弃公司"},
            )
        )
    service = _service(session_factory)
    action = service.issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-discard", reason="客户明确不再跟进"
    )
    service.claim_callback(_frame_for_action(action, msgid="provider-msg-500"))
    calls = 0

    def executor(snapshot: object) -> tuple[str, str]:
        """记录并复用现有 LeadDiscardService。"""

        nonlocal calls
        calls += 1
        current = LeadDiscardService(session_factory).discard(
            action.target_id, "sales-a", "客户明确不再跟进", operation_id=action.id
        )
        return current.status.value, "废弃完成"

    service.execute_action(action.id, executor)
    service.claim_callback(_frame_for_action(action, msgid="provider-msg-501"))
    service.execute_action(action.id, executor)

    assert calls == 1
    with session_factory() as session:
        lead = session.get(Lead, "lead-discard")
        assert lead is not None and lead.lifecycle_state == "discarded"


def test_reassignment_confirmation_uses_server_frozen_target_once(
    session_factory: sessionmaker[Session],
) -> None:
    """验证重新归属 callback 不信任客户端目标字段且重复点击不重复归属。"""

    _authorize(session_factory)
    target_id = _seed_reassignment_case(session_factory)
    service = _service(session_factory)
    action = service.issue_reassignment_action(
        actor_user_id="sales-a",
        message_id="message-source",
        segment_index=0,
        target_lead_id=target_id,
        reason="销售明确选择目标线索",
    )
    service.claim_callback(_frame_for_action(action, msgid="provider-msg-600"))
    calls = 0

    def executor(snapshot: object) -> tuple[str, str]:
        """记录并调用既有 LeadReassignmentService。"""

        nonlocal calls
        calls += 1
        LeadReassignmentService(session_factory).reassign(
            "message-source",
            0,
            action.target_id,
            "sales-a",
            "销售明确选择目标线索",
            operation_id=action.id,
        )
        return "reassigned", "重新归属完成"

    service.execute_action(action.id, executor)
    service.claim_callback(_frame_for_action(action, msgid="provider-msg-601"))
    service.execute_action(action.id, executor)

    assert calls == 1
    with session_factory() as session:
        resolution = session.scalar(
            select(LeadMessageResolution).where(
                LeadMessageResolution.message_id == "message-source"
            )
        )
        audits = session.scalars(select(MessageReassignmentAudit)).all()
        assert resolution is not None and resolution.lead_id == target_id
        assert len(audits) == 1
