"""T18 企业微信确定性卡片动作的契约与幂等测试。"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.crm.commands import parse_crm_submission_command
from app.crm.mock import MockCRMAdapter
from app.leads.discard import LeadDiscardService
from app.leads.models import (
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
    SalesAuthorization,
    WecomAction,
    WecomActionOutbox,
    WecomCallbackDelivery,
)
from app.notifications.outbound import WecomOutboundNotificationSender
from app.smart_table.adapter import SmartTableActor
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema
from app.wecom_bot.actions import (
    CARD_EVENT_KEY_CRM_DUPLICATE_CONTINUE,
    CARD_EVENT_KEY_CRM_FIELD_CONFIRM,
    CARD_EVENT_KEY_DISCARD_CONFIRM,
    CARD_EVENT_KEY_REASSIGN_CONFIRM,
    CallbackParseError,
    DeterministicWecomActionExecutor,
    InvalidActionTransition,
    TemplateCardCallbackParser,
    WecomActionService,
    WecomActionStatus,
    _transition_action,
    build_action_card,
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


def test_exact_submission_commands_never_use_llm_intent_inference() -> None:
    """验证只有两个完整命令进入 CRM 命令边界，相似自然语言全部拒绝。"""

    assert parse_crm_submission_command("提交今天的线索") == "提交今天的线索"
    assert parse_crm_submission_command("提交我的更新") == "提交我的更新"
    assert parse_crm_submission_command("帮我提交今天的线索") is None
    assert parse_crm_submission_command("提交今天的线索。") is None
    assert parse_crm_submission_command("请提交我的更新") is None


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

    assert updates == 2
    assert calls == 1
    with session_factory() as session:
        deliveries = session.scalars(
            select(WecomCallbackDelivery).where(WecomCallbackDelivery.action_id == action.id)
        ).all()
        assert all(
            delivery.transport_stage == "callback_card_update"
            and delivery.transport_status == "failed"
            for delivery in deliveries
        )


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


def test_inactive_or_unauthorized_actor_is_denied(session_factory: sessionmaker[Session]) -> None:
    """验证卡片发行后停用或撤销授权都会阻止业务动作。"""

    _authorize(session_factory)
    action = _service(session_factory).issue_discard_action(
        actor_user_id="sales-a", lead_id="lead-3", reason="停用测试"
    )
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-a")
        assert authorization is not None
        authorization.is_active = False
    assert _service(session_factory).claim_callback(
        _frame_for_action(action, msgid="provider-msg-300")
    ).code == "actor_unauthorized"

    _authorize(session_factory, "sales-b")
    action_b = _service(session_factory).issue_discard_action(
        actor_user_id="sales-b", lead_id="lead-4", reason="撤销测试"
    )
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-b")
        assert authorization is not None
        authorization.is_authorized = False
    assert _service(session_factory).claim_callback(
        _frame_for_action(action_b, msgid="provider-msg-301", actor="sales-b")
    ).code == "actor_unauthorized"


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
    executor = DeterministicWecomActionExecutor(
        session_factory, adapter, MockCRMAdapter()
    )

    result = service.execute_action(action.id, executor)

    assert claim.code == "claimed"
    assert result.executed is True
    record = adapter.get_record(next(iter(adapter.get_records())).record_id)
    assert record is not None and record.fields["AI待确认"] == []
    with session_factory() as session:
        assert session.scalars(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead_id,
                LeadFieldProvenance.is_user_confirmed.is_(True),
            )
        ).first() is not None


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
    executor = DeterministicWecomActionExecutor(
        session_factory, adapter, MockCRMAdapter()
    )

    result = service.execute_action(action.id, executor)

    assert result.executed is True
    current = adapter.get_record(record_id)
    assert current is not None and current.fields["业务线"] == "车载机器人"
    with session_factory() as session:
        assert session.scalars(
            select(LeadFieldProvenance).where(
                LeadFieldProvenance.lead_id == lead_id,
                LeadFieldProvenance.is_user_confirmed.is_(True),
            )
        ).first() is not None


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
                "业务线": "sha256:"
                + hashlib.sha256("协作机器人".encode("utf-8")).hexdigest()
            }
        }
    result = service.reconcile_field_confirmation(action.id, adapter)

    assert result.code == "confirmation_recovered"
    with session_factory() as session:
        assert session.scalars(
            select(UserConfirmationEvent).where(UserConfirmationEvent.lead_id == lead_id)
        ).first() is not None
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
