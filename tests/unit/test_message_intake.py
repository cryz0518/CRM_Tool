"""可靠消息接收应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import app.messaging.service as messaging_service
from app.ai.models import SubmissionIntent
from app.core.config import Settings
from app.leads.models import LeadProgressMessage, LeadProgressSession
from app.leads.progress import activate_progress_intent_candidate
from app.messaging.models import (
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.messaging.service import IncomingMessageCommand, MessageIntakeResult, MessageIntakeService
from workers import tasks


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供隔离的真实 SQLAlchemy 事务，用于验证消息与发件箱持久化行为。"""
    # 使用共享内存 SQLite，让每个测试都拥有独立且真实的数据库事务。
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


def authorize_salesperson(session_factory: sessionmaker[Session], user_id: str) -> None:
    """在销售授权目录中登记可录入的销售身份。"""
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id=user_id, is_authorized=True, is_active=True))


def test_authorized_message_persists_message_and_pending_outbox_together(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证普通需求、Outbox 与从首条接收时刻起算的进度候选原子提交。"""
    authorize_salesperson(session_factory, "sales-1")
    settings = Settings(
        _env_file=None,
        lead_progress_enabled=True,
        lead_progress_interval_minutes=15,
        lead_progress_idle_stop_minutes=60,
    )
    monkeypatch.setattr(messaging_service, "get_settings", lambda: settings)
    started_before = datetime.now(UTC)
    intake = MessageIntakeService(session_factory)
    result = intake.receive(
        IncomingMessageCommand(
            message_id="message-1",
            sales_user_id="sales-1",
            raw_payload={"text": "客户需要码垛机器人"},
            normalized_text="客户需要码垛机器人",
        )
    )
    duplicate = intake.receive(
        IncomingMessageCommand(
            message_id="message-1",
            sales_user_id="sales-1",
            raw_payload={"text": "客户需要码垛机器人"},
            normalized_text="客户需要码垛机器人",
        )
    )

    assert result.accepted is True
    assert result.duplicate is False
    assert duplicate.accepted is True and duplicate.duplicate is True
    with session_factory() as session:
        message = session.scalar(
            select(IncomingMessage).where(IncomingMessage.message_id == "message-1")
        )
        event = session.scalar(select(OutboxEvent).where(OutboxEvent.message_id == "message-1"))
        audit = session.scalar(
            select(BusinessAuditEvent).where(BusinessAuditEvent.event_type == "message_received")
        )
        progress = session.scalar(select(LeadProgressSession))
        progress_message = session.get(LeadProgressMessage, "message-1")

    assert message is not None
    assert message.sequence == 1
    assert event is not None
    assert event.status == "pending"
    assert event.sequence == message.sequence
    assert audit is not None
    assert progress is not None and progress_message is not None
    assert progress_message.status == "processing"
    assert progress.started_at == progress_message.received_at == message.received_at
    assert progress.next_report_at == progress.started_at + timedelta(minutes=15)
    assert progress.started_at.replace(tzinfo=UTC) >= started_before
    with session_factory() as session:
        assert session.scalar(select(LeadProgressMessage)) is not None
        assert len(session.scalars(select(LeadProgressMessage)).all()) == 1


def test_progress_registration_failure_rolls_back_message_and_outbox(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证首条进度候选与消息入库同事务，失败不会留下半套接收事实。"""
    authorize_salesperson(session_factory, "sales-1")
    monkeypatch.setattr(
        messaging_service,
        "get_settings",
        lambda: Settings(_env_file=None, lead_progress_enabled=True),
    )

    def fail_registration(*_args: object, **_kwargs: object) -> bool:
        """模拟进度登记 DML 错误。"""
        raise RuntimeError("isolated progress write failure")

    monkeypatch.setattr("app.leads.progress.register_progress_message", fail_registration)
    with pytest.raises(RuntimeError):
        MessageIntakeService(session_factory).receive(
            IncomingMessageCommand(
                message_id="progress-atomic-failure",
                sales_user_id="sales-1",
                raw_payload={"text": "客户需求"},
                normalized_text="客户需求",
            )
        )
    with session_factory() as session:
        assert session.get(IncomingMessage, "progress-atomic-failure") is None
        assert session.scalar(select(OutboxEvent)) is None
        assert session.scalar(select(LeadProgressSession)) is None


def test_submission_like_natural_language_enters_async_intent_classification(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证提交式文本先等待意图，确认需求后从原接收时间启动计时。"""
    authorize_salesperson(session_factory, "sales-1")
    settings = Settings(
        _env_file=None,
        lead_progress_enabled=True,
        lead_progress_interval_minutes=15,
        lead_progress_idle_stop_minutes=60,
    )
    monkeypatch.setattr(messaging_service, "get_settings", lambda: settings)

    MessageIntakeService(session_factory).receive(
        IncomingMessageCommand(
            message_id="message-submit-intent",
            sales_user_id="sales-1",
            raw_payload={"text": "请把我所有能提交的线索都提交一下"},
            normalized_text="请把我所有能提交的线索都提交一下",
        )
    )

    with session_factory() as session:
        event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "message-submit-intent")
        )
        message = session.get(IncomingMessage, "message-submit-intent")
        candidate = session.get(LeadProgressMessage, "message-submit-intent")
        assert session.scalar(select(LeadProgressSession)) is None

    assert event is not None
    assert event.event_type == "crm_submission_intent"
    assert message is not None and candidate is not None
    assert candidate.status == "awaiting_intent" and candidate.progress_session_id is None
    with session_factory.begin() as session:
        assert activate_progress_intent_candidate(session, message.message_id, settings)
    with session_factory() as session:
        candidate = session.get(LeadProgressMessage, "message-submit-intent")
        progress = session.scalar(select(LeadProgressSession))
    assert candidate is not None and candidate.status == "processing"
    assert progress is not None
    assert progress.started_at == candidate.received_at
    assert progress.next_report_at == candidate.received_at + timedelta(minutes=15)


def test_plain_resubmit_enters_intent_router_and_abandoned_phrase_keeps_priority(
    session_factory: sessionmaker[Session],
) -> None:
    """验证“重新提交”进入意图路由，放弃提交专用表达仍走确定性快路径。"""
    authorize_salesperson(session_factory, "sales-1")
    intake = MessageIntakeService(session_factory)
    intake.receive(
        IncomingMessageCommand(
            message_id="message-retry-intent",
            sales_user_id="sales-1",
            raw_payload={"text": "重新提交"},
            normalized_text="重新提交",
        )
    )
    intake.receive(
        IncomingMessageCommand(
            message_id="message-abandoned-fast-path",
            sales_user_id="sales-1",
            raw_payload={"text": "重新提交放弃提交的线索"},
            normalized_text="重新提交放弃提交的线索",
        )
    )

    with session_factory() as session:
        retry = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "message-retry-intent")
        )
        abandoned = session.scalar(
            select(OutboxEvent).where(
                OutboxEvent.message_id == "message-abandoned-fast-path"
            )
        )
        command_progress = session.get(LeadProgressMessage, "message-abandoned-fast-path")

    assert retry is not None and retry.event_type == "crm_submission_intent"
    assert abandoned is not None and abandoned.event_type == "crm_submission_command"
    assert command_progress is None
    assert tasks._submission_command_text(
        SubmissionIntent(intent="SUBMIT_RETRY_INCOMPLETE")
    ) == "重新提交待完善的线索"


def test_ordinary_text_skips_submission_intent_classifier_and_enters_lead_pipeline(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证普通线索文本不会被提交意图路由提前结束。"""
    authorize_salesperson(session_factory, "sales-1")
    MessageIntakeService(session_factory).receive(
        IncomingMessageCommand(
            message_id="message-follow-up",
            sales_user_id="sales-1",
            raw_payload={"text": "电话号码是 13861699726"},
            normalized_text="电话号码是 13861699726",
        )
    )
    with session_factory.begin() as session:
        event = session.scalar(
            select(OutboxEvent).where(OutboxEvent.message_id == "message-follow-up")
        )
        assert event is not None
        event.status = "processing"
        event.processing_started_at = datetime.now(UTC)
        outbox_event_id = event.id

    engine = session_factory.kw["bind"]
    assert engine is not None
    gateway = Mock()
    gateway.classify_submission_intent.side_effect = AssertionError(
        "普通线索文本不应调用提交意图分类"
    )
    consumed = Mock(return_value=SimpleNamespace(status=SimpleNamespace(value="succeeded")))
    service = Mock()
    service.consume = consumed
    monkeypatch.setattr(tasks, "_take_lead_outbox_claim", lambda *_args: True)
    monkeypatch.setattr(tasks, "get_smart_table_adapter", lambda: object())
    monkeypatch.setattr(tasks, "get_ai_gateway", lambda **_kwargs: gateway)
    monkeypatch.setattr(tasks, "get_media_attachment_service", lambda *_args: Mock())
    monkeypatch.setattr(tasks, "FirstTextLeadWorkspaceService", lambda *_args, **_kwargs: service)
    monkeypatch.setattr(tasks, "_session_factory", lambda: (engine, session_factory))

    result = tasks.consume_lead_outbox_event.run(outbox_event_id, datetime.now(UTC).isoformat())

    assert result == "succeeded"
    consumed.assert_called_once_with(outbox_event_id, claimed_for_processing=True)
    gateway.classify_submission_intent.assert_not_called()


@pytest.mark.parametrize(
    ("text", "intent"),
    [
        ("不要提交今天的线索", "SUBMIT_TODAY"),
        ("不用提交所有线索", "SUBMIT_ALL"),
        ("先别提交遨博这条线索", "SUBMIT_SINGLE"),
        ("先不要重新提交", "SUBMIT_RETRY_INCOMPLETE"),
        ("这些待完善线索可以重新提交吗？", "SUBMIT_RETRY_INCOMPLETE"),
    ],
)
def test_negative_submission_intent_never_enters_submission_workflow(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    text: str,
    intent: str,
) -> None:
    """验证模型即使误判为提交意图，原文否定表达仍不会发行动作或调用 CRM。"""
    authorize_salesperson(session_factory, "sales-1")
    claimed_at = datetime.now(UTC)
    with session_factory.begin() as session:
        message = IncomingMessage(
            message_id=f"negative-{intent}",
            sales_user_id="sales-1",
            sequence=1,
            raw_payload={"text": text},
            normalized_text=text,
        )
        session.add(message)
        event = OutboxEvent(
            message_id=message.message_id,
            sales_user_id="sales-1",
            sequence=1,
            event_type="crm_submission_intent",
            status="processing",
            processing_started_at=claimed_at,
        )
        session.add(event)
        session.flush()
        outbox_event_id = event.id

    fake_gateway = Mock()
    fake_gateway.classify_submission_intent.return_value = SubmissionIntent(intent=intent)
    consume_submission = Mock(side_effect=AssertionError("否定请求不应进入提交工作流"))
    get_crm = Mock(side_effect=AssertionError("否定请求不应构造 CRM 依赖"))
    engine = session_factory.kw["bind"]
    assert engine is not None
    monkeypatch.setattr(tasks, "_take_lead_outbox_claim", lambda *_args: True)
    monkeypatch.setattr(tasks, "get_smart_table_adapter", lambda: object())
    monkeypatch.setattr(tasks, "_is_submission_command", lambda *_args: False)
    monkeypatch.setattr(tasks, "_is_submission_intent", lambda *_args: True)
    monkeypatch.setattr(tasks, "get_ai_gateway", lambda: fake_gateway)
    monkeypatch.setattr(tasks, "consume_submission_command", consume_submission)
    monkeypatch.setattr(tasks, "get_crm_adapter", get_crm)
    monkeypatch.setattr(tasks, "_session_factory", lambda: (engine, session_factory))

    result = tasks.consume_lead_outbox_event.run(outbox_event_id, claimed_at.isoformat())

    assert result == "submission_intent_unrecognized"
    consume_submission.assert_not_called()
    get_crm.assert_not_called()


def test_duplicate_authorized_message_has_no_second_business_side_effect(
    session_factory: sessionmaker[Session],
) -> None:
    """验证相同消息标识重复到达时不会创建第二条消息或发件箱事件。"""
    authorize_salesperson(session_factory, "sales-1")
    service = MessageIntakeService(session_factory)
    command = IncomingMessageCommand(
        message_id="message-1",
        sales_user_id="sales-1",
        raw_payload={"text": "客户需要码垛机器人"},
        normalized_text="客户需要码垛机器人",
    )

    first_result = service.receive(command)
    duplicate_result = service.receive(command)

    assert first_result.accepted is True
    assert duplicate_result.accepted is True
    assert duplicate_result.duplicate is True
    with session_factory() as session:
        assert len(session.scalars(select(IncomingMessage)).all()) == 1
        assert len(session.scalars(select(OutboxEvent)).all()) == 1


def test_outbox_persistence_failure_rolls_back_the_raw_message(
    session_factory: sessionmaker[Session],
) -> None:
    """验证事务提交失败时不会留下已保存但没有 Outbox 的原始消息。"""
    authorize_salesperson(session_factory, "sales-1")

    with pytest.raises(StatementError):
        MessageIntakeService(session_factory).receive(
            IncomingMessageCommand(
                message_id="message-1",
                sales_user_id="sales-1",
                # JSON 序列化失败会在包含消息和 Outbox 的事务提交阶段触发回滚。
                raw_payload={"unsupported": object()},
            )
        )

    with session_factory() as session:
        assert session.scalars(select(IncomingMessage)).all() == []
        assert session.scalars(select(OutboxEvent)).all() == []
        assert session.scalars(select(NotificationRecord)).all() == []


def test_persisted_message_remains_idempotent_after_sales_authorization_changes(
    session_factory: sessionmaker[Session],
) -> None:
    """验证已接收消息重投不受后来停用影响，且接收提示不被重复统计。"""
    authorize_salesperson(session_factory, "sales-1")
    service = MessageIntakeService(session_factory)
    command = IncomingMessageCommand(
        message_id="message-1",
        sales_user_id="sales-1",
        raw_payload={"text": "客户需要码垛机器人"},
    )
    service.receive(command)
    with session_factory.begin() as session:
        authorization = session.get(SalesAuthorization, "sales-1")
        assert authorization is not None
        authorization.is_active = False

    result = service.receive(command)

    assert result == MessageIntakeResult(accepted=True, duplicate=True)
    with session_factory() as session:
        notices = session.scalars(select(NotificationRecord)).all()
    assert len(notices) == 1
    assert notices[0].notification_type == "lead_intake_receipt"
    assert notices[0].payload is not None
    assert notices[0].payload["receipt_count"] == 1


def test_authorized_messages_receive_monotonic_sales_sequence(
    session_factory: sessionmaker[Session],
) -> None:
    """验证同一销售的不同消息取得连续且可供 Worker 排序的持久化顺序号。"""
    authorize_salesperson(session_factory, "sales-1")
    service = MessageIntakeService(session_factory)

    service.receive(
        IncomingMessageCommand(
            message_id="message-1",
            sales_user_id="sales-1",
            raw_payload={"text": "第一条客户信息"},
        )
    )
    service.receive(
        IncomingMessageCommand(
            message_id="message-2",
            sales_user_id="sales-1",
            raw_payload={"text": "第二条客户信息"},
        )
    )

    with session_factory() as session:
        events = session.scalars(select(OutboxEvent).order_by(OutboxEvent.sequence)).all()

    assert [event.sequence for event in events] == [1, 2]


def test_receipt_coalesces_per_sales_and_duplicate_message_counts_once(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证接收提示按销售分隔合并，重复 message_id 不增加计数。"""
    monkeypatch.setattr(
        messaging_service,
        "get_settings",
        lambda: Settings(
            _env_file=None,
            lead_receipt_enabled=True,
            lead_receipt_coalesce_seconds=5,
        ),
    )
    authorize_salesperson(session_factory, "sales-a")
    authorize_salesperson(session_factory, "sales-b")
    intake = MessageIntakeService(session_factory)
    first = IncomingMessageCommand(
        message_id="receipt-a-1", sales_user_id="sales-a", raw_payload={"text": "客户A"}
    )

    intake.receive(first)
    intake.receive(first)
    intake.receive(
        IncomingMessageCommand(
            message_id="receipt-a-2", sales_user_id="sales-a", raw_payload={"text": "客户A补充"}
        )
    )
    intake.receive(
        IncomingMessageCommand(
            message_id="receipt-b-1", sales_user_id="sales-b", raw_payload={"text": "客户B"}
        )
    )

    with session_factory() as session:
        notices = session.scalars(
            select(NotificationRecord)
            .where(NotificationRecord.notification_type == "lead_intake_receipt")
            .order_by(NotificationRecord.sales_user_id)
        ).all()

    assert [(notice.sales_user_id, notice.payload["receipt_count"]) for notice in notices] == [
        ("sales-a", 2),
        ("sales-b", 1),
    ]
    assert notices[0].payload["message_ids"] == ["receipt-a-1", "receipt-a-2"]
    assert notices[1].payload["message_ids"] == ["receipt-b-1"]
    assert notices[0].content == "✅ 已收到你的 2 条消息，正在识别并录入。"


def test_submission_and_action_commands_do_not_create_lead_receipts(
    session_factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    """验证 CRM 提交命令和确定性确认动作不触发线索录入中提示。"""
    monkeypatch.setattr(
        messaging_service,
        "get_settings",
        lambda: Settings(
            _env_file=None,
            lead_receipt_enabled=True,
            lead_receipt_coalesce_seconds=5,
        ),
    )
    authorize_salesperson(session_factory, "sales-1")
    intake = MessageIntakeService(session_factory)
    intake.receive(
        IncomingMessageCommand(
            message_id="receipt-submit",
            sales_user_id="sales-1",
            raw_payload={"text": "提交今天的线索"},
            normalized_text="提交今天的线索",
        )
    )
    intake.receive(
        IncomingMessageCommand(
            message_id="receipt-action",
            sales_user_id="sales-1",
            raw_payload={"text": "t18.discard:lead-1"},
            normalized_text="t18.discard:lead-1",
        )
    )

    with session_factory() as session:
        event_types = session.scalars(select(OutboxEvent.event_type).order_by(OutboxEvent.id)).all()
        receipts = session.scalars(
            select(NotificationRecord).where(
                NotificationRecord.notification_type == "lead_intake_receipt"
            )
        ).all()

    assert event_types == ["crm_submission_command", "wecom_action_command"]
    assert receipts == []


def test_new_actor_is_registered_and_message_enters_processing_work(
    session_factory: sessionmaker[Session],
) -> None:
    """验证新 WeCom actor 自动注册、从 sequence 1 开始且不依赖 is_authorized。"""
    service = MessageIntakeService(session_factory)
    command = IncomingMessageCommand(
        message_id="message-1",
        sales_user_id="visitor-1",
        raw_payload={"text": "这是测试消息"},
        normalized_text="这是测试消息",
        display_name="测试成员",
    )

    first_result = service.receive(command)
    duplicate_result = service.receive(command)

    assert first_result.accepted is True
    assert first_result.duplicate is False
    assert duplicate_result.duplicate is True
    with session_factory() as session:
        actor = session.get(SalesAuthorization, "visitor-1")
        messages = session.scalars(select(IncomingMessage)).all()
        outbox = session.scalars(select(OutboxEvent)).all()
        notices = session.scalars(select(NotificationRecord)).all()

    assert actor is not None
    assert actor.display_name == "测试成员"
    assert actor.is_authorized is False
    assert actor.is_active is True
    assert actor.next_message_sequence == 1
    assert len(messages) == 1 and messages[0].sequence == 1
    assert len(outbox) == 1 and outbox[0].sequence == 1
    assert len(notices) == 1
    assert notices[0].notification_type == "lead_intake_receipt"


def test_inactive_actor_is_rejected_without_processing_work(
    session_factory: sessionmaker[Session],
) -> None:
    """验证管理员停用 actor 后消息 fail closed，且通知仍保持幂等。"""
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="inactive-1", is_authorized=False, is_active=False
            )
        )
    service = MessageIntakeService(session_factory)
    command = IncomingMessageCommand(
        message_id="inactive-message",
        sales_user_id="inactive-1",
        raw_payload={"text": "不应进入处理"},
    )

    first_result = service.receive(command)
    duplicate_result = service.receive(command)

    assert first_result.accepted is False
    assert duplicate_result.duplicate is True
    with session_factory() as session:
        assert session.scalars(select(IncomingMessage)).all() == []
        assert session.scalars(select(OutboxEvent)).all() == []
        notices = session.scalars(select(NotificationRecord)).all()
    assert len(notices) == 1
    assert notices[0].notification_type == "sales_actor_inactive"
