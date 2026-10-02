"""可靠消息接收应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ai.models import SubmissionIntent
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
    session_factory: sessionmaker[Session],
) -> None:
    """验证授权销售的消息在一次接收后同时形成原始消息和待处理事件。"""
    authorize_salesperson(session_factory, "sales-1")
    result = MessageIntakeService(session_factory).receive(
        IncomingMessageCommand(
            message_id="message-1",
            sales_user_id="sales-1",
            raw_payload={"text": "客户需要码垛机器人"},
            normalized_text="客户需要码垛机器人",
        )
    )

    assert result.accepted is True
    assert result.duplicate is False
    with session_factory() as session:
        message = session.scalar(
            select(IncomingMessage).where(IncomingMessage.message_id == "message-1")
        )
        event = session.scalar(select(OutboxEvent).where(OutboxEvent.message_id == "message-1"))
        audit = session.scalar(
            select(BusinessAuditEvent).where(BusinessAuditEvent.event_type == "message_received")
        )

    assert message is not None
    assert message.sequence == 1
    assert event is not None
    assert event.status == "pending"
    assert event.sequence == message.sequence
    assert audit is not None


def test_submission_like_natural_language_enters_async_intent_classification(
    session_factory: sessionmaker[Session],
) -> None:
    """验证非固定命令先进入模型意图分类，不会被普通线索处理静默忽略。"""
    authorize_salesperson(session_factory, "sales-1")

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

    assert event is not None
    assert event.event_type == "crm_submission_intent"


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

    assert retry is not None and retry.event_type == "crm_submission_intent"
    assert abandoned is not None and abandoned.event_type == "crm_submission_command"
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


def test_persisted_message_remains_idempotent_after_sales_authorization_changes(
    session_factory: sessionmaker[Session],
) -> None:
    """验证已接收消息的重投不受销售后来停用影响，也不会新增权限通知。"""
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
        assert session.scalars(select(NotificationRecord)).all() == []


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


def test_unauthorized_message_creates_no_processing_work_and_one_notice(
    session_factory: sessionmaker[Session],
) -> None:
    """验证未授权成员不进入处理链路，重复发送只保留一条权限不足提示记录。"""
    service = MessageIntakeService(session_factory)
    command = IncomingMessageCommand(
        message_id="message-1",
        sales_user_id="visitor-1",
        raw_payload={"text": "这是测试消息"},
        normalized_text="这是测试消息",
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
    assert notices[0].notification_type == "sales_authorization_denied"
    assert notices[0].attempts == 0
    assert notices[0].provider_message_id is None
    assert notices[0].sent_at is None
