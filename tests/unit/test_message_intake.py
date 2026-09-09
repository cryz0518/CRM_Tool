"""可靠消息接收应用服务测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.messaging.models import (
    Base,
    BusinessAuditEvent,
    IncomingMessage,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
)
from app.messaging.service import IncomingMessageCommand, MessageIntakeResult, MessageIntakeService


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
