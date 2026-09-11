"""企业微信 AI 机器人文本消息接入测试。"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.messaging.models import Base, IncomingMessage, OutboxEvent, SalesAuthorization
from app.messaging.service import MessageIntakeService
from app.wecom_bot.adapter import WecomMediaMessageAdapter, WecomTextMessageAdapter


@pytest.fixture
def session_factory() -> Generator[sessionmaker[Session], None, None]:
    """提供隔离的事务数据库。

    参数：无。
    返回值：返回可创建 SQLAlchemy 会话的工厂。
    异常：建表或会话创建失败时向 pytest 传播。
    副作用：创建并在测试结束后销毁内存数据库表。
    """
    # 共享内存 SQLite 让服务和断言可以在不同会话中观察同一笔事务结果。
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


def test_text_frame_is_forwarded_to_reliable_message_intake(
    session_factory: sessionmaker[Session],
) -> None:
    """验证官方文本帧进入 T02 事务边界。

    参数：session_factory 为隔离数据库会话工厂。
    返回值：无。
    异常：断言失败会由 pytest 报告。
    副作用：写入授权销售、原始消息和 Outbox 事件。
    """
    with session_factory.begin() as session:
        # 先登记发送者，确保断言覆盖授权销售的正式接收路径。
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))

    frame = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "request-1"},
        "body": {
            "msgid": "message-1",
            "from": {"userid": "sales-1"},
            "msgtype": "text",
            "text": {"content": "客户需要码垛机器人"},
        },
    }

    adapter = WecomTextMessageAdapter(MessageIntakeService(session_factory))
    result = adapter.receive_text_frame(frame)

    assert result is not None
    assert result.accepted is True
    with session_factory() as session:
        message = session.get(IncomingMessage, "message-1")
        assert message is not None
        assert message.sales_user_id == "sales-1"
        assert message.normalized_text == "客户需要码垛机器人"
        assert message.raw_payload == frame
        assert len(session.scalars(select(OutboxEvent)).all()) == 1


def test_text_frame_without_official_identity_fields_is_not_forwarded(
    session_factory: sessionmaker[Session],
) -> None:
    """验证缺少官方身份字段时拒绝转发。

    参数：session_factory 为隔离数据库会话工厂。
    返回值：无。
    异常：断言失败会由 pytest 报告。
    副作用：读取数据库以确认没有创建消息或 Outbox 事件。
    """
    frame = {"body": {"msgtype": "text", "text": {"content": "测试消息"}}}

    adapter = WecomTextMessageAdapter(MessageIntakeService(session_factory))
    result = adapter.receive_text_frame(frame)

    assert result is None
    with session_factory() as session:
        assert session.scalars(select(IncomingMessage)).all() == []
        assert session.scalars(select(OutboxEvent)).all() == []


def test_media_frame_persists_auditable_payload_without_download_credentials(
    session_factory: sessionmaker[Session],
) -> None:
    """验证媒体 URL 和 AES key 仅留在内存收据，绝不写入原始消息载荷。"""
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
    frame = {
        "body": {
            "msgid": "image-1",
            "from": {"userid": "sales-1"},
            "msgtype": "image",
            "image": {
                "url": "https://temporary.example/download",
                "aeskey": "secret",
                "mime_type": "image/png",
            },
        }
    }

    receipt = WecomMediaMessageAdapter(MessageIntakeService(session_factory)).receive_media_frame(
        frame
    )

    assert receipt is not None
    assert receipt.download_url == "https://temporary.example/download"
    with session_factory() as session:
        message = session.get(IncomingMessage, "image-1")
        assert message is not None
        assert message.requires_media_enrichment is True
        assert "url" not in message.raw_payload["body"]["image"]
        assert "aeskey" not in message.raw_payload["body"]["image"]
