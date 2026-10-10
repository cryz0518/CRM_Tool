"""企业微信 AI 机器人文本消息接入测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.messaging.models import Base, IncomingMessage, OutboxEvent, SalesAuthorization
from app.messaging.service import MessageIntakeService
from app.wecom_bot.adapter import WecomMediaMessageAdapter, WecomTextMessageAdapter
from app.wecom_bot.runner import WecomBotRuntime


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
        [event] = session.scalars(select(OutboxEvent)).all()
        assert event.event_type == "message_received"


def test_submission_command_is_persisted_as_a_command_outbox_event(
    session_factory: sessionmaker[Session],
) -> None:
    """验证精确提交命令只进入可靠命令 Outbox，不在接入层调用 CRM。"""
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
    frame = {
        "body": {
            "msgid": "command-1",
            "from": {"userid": "sales-1"},
            "msgtype": "text",
            "text": {"content": "提交今天的线索"},
        }
    }

    result = WecomTextMessageAdapter(MessageIntakeService(session_factory)).receive_text_frame(
        frame
    )

    assert result is not None and result.accepted is True
    with session_factory() as session:
        [event] = session.scalars(select(OutboxEvent)).all()
        assert event.event_type == "crm_submission_command"


def test_company_submission_request_is_persisted_as_a_command_outbox_event(
    session_factory: sessionmaker[Session],
) -> None:
    """验证按公司名称提交请求进入同一可靠命令边界，不在接入层调用 CRM。"""

    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True, is_active=True))
    frame = {
        "body": {
            "msgid": "company-command-1",
            "from": {"userid": "sales-1"},
            "msgtype": "text",
            "text": {"content": "请帮我提交上海世界纵横智能科技有限公司这条线索"},
        }
    }

    result = WecomTextMessageAdapter(MessageIntakeService(session_factory)).receive_text_frame(
        frame
    )

    assert result is not None and result.accepted is True
    with session_factory() as session:
        [event] = session.scalars(select(OutboxEvent)).all()
        assert event.event_type == "crm_submission_command"


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


@pytest.mark.parametrize("with_audio", [False, True])
def test_voice_content_is_used_once_without_media_wait(
    session_factory: sessionmaker[Session], with_audio: bool
) -> None:
    """验证官方 voice.content 单一落库、重复投递幂等，双来源优先转写且保留证据。"""
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(wecom_user_id="sales-1", is_authorized=False, is_active=True)
        )
    voice = {"content": "客户：测试语音公司，需要码垛机器人"}
    if with_audio:
        voice.update(url="https://temporary.example/audio", aeskey="secret")
    frame = {
        "cmd": "aibot_msg_callback",
        "body": {
            "msgid": "voice-text",
            "from": {"userid": "sales-1"},
            "msgtype": "voice",
            "voice": voice,
            "quote": {"msgid": "quoted-message"},
        },
    }
    adapter = WecomMediaMessageAdapter(MessageIntakeService(session_factory))
    first = adapter.receive_media_frame(frame)
    second = adapter.receive_media_frame(frame)
    assert first is not None and first.result.accepted
    assert second is not None and second.result.duplicate
    with session_factory() as session:
        message = session.get(IncomingMessage, "voice-text")
        assert message is not None
        assert message.normalized_text == voice["content"]
        assert message.requires_media_enrichment is False
        assert message.raw_payload["body"]["voice"]["content"] == voice["content"]
        assert message.raw_payload["body"]["quote"]["msgid"] == "quoted-message"
        assert len(session.scalars(select(OutboxEvent)).all()) == 1


@pytest.mark.parametrize(
    "content, with_audio", [("可信语音正文", False), ("可信语音正文", True), ("", True)]
)
def test_voice_runtime_selects_one_source_and_duplicate_delivery_is_noop(
    session_factory: sessionmaker[Session],
    content: str,
    with_audio: bool,
) -> None:
    """验证真实运行时文本跳过下载、纯音频进入 ASR 工件路径，重复投递不产生第二任务。"""
    with session_factory.begin() as session:
        session.add(
            SalesAuthorization(wecom_user_id="sales-1", is_authorized=False, is_active=True)
        )
    voice = {"content": content}
    if with_audio:
        voice["url"] = "https://temporary.example/audio"
    frame = {
        "body": {
            "msgid": "runtime-voice",
            "from": {"userid": "sales-1"},
            "msgtype": "voice",
            "voice": voice,
        }
    }
    # 不创建或连接 SDK 客户端，仅驱动正式运行时方法及真实消息接收事务。
    runtime = object.__new__(WecomBotRuntime)
    runtime._media_adapter = WecomMediaMessageAdapter(MessageIntakeService(session_factory))
    runtime._media_attachment_service = Mock()
    runtime._client = Mock(download_file=AsyncMock(return_value=(b"ID3audio", None)))
    asyncio.run(runtime._receive_media_frame(frame))
    asyncio.run(runtime._receive_media_frame(frame))
    expected = 0 if content else 1
    assert runtime._client.download_file.await_count == expected
    assert runtime._media_attachment_service.ingest.call_count == expected
    runtime._media_attachment_service.record_download_failure.assert_not_called()


def test_inactive_voice_sender_cannot_create_media_work(
    session_factory: sessionmaker[Session],
) -> None:
    """验证语音转写不会绕过停用成员校验，拒绝时不创建来源消息或工件。"""
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="inactive", is_active=False))
    frame = {
        "body": {
            "msgid": "inactive-voice",
            "from": {"userid": "inactive"},
            "msgtype": "voice",
            "voice": {"content": "客户：无权公司"},
        }
    }
    result = WecomMediaMessageAdapter(MessageIntakeService(session_factory)).receive_media_frame(
        frame
    )
    assert result is not None and not result.result.accepted
    with session_factory() as session:
        assert session.get(IncomingMessage, "inactive-voice") is None
