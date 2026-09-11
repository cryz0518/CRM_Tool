"""将企业微信 AI 机器人消息转换为可靠消息接收命令。"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Literal

from app.messaging.service import IncomingMessageCommand, MessageIntakeResult, MessageIntakeService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MediaFrameReceipt:
    """保存已持久化媒体消息及仅在内存使用的 SDK 下载信息。"""

    result: MessageIntakeResult
    message_id: str
    media_kind: Literal["image", "audio"]
    download_url: str | None
    aes_key: str | None
    declared_mime_type: str | None


class WecomTextMessageAdapter:
    """使用官方文本帧字段把企业微信消息交给 T02 的可靠接收边界。"""

    def __init__(self, message_intake_service: MessageIntakeService) -> None:
        """保存可靠消息接收服务。

        参数：message_intake_service 为 T02 提供的事务性消息接收边界。
        返回值：无。
        异常：无。
        副作用：不读取 SDK、环境变量或数据库。
        """
        self._message_intake_service = message_intake_service

    def receive_text_frame(self, frame: dict[str, Any]) -> MessageIntakeResult | None:
        """校验官方文本帧后以原样载荷转交 T02。

        参数：frame 为 SDK ``message.text`` 事件传入的完整 WebSocket 帧。
        返回值：有效消息返回 T02 接收结果；身份字段缺失或非文本帧时返回 None。
        异常：T02 的数据库异常向上抛出，确保调用者不会把未落库消息当作成功。
        副作用：有效消息会在 T02 事务中写入原始消息与 Outbox 事件。
        """
        # SDK 仅将原始 dict 透传；先确认 body 形状，绝不猜测消息身份字段。
        body = frame.get("body")
        if not isinstance(body, dict) or body.get("msgtype") != "text":
            logger.warning("wecom_bot_invalid_text_frame")
            return None

        # 官方协议以 body.msgid 作为幂等键，并以 body.from.userid 标识发送者。
        message_id = body.get("msgid")
        sender = body.get("from")
        sales_user_id = sender.get("userid") if isinstance(sender, dict) else None
        if (
            not isinstance(message_id, str)
            or not message_id
            or not isinstance(sales_user_id, str)
            or not sales_user_id
        ):
            logger.warning("wecom_bot_text_frame_missing_identity")
            return None

        # 文本正文缺失不伪造内容；T02 仍保存原始可审计帧，供后续人工排查。
        text = body.get("text")
        normalized_text = text.get("content") if isinstance(text, dict) else None
        if not isinstance(normalized_text, str):
            normalized_text = None

        return self._message_intake_service.receive(
            IncomingMessageCommand(
                message_id=message_id,
                sales_user_id=sales_user_id,
                raw_payload=frame,
                normalized_text=normalized_text,
            )
        )


class WecomMediaMessageAdapter:
    """将图片或语音帧可靠落库，并提取不持久化的 SDK 下载参数。"""

    def __init__(self, message_intake_service: MessageIntakeService) -> None:
        """保存与文本共享的可靠消息接收边界。"""
        self._message_intake_service = message_intake_service

    def receive_media_frame(self, frame: dict[str, Any]) -> MediaFrameReceipt | None:
        """接收官方图片或语音帧；无身份字段时拒绝，媒体下载稍后异步完成。"""
        body = frame.get("body")
        if not isinstance(body, dict) or body.get("msgtype") not in {"image", "voice"}:
            logger.warning("wecom_bot_invalid_media_frame")
            return None
        message_id = body.get("msgid")
        sender = body.get("from")
        sales_user_id = sender.get("userid") if isinstance(sender, dict) else None
        media_kind = body["msgtype"]
        media = body.get(media_kind)
        if not isinstance(message_id, str) or not isinstance(sales_user_id, str):
            logger.warning("wecom_bot_media_frame_missing_identity")
            return None
        media = media if isinstance(media, dict) else {}
        download_url = media.get("url")
        aes_key = media.get("aeskey")
        declared_mime_type = media.get("mime_type")
        return MediaFrameReceipt(
            result=self._message_intake_service.receive(
                IncomingMessageCommand(
                    message_id=message_id,
                    sales_user_id=sales_user_id,
                    raw_payload=frame,
                )
            ),
            message_id=message_id,
            media_kind="audio" if media_kind == "voice" else "image",
            download_url=download_url if isinstance(download_url, str) else None,
            aes_key=aes_key if isinstance(aes_key, str) else None,
            declared_mime_type=(
                declared_mime_type if isinstance(declared_mime_type, str) else None
            ),
        )
