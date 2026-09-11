"""媒体安全校验的确定性业务规则。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.media.providers import ASRProvider, FileScanProvider, OCRProvider
from app.media.storage import StorageProvider
from app.messaging.models import (
    BusinessAuditEvent,
    IncomingMessage,
    MediaProcessingTask,
    MessageAttachment,
    NotificationRecord,
    utc_now,
)


class MediaValidationError(ValueError):
    """表示媒体未通过白名单、大小或真实格式校验。"""


@dataclass(frozen=True)
class ValidatedMedia:
    """保存校验完成后可供存储和识别使用的安全媒体事实。"""

    detected_mime_type: str
    sha256: str
    size_bytes: int


class MediaValidator:
    """在媒体持久化和模型调用前执行统一安全校验。"""

    def __init__(
        self,
        *,
        image_mime_types: tuple[str, ...],
        audio_mime_types: tuple[str, ...],
        max_image_bytes: int = 10 * 1024 * 1024,
        max_audio_bytes: int = 20 * 1024 * 1024,
    ) -> None:
        """保存配置化白名单与大小上限。"""
        self._image_mime_types = frozenset(image_mime_types)
        self._audio_mime_types = frozenset(audio_mime_types)
        self._max_image_bytes = max_image_bytes
        self._max_audio_bytes = max_audio_bytes

    def validate(
        self,
        content: bytes,
        *,
        declared_mime_type: str | None,
        media_kind: Literal["image", "audio"],
    ) -> ValidatedMedia:
        """校验实际格式、声明类型、白名单和大小，并返回 SHA-256。"""
        if not content:
            raise MediaValidationError("media_empty")
        max_bytes = self._max_image_bytes if media_kind == "image" else self._max_audio_bytes
        if len(content) > max_bytes:
            raise MediaValidationError("media_too_large")
        detected = self._detect_mime_type(content)
        if detected is None:
            raise MediaValidationError("media_format_unsupported")
        if declared_mime_type is not None and declared_mime_type.lower() != detected:
            raise MediaValidationError("mime_type_mismatch")
        permitted = self._image_mime_types if media_kind == "image" else self._audio_mime_types
        if detected not in permitted:
            raise MediaValidationError("media_mime_not_allowed")
        return ValidatedMedia(
            detected_mime_type=detected,
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
        )

    @staticmethod
    def _detect_mime_type(content: bytes) -> str | None:
        """通过稳定文件签名识别首期支持的图片与音频格式。"""
        if content.startswith(b"\x89PNG\r\n\x1a\n"):
            return "image/png"
        if content.startswith(b"\xff\xd8\xff"):
            return "image/jpeg"
        if content.startswith(b"RIFF") and content[8:12] == b"WEBP":
            return "image/webp"
        if content.startswith(b"RIFF") and content[8:12] == b"WAVE":
            return "audio/wav"
        if content.startswith(b"ID3") or content.startswith(
            (b"\xff\xfb", b"\xff\xf3", b"\xff\xf2")
        ):
            return "audio/mpeg"
        if len(content) >= 12 and content[4:8] == b"ftyp":
            return "audio/mp4"
        return None


class MediaAttachmentService:
    """持久化已校验媒体，并以独立任务将 OCR/ASR 文本补充到来源消息。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        validator: MediaValidator,
        storage: StorageProvider,
        scanner: FileScanProvider,
        ocr_provider: OCRProvider,
        asr_provider: ASRProvider,
        *,
        timeout_seconds: float = 20.0,
    ) -> None:
        """保存所有可替换边界，构造时不读写数据库或媒体。"""
        self._session_factory = session_factory
        self._validator = validator
        self._storage = storage
        self._scanner = scanner
        self._ocr_provider = ocr_provider
        self._asr_provider = asr_provider
        self._timeout_seconds = timeout_seconds

    def ingest(
        self,
        message_id: str,
        content: bytes,
        *,
        media_kind: Literal["image", "audio"],
        declared_mime_type: str | None,
    ) -> str:
        """校验并保存附件；失败仅登记媒体任务，不删除来源消息。"""
        try:
            validated = self._validator.validate(
                content, declared_mime_type=declared_mime_type, media_kind=media_kind
            )
            scan_status = self._scanner.scan(
                content,
                mime_type=validated.detected_mime_type,
                timeout_seconds=self._timeout_seconds,
            )
            if scan_status not in {"clean", "not_required"}:
                raise MediaValidationError(f"scan_{scan_status}")
        except (MediaValidationError, RuntimeError) as error:
            return self._record_ingest_failure(message_id, media_kind, declared_mime_type, error)

        # 存储成功后才写入可处理元数据；失败不保存部分键，也不暴露内容到日志。
        stored = self._storage.put(content, suffix=self._suffix_for(validated.detected_mime_type))
        attachment_id = str(uuid4())
        with self._session_factory.begin() as session:
            session.add(
                MessageAttachment(
                    id=attachment_id,
                    message_id=message_id,
                    media_kind=media_kind,
                    declared_mime_type=declared_mime_type,
                    detected_mime_type=validated.detected_mime_type,
                    size_bytes=validated.size_bytes,
                    sha256=validated.sha256,
                    storage_key=stored.storage_key,
                    scan_status=scan_status,
                )
            )
            session.add(MediaProcessingTask(attachment_id=attachment_id, task_type=media_kind))
            self._audit(session, message_id, "media_attachment_stored")
        return attachment_id

    def process_pending_for_message(self, message_id: str) -> None:
        """处理来源消息的待执行附件，并将成功文本追加到标准化消息。"""
        with self._session_factory() as session:
            attachments = session.scalars(
                select(MessageAttachment).where(
                    MessageAttachment.message_id == message_id,
                    MessageAttachment.processing_status == "pending",
                )
            ).all()
        for attachment in attachments:
            self._process_attachment(attachment.id)

    def record_download_failure(self, message_id: str, *, media_kind: str) -> None:
        """保存下载失败的独立媒体任务，并要求销售以文字补充。"""
        self._record_ingest_failure(
            message_id,
            media_kind,
            None,
            MediaValidationError("media_download_failed"),
        )

    def _process_attachment(self, attachment_id: str) -> None:
        """执行一次独立 OCR 或 ASR，失败转人工补充而不抛出阻断消息队列。"""
        with self._session_factory() as session:
            attachment = session.get(MessageAttachment, attachment_id)
            if (
                attachment is None
                or attachment.storage_key is None
                or attachment.detected_mime_type is None
            ):
                return
            content = self._storage.get(attachment.storage_key)
            media_kind = attachment.media_kind
            mime_type = attachment.detected_mime_type
        try:
            text = (
                self._ocr_provider.recognize(
                    content, mime_type=mime_type, timeout_seconds=self._timeout_seconds
                )
                if media_kind == "image"
                else self._asr_provider.transcribe(
                    content, mime_type=mime_type, timeout_seconds=self._timeout_seconds
                )
            )
        except (RuntimeError, OSError) as error:
            self._record_processing_failure(attachment_id, type(error).__name__)
            return
        with self._session_factory.begin() as session:
            attachment = session.get(MessageAttachment, attachment_id)
            if attachment is None:
                return
            task = session.scalar(
                select(MediaProcessingTask).where(
                    MediaProcessingTask.attachment_id == attachment_id
                )
            )
            message = session.get(IncomingMessage, attachment.message_id)
            if task is None or message is None:
                return
            # 只把当前工件识别文本追加到本消息，后续现有字段提取流程仍负责业务判断。
            message.normalized_text = (
                "\n".join(part for part in (message.normalized_text, text.strip()) if part) or None
            )
            attachment.recognized_text = text
            attachment.processing_status = "succeeded"
            attachment.completed_at = utc_now()
            task.status = "succeeded"
            task.attempts += 1
            task.completed_at = utc_now()
            self._audit(session, attachment.message_id, "media_recognition_succeeded")

    def _record_ingest_failure(
        self, message_id: str, media_kind: str, declared_mime_type: str | None, error: Exception
    ) -> str:
        """记录校验或扫描失败工件，保证来源消息可审计且不进入模型。"""
        attachment_id = str(uuid4())
        with self._session_factory.begin() as session:
            session.add(
                MessageAttachment(
                    id=attachment_id,
                    message_id=message_id,
                    media_kind=media_kind,
                    declared_mime_type=declared_mime_type,
                    scan_status="failed",
                    processing_status="failed_pending_review",
                    error_summary=str(error)[:128],
                    completed_at=utc_now(),
                )
            )
            session.add(
                MediaProcessingTask(
                    attachment_id=attachment_id,
                    task_type=media_kind,
                    status="failed_pending_review",
                    attempts=1,
                    error_summary=str(error)[:128],
                    completed_at=utc_now(),
                )
            )
            self._audit(session, message_id, "media_validation_failed")
            self._notice(session, message_id)
        return attachment_id

    def _record_processing_failure(self, attachment_id: str, summary: str) -> None:
        """登记识别失败和幂等文字补充提示，不向调用方传播媒体失败。"""
        with self._session_factory.begin() as session:
            attachment = session.get(MessageAttachment, attachment_id)
            if attachment is None:
                return
            task = session.scalar(
                select(MediaProcessingTask).where(
                    MediaProcessingTask.attachment_id == attachment_id
                )
            )
            if task is None:
                return
            attachment.processing_status = "failed_pending_review"
            attachment.error_summary = summary
            attachment.completed_at = utc_now()
            task.status = "failed_pending_review"
            task.attempts += 1
            task.error_summary = summary
            task.completed_at = utc_now()
            self._audit(session, attachment.message_id, "media_recognition_failed")
            self._notice(session, attachment.message_id)

    @staticmethod
    def _suffix_for(mime_type: str) -> str:
        """为本地 Volume 生成不含业务信息的受控文件后缀。"""
        return {
            "image/png": ".png",
            "image/jpeg": ".jpg",
            "image/webp": ".webp",
            "audio/wav": ".wav",
            "audio/mpeg": ".mp3",
            "audio/mp4": ".m4a",
        }.get(mime_type, "")

    @staticmethod
    def _audit(session: Session, message_id: str, event_type: str) -> None:
        """记录媒体安全审计事件，不写入媒体内容、URL 或联系方式。"""
        if (
            session.scalar(
                select(BusinessAuditEvent).where(
                    BusinessAuditEvent.message_id == message_id,
                    BusinessAuditEvent.event_type == event_type,
                )
            )
            is None
        ):
            message = session.get(IncomingMessage, message_id)
            if message is not None:
                session.add(
                    BusinessAuditEvent(
                        message_id=message_id,
                        sales_user_id=message.sales_user_id,
                        event_type=event_type,
                    )
                )

    @staticmethod
    def _notice(session: Session, message_id: str) -> None:
        """为同一消息仅登记一次请销售文字补充的通知。"""
        message = session.get(IncomingMessage, message_id)
        if message is None:
            return
        key = hashlib.sha256(
            f"{message.sales_user_id}:{message_id}:media_text_input_required".encode()
        ).hexdigest()
        if session.get(NotificationRecord, key) is None:
            session.add(
                NotificationRecord(
                    notification_key=key,
                    sales_user_id=message.sales_user_id,
                    source_message_id=message_id,
                    notification_type="media_text_input_required",
                )
            )
