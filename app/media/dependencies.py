"""媒体处理服务的运行时依赖组装。"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.media.providers import (
    MockASRProvider,
    MockOCRProvider,
    NoopFileScanProvider,
    QwenASRProvider,
    QwenOCRProvider,
)
from app.media.service import MediaAttachmentService, MediaValidator
from app.media.storage import LocalVolumeStorageProvider


def get_media_attachment_service(
    session_factory: sessionmaker[Session],
) -> MediaAttachmentService:
    """依据环境配置组装开发 Volume 与可替换 Qwen/Mock 媒体服务。"""
    settings = get_settings()
    api_key = settings.qwen_api_key or ""
    # Mock 仅用于测试或未接入凭据的本地开发，生产应由就绪检查另行约束。
    ocr = (
        MockOCRProvider([])
        if settings.ocr_provider == "mock"
        else QwenOCRProvider(
            api_key=api_key, base_url=settings.qwen_base_url, model=settings.qwen_ocr_model
        )
    )
    asr = (
        MockASRProvider([])
        if settings.asr_provider == "mock"
        else QwenASRProvider(
            api_key=api_key, base_url=settings.qwen_base_url, model=settings.qwen_asr_model
        )
    )
    return MediaAttachmentService(
        session_factory,
        MediaValidator(
            image_mime_types=settings.media_image_mime_types,
            audio_mime_types=settings.media_audio_mime_types,
            max_image_bytes=settings.media_max_image_bytes,
            max_audio_bytes=settings.media_max_audio_bytes,
        ),
        LocalVolumeStorageProvider(Path(settings.media_storage_path)),
        NoopFileScanProvider(),
        ocr,
        asr,
        timeout_seconds=settings.media_processing_timeout_seconds,
    )
