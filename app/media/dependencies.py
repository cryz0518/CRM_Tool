"""媒体处理服务的显式 provider 选择和运行时依赖组装。"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings, get_settings
from app.media.providers import (
    FakeFileScanProvider,
    FileScanProvider,
    MockASRProvider,
    MockOCRProvider,
    NoopFileScanProvider,
    QwenASRProvider,
    QwenOCRProvider,
    UnconfiguredFileScanProvider,
)
from app.media.service import MediaAttachmentService, MediaValidator
from app.media.storage import (
    FakeStorageProvider,
    LocalVolumeStorageProvider,
    SignedURLProvider,
    StorageProvider,
)


def get_media_storage_provider(settings: Settings | None = None) -> StorageProvider:
    """按显式配置返回 local/fake provider，生产未安装 vendor Adapter 时 fail closed。"""
    selected = settings or get_settings()
    if selected.media_storage_provider == "local":
        return LocalVolumeStorageProvider(Path(selected.media_storage_path))
    if selected.media_storage_provider == "fake":
        return FakeStorageProvider(Path(selected.media_storage_path))
    raise RuntimeError("MEDIA_STORAGE_PROVIDER 未配置可用的 provider")


def get_media_storage_signer(
    settings: Settings | None = None,
) -> SignedURLProvider | None:
    """只返回显式具备签名能力的开发/fake provider，不伪造生产签名 Adapter。"""
    selected = settings or get_settings()
    try:
        provider = get_media_storage_provider(selected)
    except RuntimeError:
        return None
    if not provider.capabilities.signed_url or not hasattr(provider, "create_signed_url"):
        return None
    return provider  # type: ignore[return-value]


def get_media_scanner(settings: Settings | None = None) -> FileScanProvider:
    """按显式配置选择 Noop、Fake 或 fail-closed scanner。"""
    selected = settings or get_settings()
    if (
        selected.app_env in {"production", "prod"}
        and selected.media_scanner_provider != "production"
    ):
        raise RuntimeError("生产环境禁止使用未配置的媒体扫描 provider")
    if selected.media_scanner_provider == "noop":
        return NoopFileScanProvider()
    if selected.media_scanner_provider == "fake":
        return FakeFileScanProvider("clean")
    if selected.media_scanner_provider == "production":
        raise RuntimeError("生产文件扫描 Adapter 尚未安装")
    return UnconfiguredFileScanProvider()


def get_media_attachment_service(
    session_factory: sessionmaker[Session],
) -> MediaAttachmentService:
    """依据显式 provider 配置组装媒体服务，禁止生产静默回退到本地 Volume。"""
    settings = get_settings()
    api_key = settings.qwen_api_key or ""
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
        get_media_storage_provider(settings),
        get_media_scanner(settings),
        ocr,
        asr,
        timeout_seconds=settings.media_processing_timeout_seconds,
    )
