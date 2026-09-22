"""媒体处理服务的显式 provider 选择和运行时依赖组装。"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings, get_settings
from app.core.provider_policy import ProviderPolicyError, get_provider_policy
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
from app.media.retention import RetentionPolicy
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
    try:
        get_provider_policy(selected).require(
            "storage", selected.media_storage_provider, settings=selected
        )
    except ProviderPolicyError as error:
        # 保留 T22 对生产错误的可识别文本；具体原因仍由统一 readiness policy 输出。
        raise RuntimeError("生产环境禁止使用 local/fake/unconfigured 媒体存储 provider") from error
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
    provider = get_media_storage_provider(selected)
    if not provider.capabilities.signed_url or not hasattr(provider, "create_signed_url"):
        return None
    return provider  # type: ignore[return-value]


def get_media_scanner(settings: Settings | None = None) -> FileScanProvider:
    """按显式配置选择 Noop、Fake 或 fail-closed scanner。"""
    selected = settings or get_settings()
    try:
        get_provider_policy(selected).require(
            "scanner", selected.media_scanner_provider, settings=selected
        )
    except ProviderPolicyError as error:
        # 扫描器同样只允许显式真实 Provider，策略详情不通过异常正文泄露。
        raise RuntimeError("生产环境禁止使用未配置的媒体扫描 provider") from error
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
    retention_policy = None
    if get_provider_policy(settings).is_production:
        retention_policy = RetentionPolicy.from_settings(settings)
    get_provider_policy(settings).require("ocr", settings.ocr_provider, settings=settings)
    get_provider_policy(settings).require("asr", settings.asr_provider, settings=settings)
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
        retention_policy=retention_policy,
    )
