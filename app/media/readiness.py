"""T22 媒体存储、扫描和保留策略 readiness 检查。"""

from __future__ import annotations

from dataclasses import dataclass

from app.core.config import Settings
from app.core.provider_policy import get_provider_policy


@dataclass(frozen=True)
class MediaReadinessReport:
    """保存媒体安全 readiness 的脱敏结果。"""

    ready: bool
    issues: tuple[str, ...]


class ProductionMediaReadinessChecker:
    """在生产环境拒绝本地存储、Noop 扫描和隐式保留策略。"""

    def check(self, settings: Settings) -> MediaReadinessReport:
        """检查生产媒体所需的 provider、能力、签名和 data class 策略。"""
        if not get_provider_policy(settings).is_production:
            return MediaReadinessReport(True, ())

        issues: list[str] = []
        if settings.media_storage_provider != "production":
            issues.append("生产对象存储未配置")
        else:
            if not (settings.media_storage_endpoint or "").startswith("https://"):
                issues.append("生产对象存储 endpoint 必须使用 HTTPS")
            if not settings.media_storage_bucket:
                issues.append("生产对象存储 bucket/container 未配置")
            if not settings.media_storage_access_key or not settings.media_storage_secret_key:
                issues.append("生产对象存储凭据未配置")
            for enabled, label in (
                (settings.media_storage_private, "private capability"),
                (settings.media_storage_tls, "TLS capability"),
                (settings.media_storage_encryption, "encryption capability"),
                (settings.media_storage_signed_url, "signed URL capability"),
                (settings.media_storage_head, "head/stat capability"),
                (settings.media_storage_delete, "delete capability"),
            ):
                if not enabled:
                    issues.append(f"生产对象存储缺少 {label}")
            issues.append("生产对象存储适配器未安装")

        if settings.media_scanner_provider != "production":
            issues.append("生产文件扫描器未配置")

        if settings.media_signed_url_ttl_seconds is None:
            issues.append("生产 signed URL TTL 未显式配置")
        elif settings.media_signed_url_ttl_seconds > settings.media_signed_url_max_ttl_seconds:
            issues.append("生产 signed URL TTL 超过技术最大值")

        if not settings.media_retention_policy_version:
            issues.append("媒体保留期策略版本未显式配置")
        if (
            settings.media_retention_days is None
            or settings.message_payload_retention_days is None
            or settings.notification_payload_retention_days is None
        ):
            issues.append("媒体保留期策略未显式配置")

        return MediaReadinessReport(ready=not issues, issues=tuple(issues))
