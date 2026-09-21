"""应用配置定义。"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """从环境变量读取运行配置，不在代码中保存真实密钥。"""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"
    database_url: str = "postgresql+psycopg://crm:crm_local_only@postgres:5432/crm_lead"
    redis_url: str = "redis://redis:6379/0"
    smart_table_adapter: Literal["mock", "unconfigured", "wecom_cli"] = "unconfigured"
    crm_adapter: Literal["mock", "unconfigured"] = "unconfigured"
    wecom_bot_id: str | None = None
    wecom_bot_secret: str | None = None
    wecom_card_callback_enabled: bool = False
    wecom_card_transport_configured: bool = False
    wecom_card_callback_handler_configured: bool = False
    wecom_card_callback_timeout_seconds: float = 4.0
    wecom_smart_table_doc_id: str | None = None
    wecom_smart_table_sheet_id: str | None = None
    wecom_smart_table_sales_can_create_records: bool | None = None
    wecom_smart_table_sales_can_delete_records: bool | None = None
    wecom_cli_command: str = "wecom-cli"
    wecom_cli_timeout_seconds: float = 20.0
    wecom_cli_retry_count: int = 1
    lead_context_ttl_minutes: int = 30
    lead_message_retry_count: int = 1
    lead_outbox_poll_seconds: int = 10
    lead_processing_timeout_seconds: int = 300
    llm_provider: Literal["mock", "qwen"] = "qwen"
    qwen_api_key: str | None = None
    qwen_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    qwen_model: str = "qwen3.7-flash"
    ai_gateway_timeout_seconds: float = 60.0
    ai_gateway_retry_count: int = 1
    ai_high_confidence_threshold: float = 0.85
    ai_medium_confidence_threshold: float = 0.60
    crm_create_retry_count: int = 3
    media_storage_path: str = "/var/lib/crm-lead/media"
    media_image_mime_types: tuple[str, ...] = ("image/png", "image/jpeg", "image/webp")
    media_audio_mime_types: tuple[str, ...] = ("audio/mpeg", "audio/wav", "audio/mp4")
    media_max_image_bytes: int = 10 * 1024 * 1024
    media_max_audio_bytes: int = 20 * 1024 * 1024
    media_processing_timeout_seconds: float = 20.0
    media_storage_provider: Literal["unconfigured", "local", "fake", "production"] = (
        "unconfigured"
    )
    media_storage_endpoint: str | None = None
    media_storage_bucket: str | None = None
    media_storage_access_key: str | None = None
    media_storage_secret_key: str | None = None
    media_storage_private: bool = False
    media_storage_tls: bool = False
    media_storage_encryption: bool = False
    media_storage_signed_url: bool = False
    media_storage_head: bool = False
    media_storage_delete: bool = False
    media_signed_url_ttl_seconds: int | None = None
    media_signed_url_max_ttl_seconds: int = 900
    media_scanner_provider: Literal["unconfigured", "noop", "fake", "production"] = (
        "unconfigured"
    )
    media_retention_policy_version: str | None = None
    media_retention_days: int | None = None
    message_payload_retention_days: int | None = None
    notification_payload_retention_days: int | None = None
    retention_cleanup_batch_size: int = 100
    retention_cleanup_lease_seconds: int = 300
    ocr_provider: Literal["mock", "qwen"] = "qwen"
    asr_provider: Literal["mock", "qwen"] = "qwen"
    qwen_ocr_model: str = "qwen3.5-ocr"
    qwen_asr_model: str = "qwen3-asr-flash"
    console_dev_admin_token: str | None = None
    console_dev_admin_subject: str = "development-admin"
    console_dev_admin_role: str = "administrator"

    @field_validator(
        "wecom_smart_table_sales_can_create_records",
        "wecom_smart_table_sales_can_delete_records",
        mode="before",
    )
    @classmethod
    def empty_smart_table_permission_is_unset(cls, value: object) -> object:
        """将 Compose 注入的空权限变量还原为未核验状态。

        参数：value 为环境变量的原始解析值。
        返回：空字符串返回 None，其他值保持给 Pydantic 继续解析为布尔值。
        异常：无。
        副作用：防止未配置权限时因空字符串导致应用无法启动。
        """
        # 当前 CLI 不能读取权限，空值必须保留为 None 而不是错误地视作 false。
        return None if value == "" else value

    @field_validator(
        "media_storage_endpoint",
        "media_storage_bucket",
        "media_storage_access_key",
        "media_storage_secret_key",
        "media_signed_url_ttl_seconds",
        "media_retention_policy_version",
        "media_retention_days",
        "message_payload_retention_days",
        "notification_payload_retention_days",
        mode="before",
    )
    @classmethod
    def empty_t22_optional_value_is_unset(cls, value: object) -> object:
        """将 Compose 空占位符转换为缺失值，让 production readiness 明确失败。"""
        return None if value == "" else value

    @field_validator("lead_context_ttl_minutes")
    @classmethod
    def lead_context_ttl_must_be_positive(cls, value: int) -> int:
        """拒绝非正的当前客户上下文有效期配置。

        参数：value 为环境变量解析后的分钟数。
        返回值：通过校验的正整数分钟数。
        异常：分钟数不为正时抛出 ValueError，阻止服务以不安全配置启动。
        副作用：无。
        """
        if value <= 0:
            raise ValueError("LEAD_CONTEXT_TTL_MINUTES 必须大于 0")
        return value

    @field_validator("lead_message_retry_count")
    @classmethod
    def lead_message_retry_count_must_not_be_negative(cls, value: int) -> int:
        """拒绝小于零的消息处理重试次数配置。

        参数：value 为环境变量解析后的可额外重试次数。
        返回值：通过校验的非负整数。
        异常：值为负数时抛出 ValueError，阻止错误的失败检查点语义。
        副作用：无。
        """
        if value < 0:
            raise ValueError("LEAD_MESSAGE_RETRY_COUNT 不能小于 0")
        return value

    @field_validator("lead_outbox_poll_seconds")
    @classmethod
    def lead_outbox_poll_seconds_must_be_positive(cls, value: int) -> int:
        """拒绝非正的 Outbox 扫描间隔配置。

        参数：value 为环境变量解析后的秒数。
        返回值：通过校验的正整数秒数。
        异常：秒数不为正时抛出 ValueError，阻止 Worker 高频空转或停止扫描。
        副作用：无。
        """
        if value <= 0:
            raise ValueError("LEAD_OUTBOX_POLL_SECONDS 必须大于 0")
        return value

    @field_validator("lead_processing_timeout_seconds")
    @classmethod
    def lead_processing_timeout_seconds_must_be_positive(cls, value: int) -> int:
        """拒绝非正的 processing 租约超时配置。

        参数：value 为环境变量解析后的秒数。
        返回值：通过校验的正整数秒数。
        异常：秒数不为正时抛出 ValueError，阻止失联任务永久占用顺序检查点。
        副作用：无。
        """
        if value <= 0:
            raise ValueError("LEAD_PROCESSING_TIMEOUT_SECONDS 必须大于 0")
        return value

    @field_validator("crm_create_retry_count")
    @classmethod
    def crm_create_retry_count_must_be_positive(cls, value: int) -> int:
        """校验 CRM create 总尝试次数，避免传输失败无限重试。

        参数：value 为环境变量解析后的总尝试次数。
        返回值：通过校验的正整数。
        异常：值不为正时抛出 ValueError。
        副作用：无。
        """
        if value <= 0:
            raise ValueError("CRM_CREATE_RETRY_COUNT 必须大于 0")
        return value

    @field_validator("wecom_card_callback_timeout_seconds")
    @classmethod
    def wecom_card_callback_timeout_must_fit_window(cls, value: float) -> float:
        """限制 callback 内部总响应预算必须为正且小于真实五秒窗口。"""

        if value <= 0 or value >= 5:
            raise ValueError("WECOM_CARD_CALLBACK_TIMEOUT_SECONDS 必须在 0 和 5 秒之间")
        return value

    @field_validator(
        "media_retention_days",
        "message_payload_retention_days",
        "notification_payload_retention_days",
        "media_signed_url_ttl_seconds",
        "media_signed_url_max_ttl_seconds",
        "retention_cleanup_batch_size",
        "retention_cleanup_lease_seconds",
    )
    @classmethod
    def t22_positive_values_must_be_bounded(cls, value: int | None, info: object) -> int | None:
        """拒绝 T22 时长和批量配置的歧义值或失控上限。"""
        if value is None:
            return None
        if value <= 0:
            raise ValueError(f"{getattr(info, 'field_name', 'T22 配置')} 必须大于 0")
        if value > 36500 and getattr(info, "field_name", "") in {
            "media_retention_days",
            "message_payload_retention_days",
            "notification_payload_retention_days",
        }:
            raise ValueError("保留期不能超过 36500 天")
        return value

    @field_validator("media_signed_url_max_ttl_seconds")
    @classmethod
    def signed_url_max_ttl_must_fit_security_budget(cls, value: int) -> int:
        """限制签名 URL 技术最大有效期，避免配置成永久公开地址。"""
        if value > 86400:
            raise ValueError("MEDIA_SIGNED_URL_MAX_TTL_SECONDS 不能超过 86400 秒")
        return value

    def wecom_card_callback_ready(self) -> bool:
        """返回部署声明的卡片动作 capability/readiness。

        返回值：只有显式启用的非敏感 readiness 开关为 True 时返回 True；Bot 进程自身
        仍必须单独校验 bot id/secret，Worker 不复制机器人密钥。
        异常：无。
        副作用：无；该方法只读取配置，不执行网络探测。
        """
        # 该开关由部署在真实 provider 已验证后设置；非 Bot Worker 不需要复制密钥。
        return bool(
            self.wecom_card_callback_enabled
            and self.wecom_card_transport_configured
            and self.wecom_card_callback_handler_configured
        )


@lru_cache
def get_settings() -> Settings:
    """返回进程内复用的配置实例，避免重复解析环境变量。"""
    return Settings()
