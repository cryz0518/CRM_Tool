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
    wecom_bot_id: str | None = None
    wecom_bot_secret: str | None = None
    wecom_smart_table_doc_id: str | None = None
    wecom_smart_table_sheet_id: str | None = None
    wecom_smart_table_sales_can_create_records: bool | None = None
    wecom_smart_table_sales_can_delete_records: bool | None = None
    wecom_cli_command: str = "wecom-cli"
    wecom_cli_timeout_seconds: float = 20.0
    wecom_cli_retry_count: int = 1
    lead_context_ttl_minutes: int = 30

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


@lru_cache
def get_settings() -> Settings:
    """返回进程内复用的配置实例，避免重复解析环境变量。"""
    return Settings()
