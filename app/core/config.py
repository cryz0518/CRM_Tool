"""应用配置定义。"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """从环境变量读取运行配置，不在代码中保存真实密钥。"""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "development"
    log_level: str = "INFO"
    database_url: str = "postgresql+psycopg://crm:crm_local_only@postgres:5432/crm_lead"
    redis_url: str = "redis://redis:6379/0"
    smart_table_adapter: Literal["mock", "unconfigured"] = "unconfigured"


@lru_cache
def get_settings() -> Settings:
    """返回进程内复用的配置实例，避免重复解析环境变量。"""
    return Settings()
