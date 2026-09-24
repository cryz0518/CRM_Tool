"""统一隔离 pytest 与开发者本机真实环境配置。"""

from __future__ import annotations

import os

import pytest

from app.core.config import get_settings


@pytest.fixture(autouse=True, scope="session")
def isolate_settings_from_developer_env() -> None:
    """让测试默认不读取项目 `.env`，避免真实凭据改变缺失配置断言。

    参数：无。
    返回值：无。
    异常：无。
    副作用：仅在 pytest 进程内设置 `PYDANTIC_SETTINGS_DISABLE_DOTENV`，并清理配置缓存。
    """
    os.environ["PYDANTIC_SETTINGS_DISABLE_DOTENV"] = "1"
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
