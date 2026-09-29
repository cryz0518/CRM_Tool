"""统一隔离 pytest 与开发者本机真实环境配置。"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from app.core.config import Settings, get_settings

_INTEGRATION_OWNER_IDS = (
    "sales-1",
    "sales-A",
    "sales-B",
    "sales-a",
    "sales-b",
    "sales-old",
    "sales-success",
    "sales-failure",
    "sales-lost-update",
    "sales-timeout-recovery",
    "sales-claim-fence",
    "sales-stale-read",
)


def _write_isolated_employee_directory() -> str:
    """创建只供测试使用的员工目录，不读取开发者真实 employee.csv。"""
    path = Path(tempfile.gettempdir()) / "crm-t21-pytest-employees.csv"
    rows = ["id,name,nickname"]
    rows.extend(f"crm-{user_id},{user_id},{user_id}" for user_id in _INTEGRATION_OWNER_IDS)
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return str(path)


@pytest.fixture(autouse=True, scope="session")
def isolate_settings_from_developer_env() -> None:
    """让测试默认不读取项目 `.env`，避免真实凭据改变缺失配置断言。

    参数：无。
    返回值：无。
    异常：无。
    副作用：仅在 pytest 进程内设置 `PYDANTIC_SETTINGS_DISABLE_DOTENV`，并清理配置缓存。
    """
    # Pydantic Settings 没有读取 PYDANTIC_SETTINGS_DISABLE_DOTENV 这个约定；
    # 测试进程直接关闭模型默认 env_file，并用非敏感的 provider 选择覆盖外部配置。
    Settings.model_config["env_file"] = None
    os.environ.update(
        {
            "PYDANTIC_SETTINGS_DISABLE_DOTENV": "1",
            "APP_ENV": "development",
            "SMART_TABLE_ADAPTER": "mock",
            "CRM_ADAPTER": "mock",
            "TYC_PROVIDER": "mock",
            "LLM_PROVIDER": "mock",
            "OCR_PROVIDER": "mock",
            "ASR_PROVIDER": "mock",
            "MEDIA_STORAGE_PROVIDER": "fake",
            "MEDIA_SCANNER_PROVIDER": "fake",
            "EMPLOYEE_DIRECTORY_PATH": _write_isolated_employee_directory(),
            "CRM_URL": "",
            "CRM_APP_ID": "",
            "CRM_APP_AUTH_TOKEN": "",
            "CRM_PRIVATE_KEY": "",
            "CRM_AUTHORIZATION": "",
            "Authorization": "",
            "TIANYANCHA_API_KEY": "",
            "TIANYANCHA_URL": "",
            "QWEN_API_KEY": "",
        }
    )
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
