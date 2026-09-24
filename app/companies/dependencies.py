"""天眼查适配器依赖构造边界。"""

from __future__ import annotations

from app.companies.service import MockTYCAdapter, TYCAdapter
from app.companies.tyc import TianYanChaAdapter
from app.core.config import get_settings
from app.core.provider_policy import ProviderPolicyError, get_provider_policy


def get_tyc_adapter() -> TYCAdapter:
    """按统一 Provider policy 构造天眼查适配器。"""
    settings = get_settings()
    try:
        get_provider_policy(settings).require("tyc", settings.tyc_provider, settings=settings)
    except ProviderPolicyError as error:
        raise RuntimeError("TYC_PROVIDER 未配置真实实现") from error
    if settings.tyc_provider == "mock":
        return MockTYCAdapter()
    return TianYanChaAdapter(settings.tianyancha_url, settings.tianyancha_api_key or "")
