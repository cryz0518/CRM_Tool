"""CRM 适配器依赖构造边界。"""

from __future__ import annotations

from app.core.config import get_settings
from app.crm.adapter import CRMAdapter
from app.crm.mock import MockCRMAdapter


def get_crm_adapter() -> CRMAdapter:
    """按显式配置构造 CRM 适配器，禁止 Worker 默默使用测试实现。

    返回值：当前环境声明的 CRMAdapter。
    异常：未配置真实 CRM 适配器时抛出 RuntimeError，动作进入 pending_recovery。
    副作用：mock 配置下创建内存测试适配器；不调用 CRM 网络接口。
    """

    settings = get_settings()
    if settings.crm_adapter == "mock":
        return MockCRMAdapter()
    raise RuntimeError("CRM_ADAPTER 未配置真实实现")
