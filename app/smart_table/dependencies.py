"""向 Web 与业务入口提供可替换的智能表格适配器。"""

from __future__ import annotations

from functools import lru_cache

from app.core.config import get_settings
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.mock import MockSmartTableAdapter
from app.smart_table.registry import build_required_smart_table_schema
from app.smart_table.unconfigured import UnconfiguredSmartTableAdapter
from app.smart_table.wecom_cli import WecomCliSmartTableAdapter


@lru_cache
def get_smart_table_adapter() -> SmartTableAdapter:
    """根据部署配置返回智能表格适配器，未配置时返回安全占位实现。

    返回：开发环境显式启用时返回 Mock；否则返回未配置占位适配器。
    副作用：首次调用时缓存适配器，避免每个请求重复构造依赖。
    """
    if get_settings().smart_table_adapter == "mock":
        # Mock 只能由本地 Docker 或测试环境显式启用，不能掩盖生产配置缺失。
        return MockSmartTableAdapter(schema=build_required_smart_table_schema())
    if get_settings().smart_table_adapter == "wecom_cli":
        # 真实适配器仅接收部署注入的配置，任何缺失均由构造器转换为 readiness 配置错误。
        settings = get_settings()
        return WecomCliSmartTableAdapter(
            doc_id=settings.wecom_smart_table_doc_id or "",
            sheet_id=settings.wecom_smart_table_sheet_id or "",
            sales_can_create_records=settings.wecom_smart_table_sales_can_create_records,
            sales_can_delete_records=settings.wecom_smart_table_sales_can_delete_records,
            command=settings.wecom_cli_command,
            timeout_seconds=settings.wecom_cli_timeout_seconds,
            retry_count=settings.wecom_cli_retry_count,
        )
    return UnconfiguredSmartTableAdapter()
