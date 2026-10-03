"""向 Web 与业务入口提供可替换的智能表格适配器。"""

from __future__ import annotations

from functools import lru_cache

from app.core.config import get_settings
from app.core.provider_policy import ProviderPolicyError, get_provider_policy
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.audit import build_mock_audit_schema
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
    settings = get_settings()
    try:
        # 统一 policy 负责 production 禁止 Mock；工厂不自行读取 APP_ENV。
        get_provider_policy(settings).require(
            "smart_table", settings.smart_table_adapter, settings=settings
        )
    except ProviderPolicyError:
        return UnconfiguredSmartTableAdapter()
    if settings.smart_table_adapter == "mock":
        # Mock 只能由本地 Docker 或测试环境显式启用，不能掩盖生产配置缺失。
        return MockSmartTableAdapter(schema=build_required_smart_table_schema())
    if settings.smart_table_adapter == "wecom_cli":
        # 真实适配器仅接收部署注入的配置，任何缺失均由构造器转换为 readiness 配置错误。
        return WecomCliSmartTableAdapter(
            doc_id=settings.wecom_smart_table_doc_id or "",
            sheet_id=settings.wecom_smart_table_sheet_id or "",
            sheet_title=settings.wecom_smart_table_sheet_title,
            sales_can_create_records=settings.wecom_smart_table_sales_can_create_records,
            sales_can_delete_records=settings.wecom_smart_table_sales_can_delete_records,
            command=settings.wecom_cli_command,
            timeout_seconds=settings.wecom_cli_timeout_seconds,
            retry_count=settings.wecom_cli_retry_count,
        )
    return UnconfiguredSmartTableAdapter()


@lru_cache
def get_smart_table_audit_adapter() -> SmartTableAdapter:
    """根据独立审计子表配置构造 Smart Table 镜像适配器。

    返回：与主 CRM 线索子表隔离的审计适配器；未配置时返回可重试的占位实现。
    异常：Provider policy 或适配器构造异常转换为占位适配器，避免阻塞核心业务 readiness。
    副作用：首次调用时缓存适配器，不管理 Smart Table ACL。
    """
    settings = get_settings()
    try:
        # 审计镜像沿用 Smart Table provider policy，但不会参与主表 readiness。
        get_provider_policy(settings).require(
            "smart_table", settings.smart_table_adapter, settings=settings
        )
    except ProviderPolicyError:
        return UnconfiguredSmartTableAdapter()
    if settings.smart_table_adapter == "mock":
        # Mock 只提供字段契约，ACL 仍由真实企微管理员配置。
        return MockSmartTableAdapter(schema=build_mock_audit_schema(), require_owner_field=False)
    if settings.smart_table_adapter == "wecom_cli":
        if not settings.wecom_audit_sheet_id or not settings.wecom_audit_sheet_title:
            return UnconfiguredSmartTableAdapter()
        # 使用独立子表 ID/title，绝不复用 CRM 线索 sheet_id，也不读取或修改 ACL。
        return WecomCliSmartTableAdapter(
            doc_id=settings.wecom_smart_table_doc_id or "",
            sheet_id=settings.wecom_audit_sheet_id,
            sheet_title=settings.wecom_audit_sheet_title,
            require_owner_field=False,
            command=settings.wecom_cli_command,
            timeout_seconds=settings.wecom_cli_timeout_seconds,
            retry_count=settings.wecom_cli_retry_count,
        )
    return UnconfiguredSmartTableAdapter()
