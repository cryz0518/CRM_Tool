"""Operations Console 依赖注入与运行时 Adapter 组装。"""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.console.auth import (
    AdminIdentityProvider,
    CapabilityAuthorizer,
    DenyAllAdminIdentityProvider,
    DevelopmentAdminIdentityProvider,
    LocalCapabilityAuthorizer,
)
from app.console.break_glass import BreakGlassAccessService
from app.console.health import DefaultConsoleHealthProvider
from app.console.maintenance import ConsoleMaintenanceService
from app.console.queries import ConsoleQueryService
from app.core.config import get_settings
from app.core.provider_policy import ProviderPolicyError, get_provider_policy
from app.leads.admin_create import AdminLeadCreationService
from app.leads.discard import LeadDiscardService
from app.leads.service import FirstTextLeadWorkspaceService, LeadReassignmentService
from app.leads.transfer import SmartTableOwnerTransferService
from app.media.dependencies import get_media_storage_provider, get_media_storage_signer
from app.smart_table.dependencies import get_smart_table_adapter
from app.smart_table.permissions import UnconfiguredSmartTablePermissionVerifier


@lru_cache
def get_console_session_factory() -> sessionmaker[Session]:
    """创建 Console 使用的短事务数据库会话工厂。

    返回值：绑定应用数据库的 SQLAlchemy 会话工厂。
    异常：数据库 URL 非法时由 SQLAlchemy 抛出。
    副作用：首次调用时创建连接池，但不立即执行查询。
    """
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    return sessionmaker(engine)


@lru_cache
def get_admin_identity_provider() -> AdminIdentityProvider:
    """按环境选择显式开发认证或生产拒绝实现。

    返回值：AdminIdentityProvider Seam 的当前 Adapter。
    异常：无。
    副作用：仅读取配置，不发起认证请求。
    """
    settings = get_settings()
    try:
        # 环境判断集中在 policy；工厂只按显式 Provider 选择构造实现。
        get_provider_policy(settings).require(
            "admin_identity_provider", settings.admin_identity_provider, settings=settings
        )
    except ProviderPolicyError:
        # 生产缺失或误用开发 Provider 时保持请求 fail closed，同时由 readiness 报告原因。
        return DenyAllAdminIdentityProvider()
    if settings.admin_identity_provider == "development":
        return DevelopmentAdminIdentityProvider(
            token=settings.console_dev_admin_token,
            subject=settings.console_dev_admin_subject,
            roles=frozenset({settings.console_dev_admin_role}),
        )
    return DenyAllAdminIdentityProvider()


def get_capability_authorizer(
) -> CapabilityAuthorizer:
    """将认证 Provider 暴露为独立 capability 授权 Seam。

    返回值：只负责本地授权的稳定接口，Console 路由不直接耦合认证 Provider。
    异常：无。
    副作用：无外部调用。
    """
    return LocalCapabilityAuthorizer(get_console_session_factory())


@lru_cache
def get_console_query_service() -> ConsoleQueryService:
    """组装只读 ConsoleQueryService 及其健康探针。

    返回值：供 FastAPI 路由使用的 Query Service。
    异常：依赖构造失败时向应用启动或请求传播。
    副作用：首次调用时创建数据库会话工厂和健康 Adapter。
    """
    session_factory = get_console_session_factory()
    return ConsoleQueryService(
        session_factory,
        DefaultConsoleHealthProvider(session_factory, get_smart_table_adapter()),
    )


@lru_cache
def get_break_glass_access_service() -> BreakGlassAccessService:
    """组装 Break-glass 服务及待接入的私有附件签名 Adapter。

    返回值：供受保护路由调用的 BreakGlassAccessService。
    异常：无。
    副作用：仅构造服务；T22 对象存储 Adapter 接入前，附件访问会被安全拒绝。
    """
    settings = get_settings()
    # T15 身份与 capability 不变；T22 只注入显式 provider，失败时保持附件访问关闭。
    signer = get_media_storage_signer(settings)
    storage = get_media_storage_provider(settings)
    ttl = settings.media_signed_url_ttl_seconds
    if ttl is None and get_provider_policy(settings).is_production:
        raise RuntimeError("生产 signed URL TTL 未显式配置")
    if ttl is None:
        ttl = 300
    return BreakGlassAccessService(
        get_console_session_factory(),
        signed_url_provider=signer,
        storage_provider=storage,
        signed_url_ttl_seconds=ttl,
        signed_url_max_ttl_seconds=settings.media_signed_url_max_ttl_seconds,
    )


@lru_cache
def get_console_maintenance_service() -> ConsoleMaintenanceService:
    """组装 Console 管理写入口及其领域服务。"""

    session_factory = get_console_session_factory()
    smart_table = get_smart_table_adapter()
    return ConsoleMaintenanceService(
        session_factory,
        FirstTextLeadWorkspaceService(session_factory, smart_table),
        LeadReassignmentService(session_factory),
        LeadDiscardService(session_factory),
        AdminLeadCreationService(session_factory, smart_table),
        SmartTableOwnerTransferService(
            session_factory, smart_table, UnconfiguredSmartTablePermissionVerifier()
        ),
    )
