"""Operations Console 依赖注入与运行时 Adapter 组装。"""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.console.auth import (
    AdminIdentityProvider,
    DenyAllAdminIdentityProvider,
    DevelopmentAdminIdentityProvider,
)
from app.console.break_glass import BreakGlassAccessService
from app.console.health import DefaultConsoleHealthProvider
from app.console.queries import ConsoleQueryService
from app.core.config import get_settings
from app.smart_table.dependencies import get_smart_table_adapter


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
    if settings.app_env in {"development", "test"}:
        return DevelopmentAdminIdentityProvider(
            token=settings.console_dev_admin_token,
            subject=settings.console_dev_admin_subject,
            roles=frozenset({settings.console_dev_admin_role}),
        )
    return DenyAllAdminIdentityProvider()


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
    # T15 不实现生产对象存储；没有签名提供器时禁止退化为直接返回附件二进制。
    return BreakGlassAccessService(get_console_session_factory(), signed_url_provider=None)
