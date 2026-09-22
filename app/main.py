"""FastAPI 应用入口，提供健康检查与内部 Operations Console。"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from redis import Redis
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from starlette.middleware.base import RequestResponseEndpoint

from app.console.routes import router as console_router
from app.core.config import get_settings
from app.core.heartbeat import check_heartbeat
from app.core.logging import bind_log_context, configure_logging, reset_log_context
from app.core.provider_policy import get_provider_policy
from app.core.readiness import (
    ReadinessComponent,
    ReadinessRegistry,
    ReadinessReport,
    not_ready_component,
    ready_component,
)
from app.core.request_id import normalize_request_id
from app.media.readiness import ProductionMediaReadinessChecker
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.dependencies import get_smart_table_adapter
from app.smart_table.readiness import SmartTableReadinessChecker

settings = get_settings()
configure_logging(settings.log_level, environment=settings.app_env, service="app")
logger = logging.getLogger(__name__)

app = FastAPI(title="CRM 线索自动录入", version="0.1.0")
app.include_router(console_router)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next: RequestResponseEndpoint) -> Response:
    """为每个 HTTP 请求绑定请求标识并写入响应头和结构化日志。"""
    # 客户端 header 只能作为候选值；非法、超长或含控制字符时由服务端生成新 UUID。
    request_id = normalize_request_id(request.headers.get("X-Request-ID"))
    request.state.request_id = request_id
    token = bind_log_context(request_id=request_id)
    try:
        logger.info(
            "http_request_started",
            extra={"path": request.url.path, "method": request.method},
        )
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "http_request_completed",
            extra={
                "path": request.url.path,
                "method": request.method,
                "status_code": response.status_code,
            },
        )
        return response
    finally:
        # 请求结束后立即清理上下文，避免并发请求相互污染链路字段。
        reset_log_context(token)


@app.get("/health")
async def health() -> dict[str, str]:
    """返回应用进程存活状态，供 Docker 与内部运维探针调用。"""
    return {"status": "ok", "service": "app"}


def _provider_policy_components() -> tuple[ReadinessComponent, ...]:
    """返回所有外部 Provider 的集中策略判定，不连接任何外部厂商。

    返回值：包含管理员身份、智能表格、CRM、LLM、OCR、ASR、存储和扫描器的状态。
    异常：无。
    副作用：仅读取已解析配置。
    """
    policy = get_provider_policy(settings)
    provider_components = tuple(
        ReadinessComponent(item.component, item.status, item.reason_code)
        for item in policy.evaluate_settings(settings)
    )
    failed = next((item for item in provider_components if item.status != "ok"), None)
    summary = (
        ready_component("provider_policy", "all_providers_allowed")
        if failed is None
        else not_ready_component("provider_policy", failed.reason_code)
    )
    return (summary, *provider_components)


def _runtime_readiness_components() -> tuple[ReadinessComponent, ...]:
    """执行数据库、Redis、migration、Worker 和 Scheduler 的只读检查。

    返回值：安全的组件状态，不包含异常正文、连接字符串或凭据。
    异常：单项异常被转换为稳定 reason_code。
    副作用：执行 SELECT、migration 版本读取和 Redis PING，不执行业务写操作。
    """
    try:
        engine: Engine | None = create_engine(settings.database_url, pool_pre_ping=True)
    except Exception:
        # URL 解析失败也必须以安全 readiness 结果返回，不能把 DSN 放进 HTTP 异常。
        engine = None

    def database_check() -> ReadinessComponent:
        """执行数据库最小连接探针。"""
        if engine is None:
            return not_ready_component("database", "database_configuration_invalid")
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except Exception:
            return not_ready_component("database", "database_unavailable")
        return ready_component("database", "connection_ok")

    def migration_check() -> ReadinessComponent:
        """读取 migration 当前版本并拒绝未完成迁移。"""
        if engine is None:
            return not_ready_component("migration", "migration_unavailable")
        try:
            with engine.connect() as connection:
                revision = connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar()
        except Exception:
            return not_ready_component("migration", "migration_unavailable")
        if revision != "0023_ticket22_storage_retention":
            return not_ready_component("migration", "migration_pending")
        return ready_component("migration", "migration_current")

    redis_client: Redis | None = None

    def get_redis_client() -> Redis:
        """按需创建共享 Redis readiness 客户端。"""
        nonlocal redis_client
        if redis_client is None:
            redis_client = Redis.from_url(settings.redis_url, socket_connect_timeout=2)
        return redis_client

    def redis_check() -> ReadinessComponent:
        """执行 Redis PING，不返回 Redis URL 或错误正文。"""
        try:
            get_redis_client().ping()
        except Exception:
            return not_ready_component("redis", "redis_unavailable")
        return ready_component("redis", "connection_ok")

    def heartbeat_check(component: str) -> ReadinessComponent:
        """执行单个 Worker/Scheduler heartbeat 的安全检查。"""
        try:
            return check_heartbeat(get_redis_client(), component)
        except Exception:
            return not_ready_component(component, "heartbeat_unavailable")

    try:
        registry = ReadinessRegistry(
            {
                "database": database_check,
                "redis": redis_check,
                "migration": migration_check,
                "worker": lambda: heartbeat_check("worker"),
                "scheduler": lambda: heartbeat_check("scheduler"),
            }
        )
        return registry.check().components
    finally:
        if engine is not None:
            engine.dispose()


def _smart_table_component(
    adapter: SmartTableAdapter,
) -> tuple[ReadinessComponent, tuple[str, ...]]:
    """检查智能表格结构并转换为统一组件状态。

    参数：adapter 为 FastAPI 依赖注入的稳定 Smart Table Adapter。
    返回值：统一组件结果及兼容旧接口的中文问题列表。
    异常：适配器错误由统一 ReadinessRegistry 转换为稳定原因码。
    副作用：只读取智能表格 schema 和权限。
    """
    detail_issues: tuple[str, ...] = ()

    def check() -> ReadinessComponent:
        """执行智能表格只读检查并转换为组件状态。"""
        nonlocal detail_issues
        report = SmartTableReadinessChecker().check(adapter)
        detail_issues = report.issues
        if report.ready:
            return ready_component("smart_table", "schema_and_permissions_ok")
        reason = "smart_table_provider_missing" if any(
            "适配器未配置" in issue for issue in report.issues
        ) else "smart_table_configuration_invalid"
        return not_ready_component("smart_table", reason)

    registry = ReadinessRegistry({"smart_table": check})
    report = registry.check()
    component = report.components[0]
    if component.reason_code == "dependency_unavailable":
        detail_issues = ("智能表格依赖不可用",)
    return component, detail_issues


def _readiness_report(
    adapter: SmartTableAdapter,
) -> tuple[ReadinessReport, tuple[str, ...]]:
    """通过统一 Registry 执行 readiness，并保留兼容的详细问题文本。

    参数：adapter 为依赖注入的稳定 Smart Table Adapter。
    返回值：统一组件报告及 T15/T22 使用的脱敏详细问题列表。
    异常：单个组件异常由 ReadinessRegistry 转换为稳定 reason_code。
    副作用：只执行数据库、Redis、heartbeat、schema、策略和媒体配置检查，不执行业务写入。
    """
    policy = get_provider_policy(settings)
    detail_issues: list[str] = []
    provider_components: tuple[ReadinessComponent, ...] = ()
    media_report = None
    runtime_components: tuple[ReadinessComponent, ...] = ()

    def smart_table_check() -> ReadinessComponent:
        """执行智能表格检查，并缓存旧接口需要的字段级问题。"""
        component, issues = _smart_table_component(adapter)
        detail_issues.extend(issues)
        return component

    def media_check(component: str) -> ReadinessComponent:
        """把存储和扫描器共享的媒体策略检查映射为单个 readiness 组件。"""
        nonlocal media_report
        if media_report is None:
            media_report = ProductionMediaReadinessChecker().check(settings)
            detail_issues.extend(media_report.issues)
        if media_report.issues:
            return not_ready_component(component, "media_configuration_invalid")
        return ready_component(component, "media_policy_checked")

    def provider_policy_check() -> ReadinessComponent:
        """执行集中 Provider policy，并缓存各 provider 的脱敏明细。"""
        nonlocal provider_components
        provider_components = _provider_policy_components()
        return provider_components[0]

    def runtime_check(component: str) -> ReadinessComponent:
        """从统一运行时探针报告中读取指定组件结果。"""
        nonlocal runtime_components
        if not runtime_components:
            runtime_components = (
                _runtime_readiness_components()
                if policy.is_production
                else tuple(
                    ready_component(name, "non_production_not_probed")
                    for name in ("database", "redis", "migration", "worker", "scheduler")
                )
            )
        return next(item for item in runtime_components if item.component == component)

    # 所有 HTTP readiness 组件都先进入同一 Registry；Registry 负责统一异常边界。
    registry = ReadinessRegistry(
        {
            "smart_table": smart_table_check,
            "storage": lambda: media_check("storage"),
            "scanner": lambda: media_check("scanner"),
            "provider_policy": provider_policy_check,
            "database": lambda: runtime_check("database"),
            "redis": lambda: runtime_check("redis"),
            "migration": lambda: runtime_check("migration"),
            "worker": lambda: runtime_check("worker"),
            "scheduler": lambda: runtime_check("scheduler"),
        }
    )
    registry_report = registry.check()

    # Provider 明细继续保留，便于 Operations Console 定位具体 provider；其判定已由同一
    # provider policy 产生，异常时补齐固定组件和固定原因码，绝不暴露异常正文。
    if not provider_components:
        provider_components = tuple(
            not_ready_component(component, "provider_policy_unavailable")
            for component in (
                "admin_identity_provider",
                "smart_table",
                "crm",
                "llm",
                "ocr",
                "asr",
                "storage",
                "scanner",
            )
        )
    return ReadinessReport((*registry_report.components, *provider_components)), tuple(
        detail_issues
    )


@app.get("/health/ready")
async def readiness(
    adapter: Annotated[SmartTableAdapter, Depends(get_smart_table_adapter)],
) -> JSONResponse:
    """校验管理员预配置的智能表格结构与权限，并返回就绪状态。

    参数：adapter 为依赖注入的智能表格适配器。
    返回：配置正确时返回 200；缺失字段、权限或适配器时返回 503 与脱敏问题摘要。
    副作用：调用适配器读取结构和权限，并写入结构化就绪检查日志。
    """
    report, detail_issues = _readiness_report(adapter)
    components = report.components
    # 非生产环境保留 T15/T22 的详细问题契约，同时把 Registry 捕获的媒体异常转换为稳定码。
    issues = list(detail_issues)
    if not get_provider_policy(settings).is_production:
        media_components_seen: set[str] = set()
        for item in components:
            # provider 明细中也有 storage/scanner，同名组件只取 Registry 的首个结果。
            if item.component not in {"storage", "scanner"}:
                continue
            if item.component in media_components_seen:
                continue
            media_components_seen.add(item.component)
            issue = f"{item.component}:{item.reason_code}"
            if item.status != "ok" and issue not in issues:
                issues.append(issue)
    else:
        issues.extend(item for item in report.issues if item not in issues)
    ready = not issues
    if not ready:
        return JSONResponse(
            {
                "status": "error",
                "service": "app",
                "components": [item.as_dict() for item in components],
                "issues": issues,
            },
            status_code=503,
        )

    return JSONResponse(
        {
            "status": "ok",
            "service": "app",
            "components": [item.as_dict() for item in components],
            "issues": [],
        }
    )
