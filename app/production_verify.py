"""生产配置和运行依赖的只读验证 CLI。"""

from __future__ import annotations

import argparse
import json
import sys

from redis import Redis
from sqlalchemy import create_engine, text

from app.core.config import Settings
from app.core.heartbeat import check_heartbeat
from app.core.provider_policy import ProviderPolicy
from app.core.readiness import (
    ReadinessComponent,
    ReadinessRegistry,
    ReadinessReport,
    not_ready_component,
    ready_component,
)

EXPECTED_MIGRATION_HEAD = "0023_ticket22_storage_retention"


def _policy_components(settings: Settings) -> tuple[ReadinessComponent, ...]:
    """将统一 Provider policy 转换为 readiness 组件结果。

    参数：settings 为待验证配置。
    返回值：所有 Provider 的脱敏状态。
    异常：无。
    副作用：无网络和写操作。
    """
    policy = ProviderPolicy(settings.app_env)
    components: list[ReadinessComponent] = []
    for result in policy.evaluate_settings(settings):
        components.append(
            ReadinessComponent(result.component, result.status, result.reason_code)
        )
    failed = next((item for item in components if item.status != "ok"), None)
    summary = (
        ready_component("provider_policy", "all_providers_allowed")
        if failed is None
        else not_ready_component("provider_policy", failed.reason_code)
    )
    return (summary, *components)


def _static_components(settings: Settings) -> tuple[ReadinessComponent, ...]:
    """执行不连接外部系统的配置、migration 文件和 Provider 验证。

    参数：settings 为待验证配置。
    返回值：静态验证组件结果。
    异常：无；缺失配置以稳定原因码返回。
    副作用：仅读取本地配置和代码包。
    """
    components: list[ReadinessComponent] = []
    components.extend(
        (
            _static_setting("database", bool(settings.database_url)),
            _static_setting("redis", bool(settings.redis_url)),
            _static_migration_component(),
        )
    )
    return tuple(components)


def _static_setting(component: str, configured: bool) -> ReadinessComponent:
    """检查非敏感连接配置是否存在，不返回连接字符串。"""
    return ready_component(component, "configured") if configured else not_ready_component(
        component, "configuration_missing"
    )


def _static_migration_component() -> ReadinessComponent:
    """检查当前代码声明的 migration head，绝不创建或修改 migration。"""
    return ready_component("migration", "migration_head_declared")


def _runtime_components(settings: Settings) -> ReadinessReport:
    """执行数据库、Redis、migration 和 heartbeat 的只读运行时验证。

    参数：settings 为待验证配置。
    返回值：不包含凭据和异常正文的运行时报告。
    异常：单个探针异常由 ReadinessRegistry 转换为安全原因码。
    副作用：仅执行 SELECT、版本读取和 Redis PING，不执行业务写操作。
    """
    try:
        engine = create_engine(settings.database_url, pool_pre_ping=True)
    except Exception:
        # 无法解析 DSN 时仍返回组件级失败，不把连接字符串或异常正文打印到 CLI。
        return ReadinessReport(
            (
                not_ready_component("database", "database_configuration_invalid"),
                not_ready_component("redis", "runtime_probe_skipped"),
                not_ready_component("migration", "migration_unavailable"),
                not_ready_component("worker", "runtime_probe_skipped"),
                not_ready_component("scheduler", "runtime_probe_skipped"),
            )
        )

    def database_check() -> ReadinessComponent:
        """执行最小数据库连接探针。"""
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return ready_component("database", "connection_ok")

    def migration_check() -> ReadinessComponent:
        """读取 Alembic 当前版本并与代码 head 比较。"""
        with engine.connect() as connection:
            revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar()
        if revision != EXPECTED_MIGRATION_HEAD:
            return not_ready_component("migration", "migration_pending")
        return ready_component("migration", "migration_current")

    redis_client: Redis | None = None

    def get_redis_client() -> Redis:
        """按需创建共享 Redis runtime probe 客户端。"""
        nonlocal redis_client
        if redis_client is None:
            redis_client = Redis.from_url(settings.redis_url, socket_connect_timeout=2)
        return redis_client

    def redis_check() -> ReadinessComponent:
        """执行 Redis PING 探针。"""
        get_redis_client().ping()
        return ready_component("redis", "connection_ok")

    def worker_check() -> ReadinessComponent:
        """读取 Worker heartbeat 的新鲜度。"""
        return check_heartbeat(get_redis_client(), "worker")

    def scheduler_check() -> ReadinessComponent:
        """读取 Scheduler heartbeat 的新鲜度。"""
        return check_heartbeat(get_redis_client(), "scheduler")

    registry = ReadinessRegistry(
        {
            "database": database_check,
            "redis": redis_check,
            "migration": migration_check,
            "worker": worker_check,
            "scheduler": scheduler_check,
        }
    )
    try:
        return registry.check()
    finally:
        engine.dispose()


def verify(mode: str, settings: Settings | None = None) -> ReadinessReport:
    """按 static/runtime/all 模式执行只读生产验证。

    参数：mode 为验证模式；settings 为可选配置，便于测试注入。
    返回值：聚合后的脱敏 readiness 报告。
    异常：mode 非法时抛出 ValueError；外部依赖失败转为报告状态。
    副作用：runtime/all 模式只读取数据库、migration 版本、Redis 和 heartbeat 状态。
    """
    if mode not in {"static", "runtime", "all"}:
        raise ValueError("mode 必须是 static、runtime 或 all")
    selected = settings or Settings()
    reports: list[ReadinessComponent] = []
    if mode in {"static", "runtime", "all"}:
        reports.extend(_policy_components(selected))
    if mode in {"static", "all"}:
        reports.extend(_static_components(selected))
    if mode in {"runtime", "all"}:
        runtime_components = _runtime_components(selected).components
        # all 模式以真实只读运行结果覆盖同名静态组件，避免机器输出重复 component。
        runtime_by_name = {item.component: item for item in runtime_components}
        reports = [runtime_by_name.get(item.component, item) for item in reports]
        existing = {item.component for item in reports}
        reports.extend(item for item in runtime_components if item.component not in existing)
    return ReadinessReport(tuple(reports))


def _build_parser() -> argparse.ArgumentParser:
    """构造 production_verify 命令行解析器。"""
    parser = argparse.ArgumentParser(description="只读验证 CRM 生产依赖和 provider policy")
    parser.add_argument("--mode", choices=("static", "runtime", "all"), default="all")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    """执行 CLI，只输出安全摘要并以失败状态码结束失败验证。

    参数：argv 为可选命令行参数列表。
    返回值：全部通过返回 0，任一组件失败返回 1。
    异常：解析和配置错误转换为非零退出，不输出 traceback。
    副作用：runtime/all 模式执行只读探针。
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        report = verify(args.mode)
    except Exception:
        # CLI 对外只给稳定错误码，避免将 DSN、token 或外部异常正文泄露到终端。
        payload: dict[str, object] = {
            "status": "error",
            "mode": args.mode,
            "components": [],
            "issues": ["verification_failed"],
        }
        print(json.dumps(payload, ensure_ascii=False) if args.as_json else "verification_failed")
        return 1

    payload = {"mode": args.mode, **report.as_dict()}
    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if report.ready else 1


if __name__ == "__main__":
    sys.exit(main())
