"""Operations Console 依赖健康与 readiness 查询。"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Protocol

from redis import Redis
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from app.console.dto import ConsoleHealthStatusDTO
from app.core.config import get_settings
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.readiness import SmartTableReadinessChecker

logger = logging.getLogger(__name__)


class ConsoleHealthProvider(Protocol):
    """定义可替换的 Console 健康状态来源。"""

    def snapshot(self) -> list[ConsoleHealthStatusDTO]:
        """返回一次全依赖健康检查的脱敏结果。"""


class DefaultConsoleHealthProvider:
    """执行应用、PostgreSQL、Redis 和智能表格检查，并明确标记不可观测进程。"""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        smart_table_adapter: SmartTableAdapter,
    ) -> None:
        """注入数据库会话工厂和智能表格 Adapter。

        参数：session_factory 为 Console 查询使用的数据库会话工厂；adapter 为 readiness Adapter。
        返回值：无。
        异常：无，单项探针失败转换为状态 DTO。
        副作用：snapshot 会访问数据库、Redis 和智能表格配置。
        """
        self._session_factory = session_factory
        self._smart_table_adapter = smart_table_adapter

    def snapshot(self) -> list[ConsoleHealthStatusDTO]:
        """返回当前应用依赖的健康状态，失败项不泄露异常正文。

        返回值：包含 PostgreSQL、Redis、Worker、Scheduler、WeCom 和 Smart Table 的状态。
        异常：单项探针异常被捕获；整体不会因一个依赖失败而抛出。
        副作用：执行只读健康探针和智能表格 readiness 检查。
        """
        checked_at = datetime.now(UTC)
        statuses = [ConsoleHealthStatusDTO(name="app", status="ok", checked_at=checked_at)]
        statuses.append(self._check_postgres(checked_at))
        statuses.append(self._check_redis(checked_at))
        statuses.append(self._check_smart_table(checked_at))
        # 当前 Compose 中进程探针位于各自容器，app 不可安全伪造其进程级存活结论。
        for name in ("worker", "scheduler", "wecom"):
            statuses.append(
                ConsoleHealthStatusDTO(
                    name=name,
                    status="unknown",
                    detail="进程级状态由独立容器探针提供",
                    checked_at=checked_at,
                )
            )
        return statuses

    def _check_postgres(self, checked_at: datetime) -> ConsoleHealthStatusDTO:
        """执行一次最小 PostgreSQL 连接探针。"""
        try:
            with self._session_factory() as session:
                session.execute(text("SELECT 1"))
            return ConsoleHealthStatusDTO(name="postgres", status="ok", checked_at=checked_at)
        except Exception:
            logger.exception("console_postgres_health_failed")
            return ConsoleHealthStatusDTO(
                name="postgres", status="error", detail="数据库探针失败", checked_at=checked_at
            )

    @staticmethod
    def _check_redis(checked_at: datetime) -> ConsoleHealthStatusDTO:
        """执行一次 Redis ping，不将连接错误正文返回给 Console。"""
        try:
            Redis.from_url(get_settings().redis_url, socket_connect_timeout=2).ping()
            return ConsoleHealthStatusDTO(name="redis", status="ok", checked_at=checked_at)
        except Exception:
            logger.exception("console_redis_health_failed")
            return ConsoleHealthStatusDTO(
                name="redis", status="error", detail="Redis 探针失败", checked_at=checked_at
            )

    def _check_smart_table(self, checked_at: datetime) -> ConsoleHealthStatusDTO:
        """复用 SmartTableReadinessChecker 返回智能表格配置状态。"""
        try:
            report = SmartTableReadinessChecker().check(self._smart_table_adapter)
            return ConsoleHealthStatusDTO(
                name="smart_table",
                status="ok" if report.ready else "not_ready",
                detail="；".join(report.issues) if report.issues else None,
                checked_at=checked_at,
            )
        except Exception as error:
            # 只记录异常类型，避免健康状态日志携带 traceback 或外部响应正文。
            logger.error(
                "console_smart_table_health_failed",
                extra={"error_type": type(error).__name__},
            )
            return ConsoleHealthStatusDTO(
                name="smart_table", status="error", detail="智能表格探针失败", checked_at=checked_at
            )
