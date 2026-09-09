"""FastAPI 应用入口，仅提供 T01 所需健康检查。"""

from __future__ import annotations

import logging
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import RequestResponseEndpoint

from app.core.config import get_settings
from app.core.logging import bind_log_context, configure_logging, reset_log_context
from app.smart_table.adapter import SmartTableAdapter
from app.smart_table.dependencies import get_smart_table_adapter
from app.smart_table.readiness import SmartTableReadinessChecker

settings = get_settings()
configure_logging(settings.log_level, environment=settings.app_env, service="app")
logger = logging.getLogger(__name__)

app = FastAPI(title="CRM 线索自动录入", version="0.1.0")


@app.middleware("http")
async def request_id_middleware(request: Request, call_next: RequestResponseEndpoint) -> Response:
    """为每个 HTTP 请求绑定请求标识并写入响应头和结构化日志。"""
    request_id = request.headers.get("X-Request-ID", str(uuid4()))
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


@app.get("/health/ready")
async def readiness(
    adapter: Annotated[SmartTableAdapter, Depends(get_smart_table_adapter)],
) -> JSONResponse:
    """校验管理员预配置的智能表格结构与权限，并返回就绪状态。

    参数：adapter 为依赖注入的智能表格适配器。
    返回：配置正确时返回 200；缺失字段、权限或适配器时返回 503 与脱敏问题摘要。
    副作用：调用适配器读取结构和权限，并写入结构化就绪检查日志。
    """
    report = SmartTableReadinessChecker().check(adapter)
    if report.ready:
        return JSONResponse({"status": "ok", "service": "app", "issues": []})

    # 配置错误需要阻止服务接收业务流量，同时保留可操作的中文排障信息。
    return JSONResponse(
        {"status": "error", "service": "app", "issues": list(report.issues)},
        status_code=503,
    )
