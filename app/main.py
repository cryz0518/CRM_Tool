"""FastAPI 应用入口，仅提供 T01 所需健康检查。"""

from __future__ import annotations

import logging
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import RequestResponseEndpoint

from app.core.config import get_settings
from app.core.logging import bind_log_context, configure_logging, reset_log_context

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
