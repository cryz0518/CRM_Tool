"""健康检查接口测试。"""

import asyncio

import httpx

from app import healthcheck
from app.main import app


def test_health_returns_service_status_and_request_id() -> None:
    """验证健康端点返回存活状态并回传客户端请求标识。"""
    async def request_health() -> httpx.Response:
        """通过 ASGI 传输调用健康检查接口，避免启动真实网络服务。"""
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/health", headers={"X-Request-ID": "test-request-id"})

    response = asyncio.run(request_health())

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "app"}
    assert response.headers["X-Request-ID"] == "test-request-id"


def test_container_healthcheck_calls_readiness_endpoint(monkeypatch: object) -> None:
    """验证 Docker 应用探针调用 readiness 而非仅检查进程存活。"""
    requested_urls: list[str] = []

    class SuccessfulResponse:
        """模拟可由 urlopen 上下文管理器返回的成功响应。"""

        status = 200

        def __enter__(self) -> "SuccessfulResponse":
            """进入模拟响应上下文并返回当前对象。"""
            return self

        def __exit__(self, *_: object) -> None:
            """退出模拟响应上下文，无需清理外部资源。"""

    def fake_urlopen(url: str, *, timeout: int) -> SuccessfulResponse:
        """记录探针 URL 并返回成功响应，避免发起真实网络请求。"""
        requested_urls.append(url)
        assert timeout == 2
        return SuccessfulResponse()

    monkeypatch.setattr(healthcheck, "urlopen", fake_urlopen)  # type: ignore[attr-defined]

    healthcheck.check_app()

    assert requested_urls == ["http://127.0.0.1:8000/health/ready"]
