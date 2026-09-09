"""健康检查接口测试。"""

import asyncio

import httpx

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
