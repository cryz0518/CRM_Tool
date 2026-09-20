"""T15 Console HTTP 认证和只读路由测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Generator

import httpx
import pytest

from app.console.auth import AdminIdentityProvider, DevelopmentAdminIdentityProvider
from app.console.break_glass import BreakGlassAccessResult
from app.console.dependencies import (
    get_admin_identity_provider,
    get_break_glass_access_service,
    get_console_query_service,
)
from app.console.dto import (
    ConsoleLeadDTO,
    ConsoleOverviewDTO,
    ConsolePage,
    ConsoleSalesAuthorizationDTO,
)
from app.main import app


class StubConsoleQueryService:
    """提供 HTTP 路由所需的最小只读查询结果。"""

    def get_overview(self) -> ConsoleOverviewDTO:
        """返回没有敏感数据的首页结果。"""
        return ConsoleOverviewDTO(services=[], counts={}, readiness_issues=[])

    def list_sales_authorizations(
        self, *, limit: int = 50, cursor: str | None = None
    ) -> ConsolePage[ConsoleSalesAuthorizationDTO]:
        """返回固定的授权目录只读结果。"""
        del limit, cursor
        return ConsolePage(
            items=[
                ConsoleSalesAuthorizationDTO(
                    wecom_user_id="sales-1",
                    display_name="销售一",
                    department_id="sales",
                    is_authorized=True,
                    is_active=True,
                    crm_mapping_status="mapping_missing",
                    affected_pending_lead_count=2,
                )
            ]
        )

    def list_mapping_missing_leads(
        self, sales_user_id: str, *, limit: int = 50, cursor: str | None = None
    ) -> ConsolePage[ConsoleLeadDTO]:
        """返回固定的映射缺失受影响线索。"""
        del sales_user_id, limit, cursor
        return ConsolePage(items=[])


class StubBreakGlassService:
    """提供受保护原始消息端点的可控测试实现。"""

    def access(self, request: object) -> BreakGlassAccessResult:
        """返回单个测试对象的原始内容。"""
        del request
        return BreakGlassAccessResult(
            object_type="message",
            object_id="message-1",
            access_type="view_raw_message",
            payload={"raw": "only-after-audit"},
        )


def request(method: str, url: str, **kwargs: object) -> httpx.Response:
    """通过 ASGI Transport 同步执行一次 Console 请求。"""
    async def send() -> httpx.Response:
        """在异步客户端中发起请求并关闭连接。"""
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, url, **kwargs)

    return asyncio.run(send())


@pytest.fixture(autouse=True)
def console_dependencies() -> Generator[None, None, None]:
    """为每个 HTTP 测试注入显式开发管理员和假的 Query Service。"""
    provider: AdminIdentityProvider = DevelopmentAdminIdentityProvider(
        token="admin-token",
        subject="admin-1",
        roles=frozenset({"administrator"}),
    )
    app.dependency_overrides[get_admin_identity_provider] = lambda: provider
    app.dependency_overrides[get_console_query_service] = StubConsoleQueryService
    app.dependency_overrides[get_break_glass_access_service] = StubBreakGlassService
    yield
    app.dependency_overrides.pop(get_admin_identity_provider, None)
    app.dependency_overrides.pop(get_console_query_service, None)
    app.dependency_overrides.pop(get_break_glass_access_service, None)


def test_console_requires_authentication() -> None:
    """验证未认证请求返回 401，且不会进入查询服务。"""
    response = request("GET", "/api/console/overview")

    assert response.status_code == 401


def test_console_rejects_non_admin_role() -> None:
    """验证已认证但无 Console 能力的主体返回 403。"""
    provider = DevelopmentAdminIdentityProvider(
        token="sales-token",
        subject="sales-1",
        roles=frozenset({"sales"}),
    )
    app.dependency_overrides[get_admin_identity_provider] = lambda: provider

    response = request(
        "GET",
        "/api/console/overview",
        headers={"X-Console-Admin-Token": "sales-token"},
    )

    assert response.status_code == 403


def test_admin_can_read_overview_and_console_shell() -> None:
    """验证管理员可访问首页数据和内部 Console 页面。"""
    headers = {"X-Console-Admin-Token": "admin-token"}

    overview = request("GET", "/api/console/overview", headers=headers)
    shell = request("GET", "/console", headers=headers)

    assert overview.status_code == 200
    assert overview.json() == {"services": [], "counts": {}, "readiness_issues": []}
    assert shell.status_code == 200
    assert "Operations Console" in shell.text
    assert 'data-path="conflicts"' in shell.text


def test_admin_can_read_sales_authorization_mapping_status() -> None:
    """验证管理员可读取销售授权范围和 CRM 映射异常。"""
    response = request(
        "GET",
        "/api/console/sales-authorizations",
        headers={"X-Console-Admin-Token": "admin-token"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "wecom_user_id": "sales-1",
                "display_name": "销售一",
                "department_id": "sales",
                "is_authorized": True,
                "is_active": True,
                "crm_mapping_status": "mapping_missing",
                "affected_pending_lead_count": 2,
                "created_by": None,
                "updated_by": None,
            }
        ],
        "next_cursor": None,
    }


def test_admin_can_read_mapping_missing_affected_leads() -> None:
    """验证管理员可通过只读端点定位映射缺失受影响线索。"""
    response = request(
        "GET",
        "/api/console/sales-authorizations/sales-1/affected-leads",
        headers={"X-Console-Admin-Token": "admin-token"},
    )

    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}


def test_break_glass_requires_reason_and_rejects_batch_fields() -> None:
    """验证 HTTP Break-glass 不接受空原因或批量对象字段。"""
    headers = {"X-Console-Admin-Token": "admin-token"}

    missing_reason = request(
        "POST",
        "/api/console/break-glass/access",
        headers=headers,
        json={
            "object_type": "message",
            "object_id": "message-1",
            "access_type": "view_raw_message",
            "reason": "",
        },
    )
    batch = request(
        "POST",
        "/api/console/break-glass/access",
        headers=headers,
        json={
            "object_type": "message",
            "object_id": "message-1",
            "object_ids": ["message-1", "message-2"],
            "access_type": "view_raw_message",
            "reason": "排查单条消息",
        },
    )

    assert missing_reason.status_code == 422
    assert batch.status_code == 422


def test_break_glass_returns_raw_data_only_after_protected_route() -> None:
    """验证管理员可在显式原因和能力校验后访问单个原始对象。"""
    response = request(
        "POST",
        "/api/console/break-glass/access",
        headers={"X-Console-Admin-Token": "admin-token"},
        json={
            "object_type": "message",
            "object_id": "message-1",
            "access_type": "view_raw_message",
            "reason": "排查 T14 消息失败",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"raw": "only-after-audit"}
    assert response.headers["cache-control"] == "no-store"
