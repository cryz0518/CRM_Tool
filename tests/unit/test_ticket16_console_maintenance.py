"""T16 Console maintenance 写入口的编排与能力测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Generator

import httpx
import pytest

from app.console.auth import AdminIdentityProvider, DevelopmentAdminIdentityProvider
from app.console.dependencies import get_admin_identity_provider, get_console_maintenance_service
from app.console.maintenance import ConsoleMaintenanceResult
from app.main import app


class StubMaintenanceService:
    """验证路由只调用 domain facade，不直接读取 ORM。"""

    def __init__(self) -> None:
        """初始化调用记录。"""

        self.calls: list[tuple[str, str]] = []

    def require_admin(self, principal: object) -> None:
        """接受已经由测试 provider 认证的管理员主体。"""

        del principal

    def retry_failed_message(self, principal: object, **kwargs: object) -> ConsoleMaintenanceResult:
        """返回固定 retry 结果。"""

        del principal
        self.calls.append(("retry", str(kwargs["message_id"])))
        return ConsoleMaintenanceResult(
            "attempt-1", "succeeded", message_id=str(kwargs["message_id"])
        )

    def reassign(self, principal: object, **kwargs: object) -> ConsoleMaintenanceResult:
        """返回固定 reassign 结果。"""

        del principal
        self.calls.append(("reassign", str(kwargs["message_id"])))
        return ConsoleMaintenanceResult(None, "succeeded", message_id=str(kwargs["message_id"]))

    def discard(self, principal: object, **kwargs: object) -> ConsoleMaintenanceResult:
        """返回固定 discard 结果。"""

        del principal
        self.calls.append(("discard", str(kwargs["lead_id"])))
        return ConsoleMaintenanceResult(None, "discarded", lead_id=str(kwargs["lead_id"]))

    def create_lead(self, principal: object, **kwargs: object) -> ConsoleMaintenanceResult:
        """返回固定 create 结果。"""

        del principal
        self.calls.append(("create", "lead"))
        return ConsoleMaintenanceResult("operation-1", "succeeded", lead_id="lead-1")

    def transfer_owner(self, principal: object, **kwargs: object) -> ConsoleMaintenanceResult:
        """返回固定 transfer 结果。"""

        del principal
        self.calls.append(("transfer", str(kwargs["lead_id"])))
        return ConsoleMaintenanceResult("operation-2", "succeeded", lead_id=str(kwargs["lead_id"]))


def request(method: str, url: str, **kwargs: object) -> httpx.Response:
    """通过 ASGI transport 发起同步测试请求。"""

    async def send() -> httpx.Response:
        """执行一次 HTTP 请求。"""

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, url, **kwargs)

    return asyncio.run(send())


@pytest.fixture(autouse=True)
def overrides() -> Generator[StubMaintenanceService, None, None]:
    """注入可观察的 provider 和 maintenance facade。"""

    provider: AdminIdentityProvider = DevelopmentAdminIdentityProvider(
        token="admin-token", subject="admin-1", roles=frozenset({"administrator"})
    )
    service = StubMaintenanceService()
    app.dependency_overrides[get_admin_identity_provider] = lambda: provider
    app.dependency_overrides[get_console_maintenance_service] = lambda: service
    yield service
    app.dependency_overrides.pop(get_admin_identity_provider, None)
    app.dependency_overrides.pop(get_console_maintenance_service, None)


def test_operations_admin_is_rejected_from_maintenance_write() -> None:
    """验证只读运维角色不能进入管理写入口。"""

    provider = DevelopmentAdminIdentityProvider(
        token="ops-token", subject="ops-1", roles=frozenset({"operations_admin"})
    )
    app.dependency_overrides[get_admin_identity_provider] = lambda: provider
    response = request(
        "POST",
        "/api/console/maintenance/leads/lead-1/discard",
        headers={"X-Console-Admin-Token": "ops-token"},
        json={"reason": "清理测试线索"},
    )
    assert response.status_code == 403


def test_maintenance_routes_delegate_to_facade_and_reject_extra_fields(
    overrides: StubMaintenanceService,
) -> None:
    """验证路由只做输入/认证编排，并拒绝额外字段。"""

    headers = {"X-Console-Admin-Token": "admin-token", "X-Request-ID": "request-1"}
    response = request(
        "POST",
        "/api/console/maintenance/leads/lead-1/discard",
        headers=headers,
        json={"reason": "清理测试线索", "unsafe": True},
    )
    assert response.status_code == 422

    response = request(
        "POST",
        "/api/console/maintenance/leads/lead-1/smart-table-owner-transfer",
        headers=headers,
        json={"new_owner_user_id": "sales-new", "reason": "组织调整"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"
    assert overrides.calls == [("transfer", "lead-1")]
