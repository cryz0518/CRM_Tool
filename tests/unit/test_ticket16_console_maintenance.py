"""T16 Console maintenance 写入口的编排与能力测试。"""

from __future__ import annotations

import asyncio
from collections.abc import Generator

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.console.auth import AdminIdentityProvider, DevelopmentAdminIdentityProvider
from app.console.dependencies import (
    get_admin_identity_provider,
    get_capability_authorizer,
    get_console_maintenance_service,
)
from app.console.maintenance import ConsoleMaintenanceResult, ConsoleMaintenanceService
from app.core.failures import safe_audit_text
from app.main import app
from app.messaging.models import Base


class StubMaintenanceService:
    """验证路由只调用 domain facade，不直接读取 ORM。"""

    def __init__(self) -> None:
        """初始化调用记录。"""

        self.calls: list[tuple[str, str]] = []
        self.request_ids: list[str] = []

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
        self.request_ids.append(str(kwargs["request_id"]))
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

    def reconcile_transfer(self, principal: object, **kwargs: object) -> ConsoleMaintenanceResult:
        """返回固定 transfer recovery 结果。"""

        del principal
        self.calls.append(("transfer_reconcile", str(kwargs["operation_id"])))
        return ConsoleMaintenanceResult("operation-2", "succeeded", lead_id="lead-1")

    def reconcile_create(self, principal: object, **kwargs: object) -> ConsoleMaintenanceResult:
        """返回固定 admin-create recovery 结果。"""

        del principal
        self.calls.append(("create_reconcile", str(kwargs["operation_id"])))
        return ConsoleMaintenanceResult("operation-1", "succeeded", lead_id="lead-1")


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
    app.dependency_overrides[get_capability_authorizer] = lambda: app.dependency_overrides[
        get_admin_identity_provider
    ]()
    app.dependency_overrides[get_console_maintenance_service] = lambda: service
    yield service
    app.dependency_overrides.pop(get_admin_identity_provider, None)
    app.dependency_overrides.pop(get_capability_authorizer, None)
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


def test_recovery_routes_delegate_to_facade(overrides: StubMaintenanceService) -> None:
    """验证未知远端结果只能通过显式 recovery domain 入口收敛。"""

    headers = {"X-Console-Admin-Token": "admin-token", "X-Request-ID": "recovery-1"}
    response = request(
        "POST",
        "/api/console/maintenance/transfer-operations/operation-2/reconcile",
        headers=headers,
        json={"reason": "核验远端负责人"},
    )
    assert response.status_code == 200
    response = request(
        "POST",
        "/api/console/maintenance/creation-operations/operation-1/reconcile",
        headers={**headers, "X-Request-ID": "recovery-2"},
        json={"reason": "核验远端补建"},
    )
    assert response.status_code == 200
    assert overrides.calls[-2:] == [
        ("transfer_reconcile", "operation-2"),
        ("create_reconcile", "operation-1"),
    ]


def test_request_id_is_normalized_by_middleware_for_all_maintenance_requests(
    overrides: StubMaintenanceService,
) -> None:
    """验证非法、超长、缺失和合法 request id 均只使用 middleware 结果。"""

    for value in ("bad\nrequest", "x" * 129):
        response = request(
            "POST",
            "/api/console/maintenance/leads/lead-1/discard",
            headers={"X-Console-Admin-Token": "admin-token", "X-Request-ID": value},
            json={"reason": "清理测试线索"},
        )
        assert response.status_code == 200
        assert response.headers["X-Request-ID"] != value
        assert len(overrides.request_ids[-1]) == 36

    response = request(
        "POST",
        "/api/console/maintenance/leads/lead-1/discard",
        headers={"X-Console-Admin-Token": "admin-token", "X-Request-ID": "request-valid"},
        json={"reason": "清理测试线索"},
    )
    assert response.status_code == 200
    assert overrides.request_ids[-1] == "request-valid"

    response = request(
        "POST",
        "/api/console/maintenance/leads/lead-1/discard",
        headers={"X-Console-Admin-Token": "admin-token"},
        json={"reason": "清理测试线索"},
    )
    assert response.status_code == 200
    assert len(overrides.request_ids[-1]) == 36


def test_audit_reason_masks_sensitive_contact_and_credentials() -> None:
    """验证管理原因持久化前不会保留完整联系方式或 token。"""

    masked = safe_audit_text("联系人 13800000000 a.person@example.com token:secret-value")
    assert "13800000000" not in masked
    assert "a.person@example.com" not in masked
    assert "secret-value" not in masked


def test_console_audit_is_written_before_side_effect_and_replayed() -> None:
    """验证同一 request_id 重试不会再次进入领域副作用。"""

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine)
    service = ConsoleMaintenanceService(factory, object(), object(), object(), object(), object())
    principal = type(
        "Principal",
        (),
        {"subject": "admin-1", "auth_source": "test", "roles": frozenset({"administrator"})},
    )()

    assert (
        service._begin_audit(
            "audit-idempotent",
            "discard",
            "lead",
            "lead-1",
            principal,
            before={"lifecycle_state": "pending_create"},
            reason="reason",
        )
        is None
    )
    replay = service._begin_audit(
        "audit-idempotent",
        "discard",
        "lead",
        "lead-1",
        principal,
        before={},
        reason="reason",
    )
    assert replay is not None and replay.status == "processing"
    service._audit_success(
        "audit-idempotent",
        "discard",
        "lead",
        "lead-1",
        principal,
        before={"lifecycle_state": "pending_create"},
        after={"lifecycle_state": "discarded"},
        reason="reason",
    )
    replay = service._begin_audit(
        "audit-idempotent",
        "discard",
        "lead",
        "lead-1",
        principal,
        before={},
        reason="reason",
    )
    assert replay is not None and replay.status == "succeeded"
