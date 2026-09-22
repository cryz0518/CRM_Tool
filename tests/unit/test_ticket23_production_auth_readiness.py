"""T23 Phase 1 认证边界、Provider policy、Break-glass 和 readiness 测试。"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.console.auth import (
    AdminCredentials,
    AdminPrincipal,
    AuthorizationDecision,
    ConsoleCapability,
    DevelopmentAdminIdentityProvider,
)
from app.console.routes import BreakGlassRequestBody
from app.core.config import Settings
from app.core.provider_policy import ProviderPolicy
from app.core.readiness import ReadinessRegistry, not_ready_component
from app.core.request_id import normalize_request_id


def test_authenticated_principal_normalizes_claims_and_groups_without_credentials() -> None:
    """验证稳定主体只保留规范化声明，不保存 token、secret 或 raw credential。"""
    principal = AdminPrincipal(
        " admin-1 ",
        frozenset({"administrator"}),
        "Development",
        authenticated_at=datetime.now(UTC),
        claims={" Groups ": "administrator"},
        groups=frozenset({" administrator ", "operations_admin"}),
    )

    assert principal.subject == "admin-1"
    assert principal.provider == "development"
    assert principal.groups == frozenset({"administrator", "operations_admin"})
    assert principal.claims == {"groups": "administrator"}
    assert not hasattr(principal, "token")
    assert not hasattr(principal, "secret")


def test_invalid_identity_is_denied() -> None:
    """验证错误开发凭据不会产生已认证主体。"""
    provider = DevelopmentAdminIdentityProvider(
        token="known-token", subject="admin-1", roles=frozenset({"administrator"})
    )

    assert provider.authenticate(AdminCredentials("wrong-token")) is None


def test_production_missing_and_development_provider_are_not_ready() -> None:
    """验证生产缺失 Provider 或开发 Provider 均被统一 policy 拒绝。"""
    policy = ProviderPolicy("production")

    assert policy.evaluate("admin_identity_provider", None).reason_code == "provider_missing"
    assert (
        policy.evaluate("admin_identity_provider", "development").reason_code
        == "production_test_provider_forbidden"
    )


def test_administrator_capability_is_allowed_and_non_admin_is_denied() -> None:
    """验证管理员 capability 允许且普通销售不能继承管理员能力。"""
    provider = DevelopmentAdminIdentityProvider(
        token="token", subject="admin-1", roles=frozenset({"administrator"})
    )
    admin = provider.authenticate(AdminCredentials("token"))
    assert admin is not None
    assert provider.authorize(admin, ConsoleCapability.CONSOLE_READ)

    sales_provider = DevelopmentAdminIdentityProvider(
        token="token", subject="sales-1", roles=frozenset({"sales"})
    )
    sales = sales_provider.authenticate(AdminCredentials("token"))
    assert sales is not None
    assert not sales_provider.authorize(sales, ConsoleCapability.CONSOLE_READ)


def test_break_glass_body_cannot_override_verified_actor_or_role() -> None:
    """验证 operator 和 role 不是请求体字段，调用方不能伪造审计主体。"""
    with pytest.raises(ValidationError):
        BreakGlassRequestBody.model_validate(
            {
                "object_type": "message",
                "object_id": "message-1",
                "access_type": "view_raw_message",
                "reason": "排查",
                "operator": "attacker",
            }
        )
    with pytest.raises(ValidationError):
        BreakGlassRequestBody.model_validate(
            {
                "object_type": "message",
                "object_id": "message-1",
                "access_type": "view_raw_message",
                "reason": "排查",
                "role": "administrator",
            }
        )


def test_capability_decision_uses_verified_role_not_set_sorting() -> None:
    """验证多角色主体按显式安全优先级产生可审计角色。"""
    provider = DevelopmentAdminIdentityProvider(
        token="token",
        subject="admin-1",
        roles=frozenset({"auditor", "administrator"}),
    )
    principal = provider.authenticate(AdminCredentials("token"))
    assert principal is not None

    decision = provider.decide(principal, ConsoleCapability.BREAK_GLASS_RAW_MESSAGE)

    assert decision == AuthorizationDecision(
        allowed=True,
        capability=ConsoleCapability.BREAK_GLASS_RAW_MESSAGE,
        basis="verified_group:administrator",
        role="administrator",
    )


def test_request_id_invalid_input_gets_new_server_id() -> None:
    """验证非法、超长和不可信 request id 不会直接进入审计链路。"""
    for value in ("bad value", "\r\nforged", "x" * 129):
        normalized = normalize_request_id(value)
        assert normalized != value
        assert len(normalized) == 36

    assert normalize_request_id("request-1") == "request-1"


def test_production_test_providers_are_not_ready() -> None:
    """验证 production 下 mock/fake/local/noop/unconfigured 全部 fail closed。"""
    policy = ProviderPolicy("production")
    for provider in ("mock", "fake", "local", "noop", "unconfigured"):
        assert policy.evaluate("storage", provider).status == "not_ready"
        assert policy.evaluate("storage", provider).reason_code in {
            "provider_missing",
            "production_test_provider_forbidden",
        }


def test_readiness_reports_stable_reason_code_without_exception_details() -> None:
    """验证 readiness 缺失依赖返回稳定 reason_code，不泄露异常正文、token 或 secret。"""
    registry = ReadinessRegistry(
        {
            "redis": lambda: not_ready_component("redis", "redis_unavailable"),
        }
    )

    payload = registry.check().as_dict()

    assert payload == {
        "status": "error",
        "components": [
            {"component": "redis", "status": "not_ready", "reason_code": "redis_unavailable"}
        ],
        "issues": ["redis:redis_unavailable"],
    }
    assert "token" not in str(payload).lower()
    assert "secret" not in str(payload).lower()


def test_settings_policy_covers_all_required_provider_components() -> None:
    """验证统一 policy 注册 T23 要求的全部外部 Provider 组件。"""
    results = ProviderPolicy("production").evaluate_settings(
        Settings(_env_file=None, smart_table_adapter="mock")
    )

    assert {
        result.component for result in results
    } == {
        "admin_identity_provider",
        "smart_table",
        "crm",
        "llm",
        "ocr",
        "asr",
        "storage",
        "scanner",
    }
