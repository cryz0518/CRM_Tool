"""T23 Phase 1 认证边界、Provider policy、Break-glass 和 readiness 测试。"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.ai import dependencies as ai_dependencies
from app.ai.provider import MockLLMProvider
from app.console.auth import (
    AdminCredentials,
    AdminPrincipal,
    AuthenticatedPrincipal,
    AuthorizationDecision,
    ConsoleCapability,
    DevelopmentAdminIdentityProvider,
    LocalCapabilityAuthorizer,
)
from app.console.break_glass import (
    BreakGlassAccessRequest,
    BreakGlassAccessService,
)
from app.console.routes import BreakGlassRequestBody
from app.core.config import Settings
from app.core.heartbeat import check_heartbeat, heartbeat_key, publish_heartbeat
from app.core.provider_policy import ProviderPolicy, ProviderPolicyError
from app.core.readiness import ReadinessComponent, ReadinessRegistry, not_ready_component
from app.core.request_id import normalize_request_id
from app.media import dependencies as media_dependencies
from app.media.providers import MockASRProvider, MockOCRProvider
from app.media.readiness import MediaReadinessReport
from app.messaging.models import Base, SalesAuthorization


class FakeRedis:
    """提供 heartbeat 单元测试所需的最小 Redis 行为。"""

    def __init__(self) -> None:
        """初始化 key、TTL 和最近写入参数。"""
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.last_expire: int | None = None

    def set(self, key: str, value: str, *, ex: int) -> bool:
        """保存值并记录 Redis TTL。"""
        self.values[key] = value
        self.ttls[key] = ex
        self.last_expire = ex
        return True

    def get(self, key: str) -> str | None:
        """读取测试 key。"""
        return self.values.get(key)

    def ttl(self, key: str) -> int:
        """返回测试 key 的剩余 TTL。"""
        return self.ttls.get(key, -2)


@pytest.fixture
def authorization_session_factory() -> sessionmaker[Session]:
    """提供本地 SalesAuthorization 授权查询使用的隔离数据库。"""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine)
    try:
        yield factory
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def _principal(subject: str) -> AuthenticatedPrincipal:
    """构造带 external administrator group 的测试主体。"""
    return AuthenticatedPrincipal(
        subject=subject,
        provider="external-test",
        authenticated_at=datetime.now(UTC),
        groups=frozenset({"administrator"}),
    )


def test_authenticated_principal_normalizes_claims_and_groups_without_credentials() -> None:
    """验证稳定主体只保留规范化声明，不保存 token、secret 或 raw credential。"""
    principal = AdminPrincipal(
        " admin-1 ",
        frozenset({"administrator"}),
        "Development",
        authenticated_at=datetime.now(UTC),
        claims={
            " Groups ": "administrator",
            "access_token": "token-value",
            "jwt": "header.payload.signature",
            "authorization": "Bearer token-value",
            "role": "admin",
        },
        groups=frozenset({" administrator ", "operations_admin"}),
    )

    assert principal.subject == "admin-1"
    assert principal.provider == "development"
    assert principal.groups == frozenset({"administrator", "operations_admin"})
    assert principal.claims == {"groups": "administrator", "role": "admin"}
    assert "access_token" not in principal.claims
    assert "jwt" not in principal.claims
    assert "authorization" not in principal.claims
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


def test_external_admin_group_without_local_authorization_is_denied(
    authorization_session_factory: sessionmaker[Session],
) -> None:
    """验证外部 administrator group 不能单独授予本地 Console capability。"""
    authorizer = LocalCapabilityAuthorizer(authorization_session_factory)

    assert not authorizer.authorize(_principal("external-admin"), ConsoleCapability.CONSOLE_READ)


def test_inactive_local_administrator_is_denied(
    authorization_session_factory: sessionmaker[Session],
) -> None:
    """验证已撤销的本地管理员不能继续使用 Console capability。"""
    with authorization_session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="inactive-admin",
                is_authorized=True,
                is_active=False,
                is_administrator=True,
            )
        )

    authorizer = LocalCapabilityAuthorizer(authorization_session_factory)

    assert not authorizer.authorize(_principal("inactive-admin"), ConsoleCapability.CONSOLE_READ)


def test_unauthorized_local_administrator_is_denied(
    authorization_session_factory: sessionmaker[Session],
) -> None:
    """验证未获销售授权的管理员标记不能直接获得 Console capability。"""
    with authorization_session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="unauthorized-admin",
                is_authorized=False,
                is_active=True,
                is_administrator=True,
            )
        )

    authorizer = LocalCapabilityAuthorizer(authorization_session_factory)

    assert not authorizer.authorize(
        _principal("unauthorized-admin"), ConsoleCapability.CONSOLE_READ
    )


def test_active_local_administrator_is_allowed(
    authorization_session_factory: sessionmaker[Session],
) -> None:
    """验证 active administrator 是 Console capability 的本地授权事实。"""
    with authorization_session_factory.begin() as session:
        session.add(
            SalesAuthorization(
                wecom_user_id="active-admin",
                is_authorized=True,
                is_active=True,
                is_administrator=True,
            )
        )

    authorizer = LocalCapabilityAuthorizer(authorization_session_factory)
    decision = authorizer.decide(_principal("active-admin"), ConsoleCapability.AUDIT_READ)

    assert decision.allowed
    assert decision.basis == "local_sales_authorization"


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


def test_break_glass_service_rejects_arbitrary_principal(
    authorization_session_factory: sessionmaker[Session],
) -> None:
    """验证 Break-glass request 不再接受调用方手工构造的管理员主体。"""
    del authorization_session_factory
    with pytest.raises(TypeError):
        BreakGlassAccessRequest(
            principal=AdminPrincipal(
                "forged-admin",
                frozenset({"administrator"}),
                "external-test",
            ),
            object_type="message",
            object_id="message-1",
            access_type="view_raw_message",
            reason="安全排查",
            request_id="request-forged",
        )  # type: ignore[call-arg]


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


def test_unsupported_provider_and_missing_production_credential_are_not_ready() -> None:
    """验证未实现 Provider 和生产缺失 AI credential 都不会被视为 ready。"""
    policy = ProviderPolicy("production")
    assert policy.evaluate("admin_identity_provider", "oidc").reason_code == "provider_unavailable"
    result = policy.evaluate(
        "llm",
        "qwen",
        settings=Settings(_env_file=None, app_env="production", llm_provider="qwen"),
    )
    assert result.reason_code == "provider_configuration_missing"


@pytest.mark.parametrize("app_env", [" production ", "PRODUCTION"])
def test_provider_credential_check_uses_normalized_environment(app_env: str) -> None:
    """验证环境空白和大小写不会绕过生产 credential 检查。"""
    settings = Settings(_env_file=None, app_env=app_env, llm_provider="qwen")

    result = ProviderPolicy(settings.app_env).evaluate("llm", "qwen", settings=settings)

    assert result.status == "not_ready"
    assert result.reason_code == "provider_configuration_missing"


def test_provider_factories_use_one_normalized_provider_selection(
    authorization_session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """验证 LLM、OCR、ASR 统一消费 trim/lower 后的 provider 名称。"""
    settings = Settings(
        _env_file=None,
        app_env="development",
        llm_provider=" MOCK ",
        ocr_provider=" Mock ",
        asr_provider="MOCK",
        media_storage_provider=" local ",
        media_scanner_provider=" FAKE ",
        media_storage_path=str(tmp_path),
    )
    monkeypatch.setattr(ai_dependencies, "get_settings", lambda: settings)
    monkeypatch.setattr(media_dependencies, "get_settings", lambda: settings)

    gateway = ai_dependencies.get_ai_gateway()
    service = media_dependencies.get_media_attachment_service(authorization_session_factory)

    assert settings.llm_provider == "mock"
    assert settings.ocr_provider == "mock"
    assert settings.asr_provider == "mock"
    assert isinstance(gateway._provider, MockLLMProvider)
    assert isinstance(service._ocr_provider, MockOCRProvider)
    assert isinstance(service._asr_provider, MockASRProvider)


@pytest.mark.parametrize("provider_field", ["llm_provider", "ocr_provider", "asr_provider"])
def test_production_whitespace_mock_is_rejected_by_policy(provider_field: str) -> None:
    """验证生产环境中带空白或大小写伪装的 mock 仍 fail closed。"""
    settings = Settings(
        _env_file=None,
        app_env=" production ",
        **{provider_field: " MOCK "},
    )

    result = ProviderPolicy(settings.app_env).evaluate(
        provider_field.removesuffix("_provider"),
        getattr(settings, provider_field),
        settings=settings,
    )

    assert result.status == "not_ready"
    assert result.reason_code == "production_test_provider_forbidden"


def test_production_ai_and_media_factories_fail_closed_without_credential(
    authorization_session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证 Qwen LLM、OCR 和 ASR 工厂在生产缺少 key 时不会实例化 Provider。"""
    settings = Settings(
        _env_file=None,
        app_env="production",
        llm_provider="qwen",
        ocr_provider="qwen",
        asr_provider="qwen",
        media_retention_policy_version="test-v1",
        media_retention_days=30,
        message_payload_retention_days=30,
        notification_payload_retention_days=30,
    )
    monkeypatch.setattr(ai_dependencies, "get_settings", lambda: settings)
    monkeypatch.setattr(media_dependencies, "get_settings", lambda: settings)

    with pytest.raises(ProviderPolicyError):
        ai_dependencies.get_ai_gateway()
    with pytest.raises(ProviderPolicyError):
        media_dependencies.get_media_attachment_service(authorization_session_factory)


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


def test_readiness_registry_maps_media_failure_to_storage_and_scanner_components(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证媒体 readiness 失败统一映射到 storage/scanner 稳定组件码。"""
    import app.main as main_module

    monkeypatch.setattr(
        main_module.ProductionMediaReadinessChecker,
        "check",
        lambda _self, _settings: MediaReadinessReport(False, ("媒体配置不完整",)),
    )

    report, issues = main_module._readiness_report(object())  # type: ignore[arg-type]

    components: dict[str, ReadinessComponent] = {}
    for item in report.components:
        if item.component in {"storage", "scanner"}:
            components.setdefault(item.component, item)
    assert components["storage"].reason_code == "media_configuration_invalid"
    assert components["scanner"].reason_code == "media_configuration_invalid"
    assert "媒体配置不完整" in issues


def test_readiness_registry_hides_media_exception_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证媒体 readiness 异常只返回稳定 reason_code，不泄露外部响应。"""
    import app.main as main_module

    def raise_media_error(_self: object, _settings: Settings) -> MediaReadinessReport:
        """模拟携带 token、endpoint 和 secret 的外部异常。"""
        raise RuntimeError("provider token=secret endpoint=https://vendor.invalid")

    monkeypatch.setattr(main_module.ProductionMediaReadinessChecker, "check", raise_media_error)

    report, _issues = main_module._readiness_report(object())  # type: ignore[arg-type]
    payload = report.as_dict()

    assert {
        "component": "storage",
        "status": "not_ready",
        "reason_code": "dependency_unavailable",
    } in payload["components"]
    assert {
        "component": "scanner",
        "status": "not_ready",
        "reason_code": "dependency_unavailable",
    } in payload["components"]
    assert "secret" not in str(payload).lower()
    assert "vendor.invalid" not in str(payload).lower()


def test_verified_context_requires_authorizer_seal_and_service_rejects_authorizer_injection(
) -> None:
    """验证 Break-glass 上下文不能由调用方伪造，服务也不接受任意授权器。"""
    from app.console.auth import VerifiedAuthorizationContext

    with pytest.raises(TypeError):
        VerifiedAuthorizationContext(
            "admin-1",
            ConsoleCapability.BREAK_GLASS_RAW_MESSAGE,
            "forged",
            "external-test",
            "administrator",
            _seal=object(),
        )

    with pytest.raises(TypeError):
        BreakGlassAccessService(
            object(),  # type: ignore[arg-type]
            capability_authorizer=DevelopmentAdminIdentityProvider(
                token=None, subject="admin-1", roles=frozenset({"administrator"})
            ),
        )


def test_worker_scheduler_heartbeat_requires_fresh_ttl_and_instance() -> None:
    """验证 Worker/Scheduler heartbeat 缺失、过期和新鲜状态。"""
    redis = FakeRedis()

    assert check_heartbeat(redis, "worker").reason_code == "heartbeat_missing"  # type: ignore[arg-type]

    redis.values[heartbeat_key("worker")] = json.dumps(
        {"heartbeat_at": datetime.now(UTC).isoformat(), "instance_id": "worker-1"}
    )
    redis.ttls[heartbeat_key("worker")] = 0
    assert check_heartbeat(redis, "worker").reason_code == "heartbeat_expired"  # type: ignore[arg-type]

    publish_heartbeat(redis, "worker", "worker-1")  # type: ignore[arg-type]
    result = check_heartbeat(redis, "worker", now=time.time())  # type: ignore[arg-type]
    assert result.status == "ok"
    assert result.reason_code == "heartbeat_fresh"
    assert redis.last_expire == 30


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
