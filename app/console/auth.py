"""Operations Console 管理员身份认证 Seam。"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from sqlalchemy.orm import Session, sessionmaker

from app.messaging.models import SalesAuthorization

logger = logging.getLogger(__name__)

_SENSITIVE_CLAIM_MARKERS = (
    "token",
    "secret",
    "credential",
    "authorization",
    "password",
    "cookie",
    "jwt",
)


def _normalize_safe_claims(claims: Mapping[str, str]) -> dict[str, str]:
    """规范化并过滤 Provider 声明，只保留非敏感身份元数据。

    参数：claims 为认证 Provider 提供的声明映射。
    返回值：不含 token、secret、credential、授权头或 raw JWT 的声明字典。
    异常：无；无法信任的声明键和值会被丢弃。
    副作用：不保存原始 Provider payload，也不修改调用方输入。
    """
    safe_claims: dict[str, str] = {}
    for key, value in claims.items():
        normalized_key = str(key).strip().lower().replace("-", "_")
        normalized_value = str(value).strip()
        if not normalized_key or not normalized_value:
            continue
        if any(marker in normalized_key for marker in _SENSITIVE_CLAIM_MARKERS):
            continue
        safe_claims[normalized_key] = normalized_value
    return safe_claims


class ConsoleCapability(StrEnum):
    """定义 Console 可授予的最小能力集合。"""

    CONSOLE_READ = "console_read"
    CONSOLE_MAINTENANCE_WRITE = "console_maintenance_write"
    AUDIT_READ = "audit_read"
    BREAK_GLASS_RAW_MESSAGE = "break_glass_raw_message"
    BREAK_GLASS_FULL_CONTACT = "break_glass_full_contact"
    BREAK_GLASS_PREVIEW_ATTACHMENT = "break_glass_preview_attachment"
    BREAK_GLASS_DOWNLOAD_ATTACHMENT = "break_glass_download_attachment"


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    """表示认证边界输出的稳定主体，不保存任何原始凭据。

    参数：subject 为主体标识；provider 为认证 Provider；authenticated_at 为认证时间；
    claims 和 groups 为已规范化的非敏感声明；tenant 和 display_name 为可选展示信息。
    返回值：无。
    异常：无。
    副作用：无；对象不含 JWT、refresh token、secret 或 raw credential。
    """

    subject: str
    provider: str
    authenticated_at: datetime
    claims: Mapping[str, str] = field(default_factory=dict)
    groups: frozenset[str] = field(default_factory=frozenset)
    tenant: str | None = None
    display_name: str | None = None

    def __post_init__(self) -> None:
        """规范化主体标识、Provider、claims key 和 groups。

        返回值：无。
        异常：主体或 Provider 为空时抛出 ValueError。
        副作用：用脱敏的不可变值替换输入映射，避免后续调用方修改主体事实。
        """
        subject = self.subject.strip()
        provider = self.provider.strip().lower()
        if not subject or not provider:
            raise ValueError("authenticated principal 必须包含 subject 和 provider")
        normalized_claims = _normalize_safe_claims(self.claims)
        normalized_groups = frozenset(
            group.strip() for group in self.groups if group.strip()
        )
        object.__setattr__(self, "subject", subject)
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "claims", MappingProxyType(normalized_claims))
        object.__setattr__(self, "groups", normalized_groups)


@dataclass(frozen=True)
class AuthorizationDecision:
    """保存 capability 授权结果及其可审计依据。"""

    allowed: bool
    capability: ConsoleCapability
    basis: str
    role: str | None = None


_VERIFIED_CONTEXT_SEAL = object()


@dataclass(frozen=True, init=False)
class VerifiedAuthorizationContext:
    """承载本地授权完成后的不可变 Break-glass 授权事实。

    参数：仅由 CapabilityAuthorizer 的已验证实现通过内部 seal 创建。
    返回值：包含 subject、provider、capability、授权依据和稳定角色的上下文。
    异常：外部尝试直接构造上下文时抛出 TypeError。
    副作用：无；不保存原始凭据或 Provider payload。
    """

    verified_subject: str
    capability: ConsoleCapability
    authorization_basis: str
    provider: str
    role: str | None

    def __init__(
        self,
        verified_subject: str,
        capability: ConsoleCapability,
        authorization_basis: str,
        provider: str,
        role: str | None,
        *,
        _seal: object,
    ) -> None:
        """仅允许授权器内部 seal 创建已验证上下文。"""
        if _seal is not _VERIFIED_CONTEXT_SEAL:
            raise TypeError("VerifiedAuthorizationContext 只能由授权器创建")
        if not verified_subject.strip() or not provider.strip() or not authorization_basis.strip():
            raise ValueError("已验证授权上下文缺少稳定授权事实")
        object.__setattr__(self, "verified_subject", verified_subject.strip())
        object.__setattr__(self, "capability", capability)
        object.__setattr__(self, "authorization_basis", authorization_basis.strip())
        object.__setattr__(self, "provider", provider.strip().lower())
        object.__setattr__(self, "role", role.strip() if role and role.strip() else None)

    @classmethod
    def _from_decision(
        cls, principal: AuthenticatedPrincipal, decision: AuthorizationDecision
    ) -> VerifiedAuthorizationContext | None:
        """把已允许的授权判定封装为服务边界上下文。"""
        if not decision.allowed or not decision.role:
            return None
        return cls(
            principal.subject,
            decision.capability,
            decision.basis,
            principal.provider,
            decision.role,
            _seal=_VERIFIED_CONTEXT_SEAL,
        )


def _principal_roles(principal: AuthenticatedPrincipal) -> frozenset[str]:
    """从稳定主体取得已验证角色/群组，不接受请求体中的覆盖值。

    参数：principal 为认证边界输出的主体。
    返回值：规范化的角色集合。
    异常：无。
    副作用：无。
    """
    explicit_roles: frozenset[str] = getattr(principal, "roles", frozenset())
    return frozenset(explicit_roles) | principal.groups


@dataclass(frozen=True)
class AdminCredentials:
    """承载认证 Provider 所需的请求凭据，不保存原始请求对象。"""

    token: str | None


@dataclass(frozen=True, init=False, eq=False)
class AdminPrincipal(AuthenticatedPrincipal):
    """表示已通过认证的管理员主体及其认证来源。"""

    roles: frozenset[str] = field(default_factory=frozenset)
    auth_source: str = "unknown"

    def __init__(
        self,
        subject: str,
        roles: frozenset[str],
        auth_source: str,
        *,
        authenticated_at: datetime | None = None,
        claims: Mapping[str, str] | None = None,
        groups: frozenset[str] | None = None,
        tenant: str | None = None,
        display_name: str | None = None,
    ) -> None:
        """保留 T15/T16 构造方式，同时初始化稳定认证主体字段。

        参数：subject、roles、auth_source 保持旧接口；其余参数为通用主体字段。
        返回值：无。
        异常：主体或认证来源为空时抛出 ValueError。
        副作用：只在内存中生成主体，不保存请求凭据。
        """
        subject = subject.strip()
        auth_source = auth_source.strip().lower()
        roles = frozenset(role.strip() for role in roles if role.strip())
        if not subject or not auth_source:
            raise ValueError("AdminPrincipal 必须包含 subject 和 auth_source")
        claims = _normalize_safe_claims(claims or {})
        groups = frozenset(
            group.strip() for group in ((groups or frozenset()) | roles) if group.strip()
        )
        object.__setattr__(self, "subject", subject)
        object.__setattr__(
            self, "provider", auth_source
        )
        object.__setattr__(self, "authenticated_at", authenticated_at or datetime.now(UTC))
        object.__setattr__(self, "tenant", tenant)
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "auth_source", auth_source)
        object.__setattr__(self, "roles", roles)
        object.__setattr__(self, "claims", MappingProxyType(claims))
        object.__setattr__(self, "groups", groups)

    def __eq__(self, other: object) -> bool:
        """按旧主体核心字段比较，保持既有 T15 测试和调用方兼容。"""
        return isinstance(other, AdminPrincipal) and (
            self.subject,
            self.roles,
            self.auth_source,
        ) == (other.subject, other.roles, other.auth_source)

    def __hash__(self) -> int:
        """按稳定主体核心字段生成哈希值。"""
        return hash((self.subject, self.roles, self.auth_source))


class AuthenticationProvider(Protocol):
    """定义认证边界，只返回稳定主体，不向业务层传递原始凭据。"""

    def authenticate(self, credentials: AdminCredentials) -> AuthenticatedPrincipal | None:
        """验证请求凭据，失败时返回 None。"""


class CapabilityAuthorizer(Protocol):
    """定义基于稳定主体执行 capability 授权的边界。"""

    def authorize(self, principal: AuthenticatedPrincipal, capability: ConsoleCapability) -> bool:
        """判断主体是否具备指定 Console 能力。"""

    def decide(
        self, principal: AuthenticatedPrincipal, capability: ConsoleCapability
    ) -> AuthorizationDecision:
        """返回可写入审计的授权判定及依据。"""

    def verified_context(
        self, principal: AuthenticatedPrincipal, capability: ConsoleCapability
    ) -> VerifiedAuthorizationContext | None:
        """仅在授权成功后生成不可变的已验证授权上下文。"""


class AdminIdentityProvider(AuthenticationProvider, CapabilityAuthorizer, Protocol):
    """兼容 T15/T16 的认证与授权组合 Seam。"""


class DevelopmentAdminIdentityProvider:
    """只接受显式配置 Token 的开发认证实现，禁止默认匿名访问。"""

    _CAPABILITY_ROLES = {
        ConsoleCapability.CONSOLE_READ: frozenset({"administrator", "operations_admin", "auditor"}),
        # 维护写入不能由只读运维或审计角色继承，业务层还会再次核对 SalesAuthorization。
        ConsoleCapability.CONSOLE_MAINTENANCE_WRITE: frozenset({"administrator"}),
        ConsoleCapability.AUDIT_READ: frozenset({"administrator", "operations_admin", "auditor"}),
        ConsoleCapability.BREAK_GLASS_RAW_MESSAGE: frozenset({"administrator", "operations_admin"}),
        ConsoleCapability.BREAK_GLASS_FULL_CONTACT: frozenset(
            {"administrator", "operations_admin"}
        ),
        ConsoleCapability.BREAK_GLASS_PREVIEW_ATTACHMENT: frozenset(
            {"administrator", "operations_admin"}
        ),
        ConsoleCapability.BREAK_GLASS_DOWNLOAD_ATTACHMENT: frozenset({"administrator"}),
    }

    def __init__(self, *, token: str | None, subject: str, roles: frozenset[str]) -> None:
        """保存开发环境显式认证配置。

        参数：token 为必须由部署注入的访问令牌；subject 为管理员主体；roles 为角色集合。
        返回值：无。
        异常：无。
        副作用：仅保存配置，不执行外部调用。
        """
        self._token = token
        self._principal = AdminPrincipal(
            subject,
            roles,
            "development",
            claims={"roles": ",".join(sorted(roles))},
            groups=roles,
            display_name=subject,
        )

    def authenticate(self, credentials: AdminCredentials) -> AuthenticatedPrincipal | None:
        """使用常量时间比较验证显式开发 Token。

        参数：credentials 为当前请求携带的认证凭据。
        返回值：Token 匹配时返回管理员主体，否则返回 None。
        异常：无。
        副作用：无，不记录 Token。
        """
        if not self._token or not credentials.token:
            return None
        return self._principal if secrets.compare_digest(self._token, credentials.token) else None

    def authorize(self, principal: AuthenticatedPrincipal, capability: ConsoleCapability) -> bool:
        """根据固定能力表判断管理员角色，不推断业务写权限。

        参数：principal 为已认证主体；capability 为待校验能力。
        返回值：主体至少拥有一个允许角色时返回 True。
        异常：无。
        副作用：无。
        """
        return self.decide(principal, capability).allowed

    def decide(
        self, principal: AuthenticatedPrincipal, capability: ConsoleCapability
    ) -> AuthorizationDecision:
        """返回固定角色矩阵的授权结果和稳定审计依据。"""
        allowed_roles = self._CAPABILITY_ROLES.get(capability, frozenset())
        # 使用显式安全优先级，不依赖 set 排序或请求体提供的角色。
        role = next(
            (
                candidate
                for candidate in (
                    "administrator",
                    "operations_admin",
                    "auditor",
                    "sales",
                )
                if candidate in _principal_roles(principal) and candidate in allowed_roles
            ),
            None,
        )
        return AuthorizationDecision(
            allowed=role is not None,
            capability=capability,
            basis=f"verified_group:{role}" if role else "no_allowed_verified_group",
            role=role,
        )

    def verified_context(
        self, principal: AuthenticatedPrincipal, capability: ConsoleCapability
    ) -> VerifiedAuthorizationContext | None:
        """将开发环境显式角色判定封装为路由适配器所需上下文。"""
        return VerifiedAuthorizationContext._from_decision(
            principal, self.decide(principal, capability)
        )


class DenyAllAdminIdentityProvider:
    """生产身份系统尚未接入时使用的安全拒绝实现。"""

    def authenticate(self, credentials: AdminCredentials) -> AuthenticatedPrincipal | None:
        """拒绝所有凭据，避免生产环境误启用匿名或开发认证。"""
        return None

    def authorize(self, principal: AuthenticatedPrincipal, capability: ConsoleCapability) -> bool:
        """拒绝所有能力授权。"""
        return False

    def decide(
        self, principal: AuthenticatedPrincipal, capability: ConsoleCapability
    ) -> AuthorizationDecision:
        """返回生产未接入真实身份 Provider 时的拒绝判定。"""
        return AuthorizationDecision(False, capability, "provider_not_configured")

    def verified_context(
        self, principal: AuthenticatedPrincipal, capability: ConsoleCapability
    ) -> VerifiedAuthorizationContext | None:
        """生产真实身份 Provider 尚未接入时拒绝生成授权上下文。"""
        return None


class LocalCapabilityAuthorizer:
    """只依据本地 SalesAuthorization 授予 Console capability。"""

    _CAPABILITIES = frozenset(ConsoleCapability)

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """保存本地授权目录会话工厂。

        参数：session_factory 为读取本地 SalesAuthorization 的会话工厂。
        返回值：无。
        异常：无。
        副作用：仅保存依赖，不访问数据库。
        """
        self._session_factory = session_factory

    def authorize(self, principal: AuthenticatedPrincipal, capability: ConsoleCapability) -> bool:
        """依据本地 active administrator 记录判断 capability。

        参数：principal 为认证 Provider 返回的主体；capability 为待授权能力。
        返回值：本地存在 active administrator 且能力受支持时返回 True。
        异常：数据库故障按拒绝处理，不向调用方泄露数据库错误。
        副作用：只读 SalesAuthorization。
        """
        return self.decide(principal, capability).allowed

    def decide(
        self, principal: AuthenticatedPrincipal, capability: ConsoleCapability
    ) -> AuthorizationDecision:
        """生成只包含本地授权依据的 capability 判定。

        参数：principal 为认证主体；capability 为待授权能力。
        返回值：含稳定 capability、basis 和本地角色的判定。
        异常：数据库故障转换为拒绝判定。
        副作用：读取本地授权目录，不信任 principal 的 claims/groups 作为授权事实。
        """
        if capability not in self._CAPABILITIES:
            return AuthorizationDecision(False, capability, "capability_not_supported")
        try:
            with self._session_factory() as session:
                authorization = session.get(SalesAuthorization, principal.subject)
        except Exception as error:
            logger.error(
                "local_capability_authorization_failed",
                extra={"error_type": type(error).__name__},
            )
            return AuthorizationDecision(False, capability, "local_authorization_unavailable")
        if authorization is None:
            return AuthorizationDecision(False, capability, "local_authorization_missing")
        if not authorization.is_authorized:
            return AuthorizationDecision(False, capability, "local_authorization_not_authorized")
        if not authorization.is_active:
            return AuthorizationDecision(False, capability, "local_authorization_inactive")
        if not authorization.is_administrator:
            return AuthorizationDecision(False, capability, "local_authorization_not_administrator")
        return AuthorizationDecision(True, capability, "local_sales_authorization", "administrator")

    def verified_context(
        self, principal: AuthenticatedPrincipal, capability: ConsoleCapability
    ) -> VerifiedAuthorizationContext | None:
        """完成本地 SalesAuthorization 校验后生成 Break-glass 授权事实。"""
        return VerifiedAuthorizationContext._from_decision(
            principal, self.decide(principal, capability)
        )
