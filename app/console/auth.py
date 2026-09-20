"""Operations Console 管理员身份认证 Seam。"""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


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
class AdminCredentials:
    """承载认证 Provider 所需的请求凭据，不保存原始请求对象。"""

    token: str | None


@dataclass(frozen=True)
class AdminPrincipal:
    """表示已通过认证的管理员主体及其认证来源。"""

    subject: str
    roles: frozenset[str]
    auth_source: str


class AdminIdentityProvider(Protocol):
    """定义可替换的管理员认证与能力授权接口。"""

    def authenticate(self, credentials: AdminCredentials) -> AdminPrincipal | None:
        """验证请求凭据，失败时返回 None。"""

    def authorize(self, principal: AdminPrincipal, capability: ConsoleCapability) -> bool:
        """判断主体是否具备指定 Console 能力。"""


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
        self._principal = AdminPrincipal(subject, roles, "development")

    def authenticate(self, credentials: AdminCredentials) -> AdminPrincipal | None:
        """使用常量时间比较验证显式开发 Token。

        参数：credentials 为当前请求携带的认证凭据。
        返回值：Token 匹配时返回管理员主体，否则返回 None。
        异常：无。
        副作用：无，不记录 Token。
        """
        if not self._token or not credentials.token:
            return None
        return self._principal if secrets.compare_digest(self._token, credentials.token) else None

    def authorize(self, principal: AdminPrincipal, capability: ConsoleCapability) -> bool:
        """根据固定能力表判断管理员角色，不推断业务写权限。

        参数：principal 为已认证主体；capability 为待校验能力。
        返回值：主体至少拥有一个允许角色时返回 True。
        异常：无。
        副作用：无。
        """
        allowed_roles = self._CAPABILITY_ROLES.get(capability, frozenset())
        return bool(principal.roles & allowed_roles)


class DenyAllAdminIdentityProvider:
    """生产身份系统尚未接入时使用的安全拒绝实现。"""

    def authenticate(self, credentials: AdminCredentials) -> AdminPrincipal | None:
        """拒绝所有凭据，避免生产环境误启用匿名或开发认证。"""
        return None

    def authorize(self, principal: AdminPrincipal, capability: ConsoleCapability) -> bool:
        """拒绝所有能力授权。"""
        return False
