"""T15 Console 默认脱敏与管理员认证 Seam 测试。"""

from __future__ import annotations

from app.console.auth import (
    AdminCredentials,
    AdminPrincipal,
    ConsoleCapability,
    DevelopmentAdminIdentityProvider,
)
from app.console.dto import ConsoleMessageDTO
from app.console.masking import MaskingPolicy


def test_masking_hides_contact_values_and_keeps_safe_text() -> None:
    """验证默认 DTO 只能携带脱敏后的手机号、邮箱和消息摘要。"""
    policy = MaskingPolicy()

    assert policy.mask_field("手机", "13812345678") == "138****5678"
    assert policy.mask_field("邮箱", "alice@example.com") == "a***@example.com"
    assert policy.mask_text("客户电话 13812345678，邮箱 alice@example.com") == (
        "客户电话 138****5678，邮箱 a***@example.com"
    )
    assert policy.mask_text("联系人王验收") == "联系人王*收"


def test_message_dto_has_no_raw_payload_or_raw_message_body() -> None:
    """验证消息 DTO 的公开字段中不存在原始 payload 或未脱敏正文。"""
    message = ConsoleMessageDTO(
        message_id="message-1",
        sales_user_id="sales-1",
        sequence=1,
        received_at="2026-09-16T00:00:00Z",
        text_summary="客户电话 138****5678",
        has_raw_payload=True,
    )

    public_data = message.model_dump()
    assert "raw_payload" not in public_data
    assert "normalized_text" not in public_data
    assert public_data["text_summary"] == "客户电话 138****5678"


def test_development_admin_provider_requires_explicit_token_and_admin_role() -> None:
    """验证开发认证也不能匿名通过，并且非管理员不能访问 Console。"""
    provider = DevelopmentAdminIdentityProvider(
        token="console-test-token",
        subject="admin-1",
        roles=frozenset({"administrator"}),
    )

    assert provider.authenticate(AdminCredentials(token=None)) is None
    principal = provider.authenticate(AdminCredentials(token="console-test-token"))
    assert principal == AdminPrincipal(
        subject="admin-1",
        roles=frozenset({"administrator"}),
        auth_source="development",
    )
    assert provider.authorize(principal, ConsoleCapability.CONSOLE_READ)

    non_admin = DevelopmentAdminIdentityProvider(
        token="console-test-token",
        subject="operator-1",
        roles=frozenset({"sales"}),
    ).authenticate(AdminCredentials(token="console-test-token"))
    assert non_admin is not None
    assert not provider.authorize(non_admin, ConsoleCapability.CONSOLE_READ)
