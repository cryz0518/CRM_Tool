"""智能表格记录级权限验证的领域边界。

真实企业微信 CLI/API 是否支持以两个销售身份分别验证单条记录，属于外部契约；
本模块只定义可替换 seam，不猜测底层命令或伪造生产实现。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class SmartTablePermissionVerification:
    """承载负责人转交后两个销售主体的最小权限结论。"""

    new_owner_can_view: bool
    new_owner_can_edit: bool
    old_owner_can_view: bool
    old_owner_can_edit: bool

    @property
    def verified(self) -> bool:
        """判断新负责人可见可编辑且旧负责人不可见。"""

        return (
            self.new_owner_can_view
            and self.new_owner_can_edit
            and not self.old_owner_can_view
            and not self.old_owner_can_edit
        )


class SmartTablePermissionVerificationProvider(Protocol):
    """以明确销售身份验证单条记录读写权限的端口。"""

    def verify_owner_transfer(
        self, record_id: str, old_owner_user_id: str, new_owner_user_id: str
    ) -> SmartTablePermissionVerification:
        """验证负责人转交后的可见性与可编辑性。"""


class SmartTablePermissionVerificationUnavailable(RuntimeError):
    """表示部署尚未提供可证明记录级权限的真实实现。"""


class UnconfiguredSmartTablePermissionVerifier:
    """默认阻断器，避免把普通机器人读取误当成销售权限验证。"""

    def verify_owner_transfer(
        self, record_id: str, old_owner_user_id: str, new_owner_user_id: str
    ) -> SmartTablePermissionVerification:
        """明确报告外部权限验证契约缺失。"""

        del record_id, old_owner_user_id, new_owner_user_id
        raise SmartTablePermissionVerificationUnavailable(
            "真实智能表格尚未提供按销售身份验证单记录权限的契约"
        )
