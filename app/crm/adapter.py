"""CRM 首次创建的最小稳定适配器契约。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol


@dataclass(frozen=True)
class CRMCreateResult:
    """描述 CRM 成功创建后返回的最小身份事实。"""

    crm_lead_id: str
    crm_lead_owner_user_id: str | None
    response_summary: str


class CRMAdapter(Protocol):
    """隔离 T12 所需的单一 CRM 创建动作。"""

    def create_lead(
        self, payload: Mapping[str, object], *, idempotency_key: str, crm_user_id: str
    ) -> CRMCreateResult:
        """以冻结的提交人 CRM 身份创建线索，并由实现再次校验最低条件。"""
        ...

    def update_lead(
        self,
        crm_lead_id: str,
        payload: Mapping[str, object],
        *,
        idempotency_key: str,
        crm_user_id: str,
    ) -> CRMCreateResult:
        """以冻结提交身份更新既有 CRM 线索，且不得改变其负责人。"""
        ...
