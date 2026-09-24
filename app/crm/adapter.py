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


@dataclass(frozen=True)
class CRMSearchResult:
    """描述 CRM 按线索名称查重返回的一条既有线索身份。"""

    crm_lead_id: str
    crm_lead_owner_user_id: str | None
    response_summary: str


class CRMAdapter(Protocol):
    """隔离 CRM 查重、创建和更新动作。"""

    def search_by_company_name(self, company_name: str) -> tuple[CRMSearchResult, ...]:
        """按线索名称查询 CRM 中已有的线索身份。"""
        ...

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
