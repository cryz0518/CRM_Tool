"""用于 T12 闭环测试的内存 CRM 创建适配器。"""

from __future__ import annotations

import hashlib
from typing import Mapping

from app.crm.adapter import CRMCreateResult, CRMSearchResult


class MockCRMAdapter:
    """记录调用并以稳定返回值模拟 CRM，不猜测真实 CRM 契约。"""

    def __init__(
        self, search_results: Mapping[str, tuple[CRMSearchResult, ...]] | None = None
    ) -> None:
        """初始化调用计数、载荷历史和可预置的 CRM 查重结果。"""
        self.calls = 0
        self.payloads: list[dict[str, object]] = []
        self.idempotency_keys: list[str] = []
        self.crm_user_ids: list[str] = []
        self.update_calls = 0
        self.update_payloads: list[dict[str, object]] = []
        self.update_idempotency_keys: list[str] = []
        self.update_crm_user_ids: list[str] = []
        self.delete_calls = 0
        self.search_calls = 0
        self.search_company_names: list[str] = []
        self._search_results = dict(search_results or {})

    def search_by_company_name(self, company_name: str) -> tuple[CRMSearchResult, ...]:
        """返回预置或本次 Mock 创建出的 CRM 线索，模拟查重接口。"""
        self.search_calls += 1
        self.search_company_names.append(company_name)
        return self._search_results.get(company_name, ())

    def create_lead(
        self, payload: Mapping[str, object], *, idempotency_key: str, crm_user_id: str
    ) -> CRMCreateResult:
        """防御性校验最低创建条件后记录首次调用并返回模拟 CRM 身份。"""
        if (
            not payload.get("name")
            or not payload.get("product_line_data_permission")
            or not payload.get("source")
            or not payload.get("contactName")
            or not payload.get("contactTitle")
            or not payload.get("communicationWay")
            or not payload.get("mobile")
            or not payload.get("remark")
        ):
            raise ValueError("CRM minimum create 条件不满足")
        self.calls += 1
        self.payloads.append(dict(payload))
        self.idempotency_keys.append(idempotency_key)
        self.crm_user_ids.append(crm_user_id)
        result = CRMCreateResult(
            # 模拟独立远端服务：identity 仅由稳定幂等键决定，不依赖本地实例内存。
            crm_lead_id=f"mock-crm-{hashlib.sha256(idempotency_key.encode()).hexdigest()[:24]}",
            crm_lead_owner_user_id=crm_user_id,
            response_summary="mock CRM 创建成功",
        )
        company_name = payload.get("name")
        if isinstance(company_name, str):
            self._search_results[company_name] = (
                CRMSearchResult(result.crm_lead_id, crm_user_id, "mock CRM 查重命中"),
            )
        return result

    def update_lead(
        self,
        crm_lead_id: str,
        payload: Mapping[str, object],
        *,
        idempotency_key: str,
        crm_user_id: str,
    ) -> CRMCreateResult:
        """记录对既有身份的模拟更新，并保留由已有 CRM 决定的负责人。"""
        self.update_calls += 1
        self.update_payloads.append(dict(payload))
        self.update_idempotency_keys.append(idempotency_key)
        self.update_crm_user_ids.append(crm_user_id)
        return CRMCreateResult(crm_lead_id, None, "mock CRM 更新成功")

    def delete_lead(self, _crm_lead_id: str) -> None:
        """记录被禁止的 CRM 删除调用，供 T14 断言外部事实优先而不补偿删除。"""
        self.delete_calls += 1
        raise AssertionError("T14 禁止调用 CRM delete")
