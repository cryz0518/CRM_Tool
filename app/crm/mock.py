"""用于 T12 闭环测试的内存 CRM 创建适配器。"""

from __future__ import annotations

from typing import Mapping

from app.crm.adapter import CRMCreateResult


class MockCRMAdapter:
    """记录调用并以稳定返回值模拟 CRM，不猜测真实 CRM 契约。"""

    def __init__(self) -> None:
        """初始化调用计数、载荷历史和按幂等键缓存的创建结果。"""
        self.calls = 0
        self.payloads: list[dict[str, object]] = []
        self._results: dict[str, CRMCreateResult] = {}

    def create_lead(
        self, payload: Mapping[str, object], *, idempotency_key: str, crm_user_id: str
    ) -> CRMCreateResult:
        """防御性校验最低创建条件后记录首次调用并返回模拟 CRM 身份。"""
        if not payload.get("线索名称") or not payload.get("业务线") or not any(
            payload.get(name) for name in ("手机", "电话", "邮箱")
        ):
            raise ValueError("CRM minimum create 条件不满足")
        if idempotency_key in self._results:
            return self._results[idempotency_key]
        self.calls += 1
        self.payloads.append(dict(payload))
        result = CRMCreateResult(
            crm_lead_id=f"mock-crm-{self.calls}",
            crm_lead_owner_user_id=crm_user_id,
            response_summary="mock CRM 创建成功",
        )
        self._results[idempotency_key] = result
        return result
