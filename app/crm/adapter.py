"""CRM 统一线索提交的适配器契约。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping, Protocol


@dataclass(frozen=True)
class CRMCreateResult:
    """描述 CRM 统一提交成功后的返回事实。"""

    crm_lead_id: str | None
    crm_lead_owner_user_id: str | None
    response_summary: str
    action: Literal["CREATE", "FOLLOW_UP"] = "CREATE"


@dataclass(frozen=True)
class CRMSearchResult:
    """描述 CRM 按线索名称查重返回的一条既有线索身份。"""

    crm_lead_id: str
    crm_lead_owner_user_id: str | None
    response_summary: str


class CRMAdapter(Protocol):
    """隔离 CRM 线索提交接口。"""

    def submit_lead(
        self, payload: Mapping[str, object], *, idempotency_key: str, crm_user_id: str
    ) -> CRMCreateResult:
        """向 CRM 统一线索接口提交字段，由 CRM 决定创建或生成跟进。

        参数：payload 为已校验的 CRM 字段；idempotency_key 用于远端幂等；
        crm_user_id 为映射后的提交人。
        返回值：CRM 返回的新线索身份；仅生成跟进时身份字段可以为空。
        异常：接口或响应无法确认成功时抛出适配器异常。
        副作用：向 CRM 发起一次统一提交请求。
        """
        ...
