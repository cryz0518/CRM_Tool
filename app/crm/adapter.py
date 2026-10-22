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

    def search_by_company_name(
        self, payload: Mapping[str, object] | str
    ) -> tuple[CRMSearchResult, ...]:
        """按线索名称查询 CRM 当前重复记录。

        参数：payload 为规范 CRM 线索字段或公司名称。
        返回值：当前 CRM 中匹配的线索身份；没有匹配项时返回空元组。
        异常：查询失败时抛出适配器异常，由提交服务记录安全失败证据。
        副作用：向 CRM 发起一次只读查重请求。
        """
        ...

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

    def update_lead(
        self,
        crm_lead_id: str,
        payload: Mapping[str, object],
        *,
        idempotency_key: str,
        crm_user_id: str,
    ) -> CRMCreateResult:
        """更新查重确认后的 CRM 既有线索并保留 CRM 原负责人。

        参数：crm_lead_id 为查重得到的 CRM 线索标识；payload 为已校验的最终字段；
        idempotency_key 为冻结更新键；crm_user_id 为已映射的提交人。
        返回值：更新后的 CRM 线索身份。
        异常：更新被拒绝或远端结果无法确认时抛出适配器异常。
        副作用：向 CRM 发起一次单条更新请求。
        """
        ...
