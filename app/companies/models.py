"""公司解析领域的值对象和稳定适配器输入输出。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class CompanyRegion(StrEnum):
    """表示公司可审计的地域判定。"""

    DOMESTIC = "domestic"
    FOREIGN = "foreign"
    UNKNOWN = "unknown"


class CompanyVerificationStatus(StrEnum):
    """表示公司名称的核验与人工确认事实。"""

    INCOMPLETE_COMPANY = "incomplete_company"
    TYC_VERIFIED = "tyc_verified"
    # 兼容历史代码读取旧枚举名称；新业务代码统一使用天眼查命名。
    QCC_VERIFIED = TYC_VERIFIED
    FOREIGN_DECLARED = "foreign_declared"
    COMPANY_UNVERIFIED = "company_unverified"
    USER_CONFIRMED_UNVERIFIED = "user_confirmed_unverified"
    VERIFICATION_CONFLICT = "verification_conflict"


@dataclass(frozen=True)
class CompanyRegionEvidence:
    """封装地域判定可用的原始证据，避免将名称风格误作地域事实。"""

    message_text: str | None = None
    company_name: str | None = None
    email: str | None = None
    phone: str | None = None
    explicit_foreign: bool = False
    sales_confirmed_region: CompanyRegion | None = None


@dataclass(frozen=True)
class TYCCandidate:
    """保存天眼查返回的可审计公司候选。"""

    standard_company_name: str
    company_id: str


@dataclass(frozen=True)
class TYCLookupResult:
    """描述天眼查查询的确定性业务结论。"""

    status: str
    candidates: tuple[TYCCandidate, ...] = ()

    @classmethod
    def matched(cls, candidate: TYCCandidate) -> TYCLookupResult:
        """构造唯一可靠的天眼查匹配结果。

        参数：candidate 为唯一命中的标准公司。
        返回值：带 matched 状态的查询结果。
        异常：无。
        副作用：无。
        """
        return cls(status="matched", candidates=(candidate,))

    @classmethod
    def not_found(cls) -> TYCLookupResult:
        """构造天眼查无结果的查询结论。

        返回值：不含候选的 not_found 结果。
        异常：无。
        副作用：无。
        """
        return cls(status="not_found")

    @classmethod
    def ambiguous(cls, candidates: tuple[TYCCandidate, ...]) -> TYCLookupResult:
        """构造多个候选而不能自动认定的查询结论。

        参数：candidates 为待销售审核的多个候选。
        返回值：带 ambiguous 状态的查询结果。
        异常：无。
        副作用：无。
        """
        return cls(status="ambiguous", candidates=candidates)


@dataclass(frozen=True)
class CompanyResolution:
    """封装一次公司解析所得的名称、核验和可恢复失败审计事实。"""

    standard_company_name: str | None
    tyc_customer_id: str | None
    verification_status: CompanyVerificationStatus
    candidates: tuple[TYCCandidate, ...] = ()
    tyc_failure_event_type: str | None = None

    @property
    def qcc_company_id(self) -> str | None:
        """兼容旧调用方读取历史企查查标识；新代码应读取 tyc_customer_id。"""
        return self.tyc_customer_id

    @property
    def qcc_failure_event_type(self) -> str | None:
        """兼容旧调用方读取历史失败事件字段；新代码应读取 tyc_failure_event_type。"""
        return self.tyc_failure_event_type


@dataclass(frozen=True)
class CompanyUpsertCommand:
    """描述一条消息或人工确认驱动的公司线索增量。"""

    source_message_id: str
    sales_user_id: str
    fields: dict[str, object]
    existing_lead_id: str | None = None
    region_evidence: CompanyRegionEvidence | None = None
    user_confirmed_company: bool = False
    defer_smart_table_sync: bool = False
    source_segment_index: int = 0


@dataclass(frozen=True)
class CompanyUpsertResult:
    """返回公司处理后可观察的线索事实。"""

    lead_id: str
    smart_table_record_id: str | None
    lifecycle_state: str
    standard_company_name: str | None
    verification_status: CompanyVerificationStatus
    smart_table_patch: dict[str, object] = field(default_factory=dict)


# 兼容现有测试和外部适配器导入；生产入口与文案统一使用 TYC。
QCCCandidate = TYCCandidate
QCCLookupResult = TYCLookupResult
