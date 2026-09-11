"""公司解析领域的值对象和稳定适配器输入输出。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class CompanyRegion(StrEnum):
    """表示公司可审计的地域判定。"""

    DOMESTIC = "domestic"
    FOREIGN = "foreign"
    UNKNOWN = "unknown"


class CompanyVerificationStatus(StrEnum):
    """表示公司名称的核验与人工确认事实。"""

    INCOMPLETE_COMPANY = "incomplete_company"
    QCC_VERIFIED = "qcc_verified"
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
class QCCCandidate:
    """保存企查查返回的可审计公司候选。"""

    standard_company_name: str
    company_id: str


@dataclass(frozen=True)
class QCCLookupResult:
    """描述企查查查询的确定性业务结论。"""

    status: str
    candidates: tuple[QCCCandidate, ...] = ()

    @classmethod
    def matched(cls, candidate: QCCCandidate) -> QCCLookupResult:
        """构造唯一可靠的企查查匹配结果。

        参数：candidate 为唯一命中的标准公司。
        返回值：带 matched 状态的查询结果。
        异常：无。
        副作用：无。
        """
        return cls(status="matched", candidates=(candidate,))

    @classmethod
    def not_found(cls) -> QCCLookupResult:
        """构造企查查无结果的查询结论。

        返回值：不含候选的 not_found 结果。
        异常：无。
        副作用：无。
        """
        return cls(status="not_found")

    @classmethod
    def ambiguous(cls, candidates: tuple[QCCCandidate, ...]) -> QCCLookupResult:
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
    qcc_company_id: str | None
    verification_status: CompanyVerificationStatus
    candidates: tuple[QCCCandidate, ...] = ()
    qcc_failure_event_type: str | None = None


@dataclass(frozen=True)
class CompanyUpsertCommand:
    """描述一条消息或人工确认驱动的公司线索增量。"""

    source_message_id: str
    sales_user_id: str
    fields: dict[str, str]
    existing_lead_id: str | None = None
    region_evidence: CompanyRegionEvidence | None = None
    user_confirmed_company: bool = False


@dataclass(frozen=True)
class CompanyUpsertResult:
    """返回公司处理后可观察的线索事实。"""

    lead_id: str
    smart_table_record_id: str | None
    lifecycle_state: str
    standard_company_name: str | None
    verification_status: CompanyVerificationStatus
