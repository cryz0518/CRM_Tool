"""将既有领域事实投影为 Operations Console 冲突 DTO。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from app.console.dto import ConsoleConflictDTO
from app.leads.models import Lead, LeadFieldProvenance


class ConflictProjection:
    """只读组合 T14 及既有线索状态，不创建新的业务冲突真相。"""

    _VERIFICATION_CONFLICTS = frozenset(
        {"company_unverified", "verification_conflict", "ambiguous"}
    )
    _TASK_CONFLICT_STATUSES = frozenset({"failed_pending_review", "unknown"})

    @classmethod
    def for_lead(
        cls, lead: Lead, provenances: Sequence[LeadFieldProvenance]
    ) -> list[ConsoleConflictDTO]:
        """投影单条线索的公司身份、核验和人工修改冲突。

        参数：lead 为线索事实；provenances 为该线索字段来源事实。
        返回值：不含敏感字段值的冲突 DTO 列表。
        异常：无。
        副作用：无，不修改任何领域状态。
        """
        conflicts: list[ConsoleConflictDTO] = []
        if lead.lifecycle_state == "company_identity_change_pending_review":
            conflicts.append(
                cls._lead_conflict(
                    lead,
                    "company_identity",
                    "blocking",
                    "公司标准名称发生变化，等待人工判定",
                )
            )
        if lead.company_verification_status in cls._VERIFICATION_CONFLICTS:
            conflicts.append(
                cls._lead_conflict(
                    lead,
                    "company_verification",
                    "blocking",
                    "公司核验尚未形成唯一可信结果",
                )
            )
        changed_fields = tuple(
            sorted({item.field_name for item in provenances if item.is_user_modified})
        )
        if changed_fields:
            conflicts.append(
                cls._lead_conflict(
                    lead,
                    "user_modified_field",
                    "review_required",
                    "销售已修改字段，AI 不得自动覆盖",
                    affected_fields=changed_fields,
                )
            )
        return conflicts

    @classmethod
    def for_task(
        cls,
        *,
        task_kind: str,
        subject_id: str,
        status: str,
        failure_category: str | None,
        detail: str | None,
        created_at: datetime,
        lead_id: str | None = None,
        message_id: str | None = None,
    ) -> ConsoleConflictDTO | None:
        """投影失败、未知结果或 T14 重试任务冲突。

        参数：任务参数均为已白名单化的状态和引用；detail 只允许错误摘要。
        返回值：需要人工处理时返回冲突 DTO，否则返回 None。
        异常：无。
        副作用：无。
        """
        if status not in cls._TASK_CONFLICT_STATUSES:
            return None
        conflict_status = "blocking" if status == "unknown" else "open"
        return ConsoleConflictDTO(
            conflict_id=f"task:{task_kind}:{subject_id}",
            source=task_kind,
            conflict_kind="task_failure",
            severity=conflict_status,
            status="failed_pending_review" if status != "unknown" else "unknown",
            lead_id=lead_id,
            message_id=message_id,
            source_status=status,
            failure_category=failure_category,
            detail=detail,
            created_at=created_at,
            updated_at=created_at,
        )

    @staticmethod
    def for_audit(
        *,
        audit_id: str,
        event_type: str,
        message_id: str | None,
        created_at: datetime,
    ) -> ConsoleConflictDTO | None:
        """将已记录的外部事实冲突转换为只读投影。

        参数：audit_id、event_type、message_id 和 created_at 为业务审计字段。
        返回值：事件属于 T14 废弃竞态时返回冲突 DTO，否则返回 None。
        异常：无。
        副作用：无。
        """
        if event_type != "discard_request_not_effective":
            return None
        return ConsoleConflictDTO(
            conflict_id=f"audit:{audit_id}",
            source="business_audit",
            conflict_kind="discard_request_not_effective",
            severity="review_required",
            status="not_effective",
            message_id=message_id,
            detail="CRM 已成功创建，废弃请求未生效",
            created_at=created_at,
            updated_at=created_at,
        )

    @staticmethod
    def _lead_conflict(
        lead: Lead,
        kind: str,
        severity: str,
        detail: str,
        *,
        affected_fields: tuple[str, ...] = (),
    ) -> ConsoleConflictDTO:
        """构造单条线索冲突 DTO，避免返回公司或联系人原值。"""
        return ConsoleConflictDTO(
            conflict_id=f"lead:{lead.id}:{kind}",
            source="lead",
            conflict_kind=kind,
            severity=severity,
            status="open",
            lead_id=lead.id,
            source_status=lead.lifecycle_state,
            affected_fields=affected_fields,
            detail=detail,
            created_at=lead.created_at,
            updated_at=lead.updated_at,
        )
