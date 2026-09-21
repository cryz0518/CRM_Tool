"""T22 retention policy、清理操作签发和租约 fencing。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import Settings
from app.media.storage import StorageDeleteOutcome, StorageProvider
from app.messaging.models import (
    IncomingMessage,
    MessageAttachment,
    NotificationRecord,
    StorageCleanupOperation,
    StorageIngestOperation,
    utc_now,
)


@dataclass(frozen=True)
class RetentionPolicy:
    """冻结一次清理操作所需的 data class 保留配置，不包含法律默认年限。"""

    version: str
    media_retention_days: int
    message_payload_retention_days: int
    notification_payload_retention_days: int

    def __post_init__(self) -> None:
        """拒绝空版本、非正数和超出技术边界的保留配置。"""
        if not self.version.strip():
            raise ValueError("retention policy version 不能为空")
        for name in (
            "media_retention_days",
            "message_payload_retention_days",
            "notification_payload_retention_days",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"{name} 必须大于 0")
            if value > 36500:
                raise ValueError(f"{name} 不能超过 36500 天")

    @classmethod
    def from_settings(cls, settings: Settings) -> "RetentionPolicy":
        """从显式配置构造策略，缺失值直接 fail closed。"""
        values = (
            settings.media_retention_days,
            settings.message_payload_retention_days,
            settings.notification_payload_retention_days,
        )
        if not settings.media_retention_policy_version or any(value is None for value in values):
            raise RuntimeError("retention_policy_not_configured")
        media_days, message_days, notification_days = values
        assert media_days is not None
        assert message_days is not None
        assert notification_days is not None
        return cls(
            version=settings.media_retention_policy_version,
            media_retention_days=media_days,
            message_payload_retention_days=message_days,
            notification_payload_retention_days=notification_days,
        )


@dataclass(frozen=True)
class CleanupIssue:
    """描述已持久化且可重复获取的清理 operation。"""

    operation_id: str
    operation_key: str
    operation: StorageCleanupOperation


@dataclass(frozen=True)
class CleanupClaim:
    """描述一次带 claim token 和 generation 的 Worker 租约。"""

    operation_id: str
    claim_token: str
    generation: int


class RetentionCleanupService:
    """签发稳定清理 operation，并提供带 fencing 的 claim/finalize 原语。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """保存短事务数据库会话工厂，不在构造时执行扫描或远程调用。"""
        self._session_factory = session_factory

    def issue_attachment_cleanup(
        self,
        attachment_id: str,
        *,
        policy: RetentionPolicy,
        now: datetime | None = None,
    ) -> CleanupIssue:
        """以数据库唯一键幂等签发单个附件清理 operation。"""
        current_time = _as_utc(now or utc_now())
        operation_key = f"media:{attachment_id}:{policy.version}"
        with self._session_factory.begin() as session:
            attachment = session.get(MessageAttachment, attachment_id)
            if attachment is None:
                raise ValueError("attachment_not_found")
            operation = session.scalar(
                select(StorageCleanupOperation).where(
                    StorageCleanupOperation.operation_key == operation_key
                )
            )
            if operation is None:
                try:
                    with session.begin_nested():
                        operation = StorageCleanupOperation(
                            operation_key=operation_key,
                            data_class="raw_media_object",
                            target_type="message_attachment",
                            target_id=attachment.id,
                            storage_provider=attachment.storage_provider,
                            storage_key=attachment.storage_key,
                            storage_key_digest=_digest(attachment.storage_key),
                            policy_version=policy.version,
                            retention_cutoff=_as_utc(
                                attachment.retention_expires_at or current_time
                            ),
                        )
                        session.add(operation)
                        session.flush()
                except IntegrityError:
                    # 并发 scheduler 的唯一键冲突就是另一方已经签发成功。
                    operation = session.scalar(
                        select(StorageCleanupOperation).where(
                            StorageCleanupOperation.operation_key == operation_key
                        )
                    )
            if operation is None:
                raise RuntimeError("cleanup_operation_issuance_failed")
            attachment.cleanup_operation_id = operation.id
            return CleanupIssue(operation.id, operation_key, operation)

    def claim(
        self,
        operation_id: str,
        *,
        now: datetime | None = None,
        lease_seconds: int,
    ) -> CleanupClaim | None:
        """原子认领待处理或过期 operation，并刷新 token/generation。"""
        current_time = _as_utc(now or utc_now())
        if lease_seconds <= 0:
            raise ValueError("lease_seconds 必须大于 0")
        with self._session_factory.begin() as session:
            operation = session.scalar(
                select(StorageCleanupOperation)
                .where(StorageCleanupOperation.id == operation_id)
                .with_for_update()
            )
            if operation is None or operation.status in {
                "succeeded",
                "failed_pending_review",
            }:
                return None
            expired = operation.status == "processing" and (
                operation.lease_expires_at is None
                or _as_utc(operation.lease_expires_at) <= current_time
            )
            if operation.status == "processing" and not expired:
                return None
            if (
                operation.status not in {"pending", "retrying", "reconcile_required"}
                and not expired
            ):
                return None
            token = uuid4().hex
            operation.status = "processing"
            operation.claim_token = token
            operation.generation += 1
            operation.attempt_count += 1
            operation.processing_started_at = current_time
            operation.lease_expires_at = current_time + timedelta(seconds=lease_seconds)
            return CleanupClaim(operation.id, token, operation.generation)

    def finalize_success(
        self,
        operation_id: str,
        claim: CleanupClaim,
        *,
        now: datetime | None = None,
    ) -> bool:
        """以 claim token/generation 栅栏完成 operation 并写入附件 tombstone。"""
        completed_at = _as_utc(now or utc_now())
        with self._session_factory.begin() as session:
            result = cast(CursorResult[object], session.execute(
                update(StorageCleanupOperation)
                .where(
                    StorageCleanupOperation.id == operation_id,
                    StorageCleanupOperation.status == "processing",
                    StorageCleanupOperation.claim_token == claim.claim_token,
                    StorageCleanupOperation.generation == claim.generation,
                )
                .values(
                    status="succeeded",
                    completed_at=completed_at,
                    updated_at=completed_at,
                    lease_expires_at=None,
                    storage_key=None,
                )
            ))
            if result.rowcount != 1:
                return False
            operation = session.get(StorageCleanupOperation, operation_id)
            if operation is None:
                return False
            attachment = session.get(MessageAttachment, operation.target_id)
            if attachment is not None:
                attachment.storage_key = None
                attachment.storage_etag = None
                attachment.deletion_status = "deleted"
                attachment.deleted_at = completed_at
                attachment.deletion_reason = "retention_cleanup"
                attachment.cleanup_operation_id = operation.id
            return True

    def execute(
        self,
        operation_id: str,
        storage: StorageProvider,
        *,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> str:
        """先 HEAD 再删除并以远端事实收敛单个清理 operation。"""
        claim = self.claim(operation_id, now=now, lease_seconds=lease_seconds)
        if claim is None:
            return "not_claimed"
        with self._session_factory() as session:
            operation = session.get(StorageCleanupOperation, operation_id)
            attachment = (
                session.get(MessageAttachment, operation.target_id)
                if operation is not None
                else None
            )
            if operation is None:
                return "missing_operation"
            storage_key = operation.storage_key or (attachment.storage_key if attachment else None)
            if attachment is not None and attachment.deletion_status == "deleted":
                self._set_remote_outcome(operation_id, claim, StorageDeleteOutcome.NOT_FOUND)
                self.finalize_success(operation_id, claim, now=now)
                return "succeeded"
        if not storage_key:
            self._mark_failure(operation_id, claim, "storage_key_missing", "清理对象键缺失")
            return "failed_pending_review"

        try:
            metadata = storage.head(storage_key)
        except Exception:
            self._mark_failure(operation_id, claim, "head_failed", "对象状态检查失败")
            return "retrying"
        if metadata is None:
            self._set_remote_outcome(operation_id, claim, StorageDeleteOutcome.NOT_FOUND)
            return (
                "succeeded"
                if self.finalize_success(operation_id, claim, now=now)
                else "stale_claim"
            )

        try:
            result = storage.delete(storage_key)
        except Exception:
            self._mark_failure(operation_id, claim, "delete_failed", "对象删除失败")
            return "retrying"
        if result.outcome in {
            StorageDeleteOutcome.CONFIRMED_DELETED,
            StorageDeleteOutcome.NOT_FOUND,
        }:
            self._set_remote_outcome(operation_id, claim, result.outcome)
            return (
                "succeeded"
                if self.finalize_success(operation_id, claim, now=now)
                else "stale_claim"
            )
        self._mark_failure(
            operation_id, claim, "delete_unknown", "对象删除结果未知", reconcile=True
        )
        return "reconcile_required"

    def _set_remote_outcome(
        self,
        operation_id: str,
        claim: CleanupClaim,
        outcome: StorageDeleteOutcome,
    ) -> bool:
        """在持有当前 claim 时记录远端删除事实。"""
        with self._session_factory.begin() as session:
            result = cast(CursorResult[object], session.execute(
                update(StorageCleanupOperation)
                .where(
                    StorageCleanupOperation.id == operation_id,
                    StorageCleanupOperation.status == "processing",
                    StorageCleanupOperation.claim_token == claim.claim_token,
                    StorageCleanupOperation.generation == claim.generation,
                )
                .values(remote_outcome=outcome.value, updated_at=utc_now())
            ))
            return bool(result.rowcount == 1)

    def _mark_failure(
        self,
        operation_id: str,
        claim: CleanupClaim,
        failure_kind: str,
        summary: str,
        *,
        reconcile: bool = False,
    ) -> bool:
        """以当前 claim 把远端不确定或可重试失败持久化。"""
        with self._session_factory.begin() as session:
            result = cast(CursorResult[object], session.execute(
                update(StorageCleanupOperation)
                .where(
                    StorageCleanupOperation.id == operation_id,
                    StorageCleanupOperation.status == "processing",
                    StorageCleanupOperation.claim_token == claim.claim_token,
                    StorageCleanupOperation.generation == claim.generation,
                )
                .values(
                    status="reconcile_required" if reconcile else "retrying",
                    remote_outcome=StorageDeleteOutcome.UNKNOWN.value if reconcile else None,
                    failure_kind=failure_kind,
                    failure_summary=summary,
                    updated_at=utc_now(),
                    lease_expires_at=None,
                )
            ))
            return bool(result.rowcount == 1)


class RetentionCleanupScheduler:
    """只扫描并签发 cleanup operation，不直接执行远端存储 IO。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """保存数据库会话工厂和 operation 服务。"""
        self._session_factory = session_factory
        self._operations = RetentionCleanupService(session_factory)

    def scan_and_issue(
        self,
        policy: RetentionPolicy,
        *,
        batch_size: int,
        now: datetime | None = None,
    ) -> list[str]:
        """按 keyset 风格扫描到期附件并幂等签发 operation 标识。"""
        if batch_size <= 0:
            raise ValueError("batch_size 必须大于 0")
        current_time = _as_utc(now or utc_now())
        with self._session_factory() as session:
            attachment_ids = session.scalars(
                select(MessageAttachment.id)
                .where(
                    MessageAttachment.deletion_status == "active",
                    MessageAttachment.retention_expires_at.is_not(None),
                    MessageAttachment.retention_expires_at <= current_time,
                )
                .order_by(MessageAttachment.id)
                .limit(batch_size)
            ).all()
        return [
            self._operations.issue_attachment_cleanup(
                attachment_id, policy=policy, now=current_time
            ).operation_id
            for attachment_id in attachment_ids
        ]


class StorageIngestRecoveryService:
    """恢复媒体写入后数据库 finalize 失败的远端对象事实。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """保存数据库会话工厂。"""
        self._session_factory = session_factory

    def reconcile(self, operation_id: str, storage: StorageProvider) -> str:
        """先 HEAD；对象存在时才受控删除，unknown 结果保留 recovery 状态。"""
        with self._session_factory() as session:
            operation = session.get(StorageIngestOperation, operation_id)
            if operation is None or operation.status == "succeeded":
                return "succeeded" if operation is not None else "missing_operation"
            storage_key = operation.storage_key
        if not storage_key:
            return "failed_pending_review"
        try:
            metadata = storage.head(storage_key)
        except Exception:
            self._set_status(operation_id, "reconcile_required", "head_failed")
            return "reconcile_required"
        if metadata is None:
            self._set_status(operation_id, "succeeded", "not_found")
            return "succeeded"
        try:
            result = storage.delete(storage_key)
        except Exception:
            self._set_status(operation_id, "reconcile_required", "delete_failed")
            return "reconcile_required"
        if result.outcome in {
            StorageDeleteOutcome.CONFIRMED_DELETED,
            StorageDeleteOutcome.NOT_FOUND,
        }:
            self._set_status(operation_id, "succeeded", result.outcome.value)
            return "succeeded"
        self._set_status(operation_id, "reconcile_required", "delete_unknown")
        return "reconcile_required"

    def _set_status(self, operation_id: str, status: str, failure_summary: str) -> None:
        """更新 ingest recovery 事实，删除成功后清除可操作 object key。"""
        with self._session_factory.begin() as session:
            operation = session.get(StorageIngestOperation, operation_id)
            if operation is None:
                return
            operation.status = status
            operation.failure_summary = failure_summary
            if status == "succeeded":
                operation.storage_key = None


class RetentionPayloadScrubService:
    """只清理可删除的消息/通知正文，不改写不可变业务事实和 T18 identity。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """保存短事务数据库会话工厂。"""
        self._session_factory = session_factory

    def scrub_expired_payloads(
        self, policy: RetentionPolicy, *, now: datetime | None = None
    ) -> tuple[int, int]:
        """按独立 data class cutoff scrub raw message 与 notification payload。"""
        current_time = _as_utc(now or utc_now())
        message_cutoff = current_time - timedelta(days=policy.message_payload_retention_days)
        notification_cutoff = current_time - timedelta(
            days=policy.notification_payload_retention_days
        )
        message_count = 0
        notification_count = 0
        with self._session_factory.begin() as session:
            messages = session.scalars(
                select(IncomingMessage).where(IncomingMessage.received_at <= message_cutoff)
            ).all()
            for message in messages:
                if message.raw_payload != {"_retention": "scrubbed"} or message.normalized_text:
                    message.raw_payload = {"_retention": "scrubbed"}
                    message.normalized_text = None
                    message_count += 1
            notifications = session.scalars(
                select(NotificationRecord).where(
                    NotificationRecord.created_at <= notification_cutoff
                )
            ).all()
            for notification in notifications:
                if notification.payload is not None or notification.content is not None:
                    notification.payload = None
                    notification.content = None
                    notification_count += 1
        return message_count, notification_count


def _digest(value: str | None) -> str | None:
    """返回 object key 的安全摘要，不在长期事实中重复保存可操作路径。"""
    return hashlib.sha256(value.encode()).hexdigest() if value else None


def _as_utc(value: datetime) -> datetime:
    """将数据库可能返回的朴素时间统一解释为 UTC。"""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
