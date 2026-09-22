"""T22 retention policy、清理操作签发和租约 fencing。"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

from sqlalchemy import and_, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.console.models import AIExecutionRecord
from app.core.config import Settings
from app.leads.models import CrmSyncRecord, Lead, MessageRetryAttempt, SmartTableSync
from app.media.storage import GeneratedObjectKey, StorageDeleteOutcome, StorageProvider
from app.messaging.models import (
    IncomingMessage,
    MediaProcessingTask,
    MessageAttachment,
    NotificationRecord,
    OutboxEvent,
    StorageCleanupOperation,
    StorageIngestOperation,
    utc_now,
)

logger = logging.getLogger(__name__)


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
        # policy 版本只冻结在 operation 元数据中，不参与逻辑身份。
        operation_key = f"media:{attachment_id}"
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
                            policy_version=attachment.retention_policy_version or policy.version,
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
            if attachment.deletion_status == "deleted" and operation.status != "succeeded":
                # 已有 tombstone 但没有已确认远端事实时，禁止重新签发并伪装清理成功。
                raise ValueError("attachment_tombstone_recovery_required")
            attachment.cleanup_operation_id = operation.id
            logger.info(
                "retention_operation_issued",
                extra={
                    "media_id": attachment_id,
                    "operation_id": operation.id,
                    "status": operation.status,
                },
            )
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
            current = session.get(StorageCleanupOperation, operation_id)
            if current is None or current.remote_outcome not in {
                StorageDeleteOutcome.CONFIRMED_DELETED.value,
                StorageDeleteOutcome.NOT_FOUND.value,
            }:
                return False
            attachment = session.get(MessageAttachment, current.target_id)
            if attachment is None:
                # 没有附件行就不能写完整 tombstone；保留 operation 供人工恢复。
                return False
            result = cast(
                CursorResult[object],
                session.execute(
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
                ),
            )
            if result.rowcount != 1:
                return False
            operation = session.get(StorageCleanupOperation, operation_id)
            if operation is None:
                return False
            attachment.storage_key = None
            attachment.storage_etag = None
            attachment.deletion_status = "deleted"
            attachment.deleted_at = completed_at
            attachment.deletion_reason = "retention_cleanup"
            attachment.cleanup_operation_id = operation.id
            logger.info(
                "retention_completed",
                extra={
                    "media_id": operation.target_id,
                    "operation_id": operation.id,
                    "status": "succeeded",
                },
            )
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
            if (
                attachment is not None
                and attachment.deletion_status == "deleted"
                and operation.remote_outcome
                in {
                    StorageDeleteOutcome.CONFIRMED_DELETED.value,
                    StorageDeleteOutcome.NOT_FOUND.value,
                }
            ):
                if not self._set_remote_outcome(
                    operation_id, claim, StorageDeleteOutcome.NOT_FOUND
                ):
                    return "stale_claim"
                return (
                    "succeeded"
                    if self.finalize_success(operation_id, claim, now=now)
                    else "stale_claim"
                )
        if not storage_key:
            self._mark_failure(operation_id, claim, "storage_key_missing", "清理对象键缺失")
            return "failed_pending_review"

        try:
            metadata = storage.head(storage_key)
        except Exception:
            self._mark_failure(
                operation_id, claim, "head_failed", "对象状态检查失败", reconcile=True
            )
            return "reconcile_required"
        if metadata is None:
            if not self._set_remote_outcome(operation_id, claim, StorageDeleteOutcome.NOT_FOUND):
                return "stale_claim"
            return (
                "succeeded"
                if self.finalize_success(operation_id, claim, now=now)
                else "stale_claim"
            )

        if not self._mark_remote_started(operation_id, claim):
            return "stale_claim"
        try:
            result = storage.delete(storage_key)
        except Exception:
            # 连接断开无法证明远端没有执行 delete，下一轮必须先 HEAD。
            self._mark_failure(
                operation_id, claim, "delete_unknown", "对象删除结果未知", reconcile=True
            )
            return "reconcile_required"
        if result.outcome in {
            StorageDeleteOutcome.CONFIRMED_DELETED,
            StorageDeleteOutcome.NOT_FOUND,
        }:
            if not self._set_remote_outcome(operation_id, claim, result.outcome):
                return "stale_claim"
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
            result = cast(
                CursorResult[object],
                session.execute(
                    update(StorageCleanupOperation)
                    .where(
                        StorageCleanupOperation.id == operation_id,
                        StorageCleanupOperation.status == "processing",
                        StorageCleanupOperation.claim_token == claim.claim_token,
                        StorageCleanupOperation.generation == claim.generation,
                    )
                    .values(remote_outcome=outcome.value, updated_at=utc_now())
                ),
            )
            return bool(result.rowcount == 1)

    def _mark_remote_started(self, operation_id: str, claim: CleanupClaim) -> bool:
        """在远端 delete 前提交 remote-started 事实并执行 claim fencing。"""
        with self._session_factory.begin() as session:
            result = cast(
                CursorResult[object],
                session.execute(
                    update(StorageCleanupOperation)
                    .where(
                        StorageCleanupOperation.id == operation_id,
                        StorageCleanupOperation.status == "processing",
                        StorageCleanupOperation.claim_token == claim.claim_token,
                        StorageCleanupOperation.generation == claim.generation,
                    )
                    .values(remote_started_at=utc_now(), updated_at=utc_now())
                ),
            )
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
            result = cast(
                CursorResult[object],
                session.execute(
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
                ),
            )
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
        cursor: tuple[datetime, str] | None = None,
    ) -> list[str]:
        """按 keyset 风格扫描到期附件并幂等签发 operation 标识。"""
        if batch_size <= 0:
            raise ValueError("batch_size 必须大于 0")
        current_time = _as_utc(now or utc_now())
        with self._session_factory() as session:
            filters = [
                MessageAttachment.deletion_status == "active",
                MessageAttachment.retention_expires_at.is_not(None),
                MessageAttachment.retention_expires_at <= current_time,
                # 已有 logical operation 的行由 Worker 重试，避免每轮从头反复扫描同一批。
                MessageAttachment.cleanup_operation_id.is_(None),
            ]
            if cursor is not None:
                filters.append(
                    or_(
                        MessageAttachment.retention_expires_at > cursor[0],
                        and_(
                            MessageAttachment.retention_expires_at == cursor[0],
                            MessageAttachment.id > cursor[1],
                        ),
                    )
                )
            attachment_ids = session.scalars(
                select(MessageAttachment.id)
                .where(*filters)
                .order_by(MessageAttachment.retention_expires_at, MessageAttachment.id)
                .limit(batch_size)
            ).all()
        return [
            self._operations.issue_attachment_cleanup(
                attachment_id, policy=policy, now=current_time
            ).operation_id
            for attachment_id in attachment_ids
        ]

    def runnable_operation_ids(
        self, *, batch_size: int, now: datetime | None = None
    ) -> list[str]:
        """扫描待重试或租约已过期的 operation，供 Worker 重新入队。"""
        if batch_size <= 0:
            raise ValueError("batch_size 必须大于 0")
        current_time = _as_utc(now or utc_now())
        with self._session_factory() as session:
            return list(session.scalars(
                select(StorageCleanupOperation.id)
                .where(
                    or_(
                        StorageCleanupOperation.status.in_(
                            ("pending", "retrying", "reconcile_required")
                        ),
                        and_(
                            StorageCleanupOperation.status == "processing",
                            or_(
                                StorageCleanupOperation.lease_expires_at.is_(None),
                                StorageCleanupOperation.lease_expires_at <= current_time,
                            ),
                        ),
                    )
                )
                .order_by(StorageCleanupOperation.created_at, StorageCleanupOperation.id)
                .limit(batch_size)
            ).all())


class StorageIngestRecoveryService:
    """执行带固定 object key、lease 和 fencing 的上传及恢复。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        """保存短事务数据库会话工厂。"""
        self._session_factory = session_factory

    def upload(
        self,
        operation_id: str,
        storage: StorageProvider,
        *,
        content: bytes,
        suffix: str,
        timeout_seconds: float,
    ) -> str:
        """认领 intent、先 HEAD 再 put，并以 fenced finalize 收敛附件。"""
        claim = self._claim(operation_id, lease_seconds=max(30, int(timeout_seconds) + 10))
        if claim is None:
            return "not_claimed"
        try:
            return self._put_or_reconcile(
                operation_id, claim, storage, content=content, suffix=suffix
            )
        except Exception:
            # intent 与 remote-started 已提交时，任何 DB 短暂失败都留给下一轮 recovery。
            logger.exception(
                "media_ingest_reconcile_required",
                extra={"operation_id": operation_id, "status": "reconcile_required"},
            )
            return "reconcile_required"

    def reconcile(
        self,
        operation_id: str,
        storage: StorageProvider,
        *,
        content: bytes | None = None,
        suffix: str = "",
    ) -> str:
        """接管 recovery；对象缺失时只允许用原 key 受控重试，绝不生成新 key。"""
        claim = self._claim(operation_id, lease_seconds=300)
        if claim is None:
            return "not_claimed"
        with self._session_factory() as session:
            operation = session.get(StorageIngestOperation, operation_id)
            if operation is None:
                return "missing_operation"
            storage_key = operation.storage_key
        if not storage_key:
            return "failed_pending_review"
        try:
            metadata = storage.head(storage_key)
        except Exception:
            self._set_remote_status(operation_id, claim, "unknown", "head_failed")
            return "reconcile_required"
        if metadata is not None:
            if not self._set_remote_status(operation_id, claim, "exists", "head_exists"):
                return "stale_claim"
            return "succeeded" if self._finalize(operation_id, claim, metadata) else "stale_claim"
        if content is None:
            self._set_remote_status(operation_id, claim, "not_found", "object_missing")
            return "failed_pending_review"
        return self._put_or_reconcile(
            operation_id, claim, storage, content=content, suffix=suffix, head_checked=True
        )

    def _claim(self, operation_id: str, *, lease_seconds: int) -> tuple[str, int] | None:
        """原子认领 ingest operation 并递增 generation。"""
        now = utc_now()
        with self._session_factory.begin() as session:
            operation = session.scalar(
                select(StorageIngestOperation)
                .where(StorageIngestOperation.id == operation_id)
                .with_for_update()
            )
            if operation is None or operation.status == "succeeded":
                return None
            expired = operation.status == "processing" and (
                operation.lease_expires_at is None or _as_utc(operation.lease_expires_at) <= now
            )
            if operation.status == "processing" and not expired:
                return None
            if (
                operation.status not in {"pending", "retrying", "reconcile_required"}
                and not expired
            ):
                return None
            operation.status = "processing"
            operation.claim_token = uuid4().hex
            operation.generation += 1
            operation.attempt_count += 1
            operation.processing_started_at = now
            operation.lease_expires_at = now + timedelta(seconds=lease_seconds)
            return operation.claim_token, operation.generation

    def _put_or_reconcile(
        self,
        operation_id: str,
        claim: tuple[str, int],
        storage: StorageProvider,
        *,
        content: bytes,
        suffix: str,
        head_checked: bool = False,
    ) -> str:
        """在冻结 key 上执行 put；未知结果进入 reconcile_required。"""
        with self._session_factory() as session:
            operation = session.get(StorageIngestOperation, operation_id)
            if operation is None or not operation.storage_key:
                return "failed_pending_review"
            storage_key = operation.storage_key
            content_type = operation.content_type
            content_sha256 = operation.content_sha256
        if not head_checked:
            try:
                metadata = storage.head(storage_key)
            except Exception:
                self._set_remote_status(operation_id, claim, "unknown", "head_failed")
                return "reconcile_required"
            if metadata is not None:
                if not self._set_remote_status(operation_id, claim, "exists", "head_exists"):
                    return "stale_claim"
                return (
                    "succeeded" if self._finalize(operation_id, claim, metadata) else "stale_claim"
                )
        if not self._mark_remote_started(operation_id, claim):
            return "stale_claim"
        try:
            stored = storage.put(
                content,
                suffix=suffix,
                object_key=GeneratedObjectKey(storage_key),
                content_type=content_type,
                sha256=content_sha256,
            )
        except Exception:
            self._set_remote_status(operation_id, claim, "unknown", "put_unknown")
            return "reconcile_required"
        if not self._set_remote_status(operation_id, claim, "put_succeeded", "put_succeeded"):
            return "stale_claim"
        return "succeeded" if self._finalize(operation_id, claim, stored) else "reconcile_required"

    def _mark_remote_started(self, operation_id: str, claim: tuple[str, int]) -> bool:
        """在外部 put 前提交 remote-started 事实。"""
        token, generation = claim
        with self._session_factory.begin() as session:
            result = cast(
                CursorResult[object],
                session.execute(
                    update(StorageIngestOperation)
                    .where(
                        StorageIngestOperation.id == operation_id,
                        StorageIngestOperation.status == "processing",
                        StorageIngestOperation.claim_token == token,
                        StorageIngestOperation.generation == generation,
                    )
                    .values(remote_started_at=utc_now(), updated_at=utc_now())
                ),
            )
            return bool(result.rowcount == 1)

    def _set_remote_status(
        self, operation_id: str, claim: tuple[str, int], outcome: str, summary: str
    ) -> bool:
        """持久化远端事实，只有当前 claimant 可以写入。"""
        token, generation = claim
        with self._session_factory.begin() as session:
            result = cast(
                CursorResult[object],
                session.execute(
                    update(StorageIngestOperation)
                    .where(
                        StorageIngestOperation.id == operation_id,
                        StorageIngestOperation.status == "processing",
                        StorageIngestOperation.claim_token == token,
                        StorageIngestOperation.generation == generation,
                    )
                    .values(
                        remote_outcome=outcome,
                        failure_summary=summary,
                        status=(
                            "reconcile_required"
                            if outcome == "unknown"
                            else "failed_pending_review"
                            if outcome == "not_found"
                            else "processing"
                        ),
                        lease_expires_at=None,
                        updated_at=utc_now(),
                    )
                ),
            )
            return bool(result.rowcount == 1)

    def _finalize(self, operation_id: str, claim: tuple[str, int], stored: object) -> bool:
        """以 generation fencing 完成附件本地状态并清理 operation 的可操作 key。"""
        token, generation = claim
        now = utc_now()
        with self._session_factory.begin() as session:
            operation = session.get(StorageIngestOperation, operation_id)
            if operation is None or operation.remote_outcome not in {"exists", "put_succeeded"}:
                return False
            attachment = session.get(MessageAttachment, operation.attachment_id)
            if attachment is None:
                # intent 仍需保留给人工恢复，不能先把唯一可操作 key scrub 掉。
                return False
            result = cast(
                CursorResult[object],
                session.execute(
                    update(StorageIngestOperation)
                    .where(
                        StorageIngestOperation.id == operation_id,
                        StorageIngestOperation.status == "processing",
                        StorageIngestOperation.claim_token == token,
                        StorageIngestOperation.generation == generation,
                    )
                    .values(
                        status="succeeded",
                        completed_at=now,
                        updated_at=now,
                        lease_expires_at=None,
                        storage_key=None,
                    )
                ),
            )
            if result.rowcount != 1:
                return False
            attachment.processing_status = "pending"
            attachment.storage_etag = getattr(stored, "etag", None)
            attachment.storage_encryption_mode = getattr(stored, "encryption_mode", None)
            session.add(
                MediaProcessingTask(attachment_id=attachment.id, task_type=attachment.media_kind)
            )
            logger.info(
                "media_ingest_completed",
                extra={
                    "media_id": attachment.id,
                    "operation_id": operation_id,
                    "status": "succeeded",
                },
            )
            return True


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
            active_message_ids = set(
                session.scalars(
                    select(OutboxEvent.message_id).where(
                        OutboxEvent.status.in_(("pending", "processing", "retrying"))
                    )
                ).all()
            )
            active_message_ids.update(
                session.scalars(
                    select(MessageRetryAttempt.message_id).where(
                        MessageRetryAttempt.status.in_(("pending", "processing", "retrying"))
                    )
                ).all()
            )
            # CRM、智能表格和 AI 仍在处理中时，保留其唯一的来源消息正文供恢复使用。
            for message_id in session.scalars(
                select(Lead.source_message_id).where(
                    Lead.source_message_id.is_not(None),
                    Lead.lifecycle_state.in_(
                        ("temporary", "pending_create", "pending_update")
                    ),
                )
            ).all():
                if message_id is not None:
                    active_message_ids.add(message_id)
            active_message_ids.update(
                session.scalars(
                    select(CrmSyncRecord.request_message_id).where(
                        CrmSyncRecord.status.in_(
                            ("pending", "processing", "retrying")
                        )
                    )
                ).all()
            )
            active_message_ids.update(
                session.scalars(
                    select(SmartTableSync.source_message_id).where(
                        SmartTableSync.status.in_(
                            ("pending", "processing", "retrying")
                        )
                    )
                ).all()
            )
            for message_id in session.scalars(
                select(AIExecutionRecord.message_id).where(
                    AIExecutionRecord.message_id.is_not(None),
                    AIExecutionRecord.status.in_(
                        ("pending", "processing", "retrying")
                    ),
                )
            ).all():
                if message_id is not None:
                    active_message_ids.add(message_id)
            active_message_ids.update(
                session.scalars(
                    select(MessageAttachment.message_id)
                    .join(
                        MediaProcessingTask,
                        MediaProcessingTask.attachment_id == MessageAttachment.id,
                    )
                    .where(MediaProcessingTask.status.in_(("pending", "processing", "retrying")))
                ).all()
            )
            messages = session.scalars(
                select(IncomingMessage).where(
                    IncomingMessage.received_at <= message_cutoff,
                    IncomingMessage.scrubbed_at.is_(None),
                    IncomingMessage.requires_media_enrichment.is_(False),
                ).with_for_update()
            ).all()
            for message in messages:
                if message.message_id in active_message_ids:
                    continue
                if message.raw_payload != {"_retention": "scrubbed"} or message.normalized_text:
                    message.raw_payload = {"_retention": "scrubbed"}
                    message.normalized_text = None
                    message.scrubbed_at = current_time
                    message.retention_policy_version = policy.version
                    message_count += 1
            notifications = session.scalars(
                select(NotificationRecord).where(
                    NotificationRecord.created_at <= notification_cutoff,
                    NotificationRecord.scrubbed_at.is_(None),
                    NotificationRecord.status.in_(("succeeded", "failed", "denied", "expired")),
                    NotificationRecord.processing_claim_token.is_(None),
                    NotificationRecord.processing_lease_expires_at.is_(None),
                ).with_for_update()
            ).all()
            for notification in notifications:
                if notification.payload is not None or notification.content is not None:
                    notification.payload = None
                    notification.content = None
                    notification.scrubbed_at = current_time
                    notification.retention_policy_version = policy.version
                    notification_count += 1
        logger.info(
            "retention_payload_scrubbed",
            extra={
                "status": "succeeded",
                "message_count": message_count,
                "notification_count": notification_count,
            },
        )
        return message_count, notification_count


def _digest(value: str | None) -> str | None:
    """返回 object key 的安全摘要，不在长期事实中重复保存可操作路径。"""
    return hashlib.sha256(value.encode()).hexdigest() if value else None


def _as_utc(value: datetime) -> datetime:
    """将数据库可能返回的朴素时间统一解释为 UTC。"""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
