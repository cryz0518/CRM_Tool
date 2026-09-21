"""T22 真实 PostgreSQL 并发、fencing 和远端删除恢复验证。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.media.retention import RetentionCleanupService, RetentionPolicy
from app.media.storage import (
    LocalVolumeStorageProvider,
    StorageDeleteOutcome,
    StorageDeleteResult,
)
from app.messaging.models import (
    IncomingMessage,
    MessageAttachment,
    SalesAuthorization,
    StorageCleanupOperation,
)


@pytest.fixture(scope="module")
def postgres_session_factory() -> sessionmaker[Session]:
    """连接 Compose 提供的真实 PostgreSQL，不以 SQLite 替代并发语义。"""
    engine = create_engine(get_settings().database_url, pool_pre_ping=True)
    with engine.connect() as connection:
        connection.execute(select(1))
    factory = sessionmaker(engine)
    try:
        yield factory
    finally:
        engine.dispose()


def _seed_attachment(session_factory: sessionmaker[Session], *, storage_key: str) -> str:
    """写入一次独立附件测试事实并返回附件标识。"""
    suffix = uuid4().hex
    message_id = str(uuid4())
    attachment_id = str(uuid4())
    with session_factory.begin() as session:
        sales_user_id = f"t22-sales-{suffix}"
        session.add(SalesAuthorization(wecom_user_id=sales_user_id, is_authorized=True))
        session.flush()
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=1,
                raw_payload={},
            )
        )
        session.add(
            MessageAttachment(
                id=attachment_id,
                message_id=message_id,
                media_kind="image",
                storage_key=storage_key,
                storage_provider="local",
                scan_status="clean",
                retention_expires_at=datetime.now(UTC) - timedelta(minutes=1),
            )
        )
    return attachment_id


def _policy() -> RetentionPolicy:
    """返回仅用于测试的显式策略版本，不代表生产法务期限。"""
    return RetentionPolicy(
        version=f"test-policy-{uuid4().hex}",
        media_retention_days=7,
        message_payload_retention_days=7,
        notification_payload_retention_days=7,
    )


def test_postgres_concurrent_scheduler_issue_is_unique(
    postgres_session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证两个并发 Scheduler 通过数据库唯一键只产生一个 operation。"""
    storage = LocalVolumeStorageProvider(tmp_path)
    stored = storage.put(b"delete-me", suffix=".png")
    attachment_id = _seed_attachment(postgres_session_factory, storage_key=stored.storage_key)
    policy = _policy()
    service = RetentionCleanupService(postgres_session_factory)
    barrier = Barrier(2)

    def issue() -> str:
        """在并发屏障后签发同一逻辑 operation。"""
        barrier.wait()
        return service.issue_attachment_cleanup(attachment_id, policy=policy).operation_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        operation_ids = list(executor.map(lambda _: issue(), range(2)))

    assert operation_ids[0] == operation_ids[1]
    with postgres_session_factory() as session:
        assert (
            session.scalar(
                select(func.count(StorageCleanupOperation.id)).where(
                    StorageCleanupOperation.operation_key
                    == f"media:{attachment_id}:{policy.version}"
                )
            )
            == 1
        )


def test_postgres_stale_worker_reconciles_confirmed_delete(
    postgres_session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证旧 Worker 失去租约后不能 finalize，新 Worker 先 HEAD 收敛删除事实。"""
    storage = LocalVolumeStorageProvider(tmp_path)
    stored = storage.put(b"delete-me", suffix=".png")
    attachment_id = _seed_attachment(postgres_session_factory, storage_key=stored.storage_key)
    service = RetentionCleanupService(postgres_session_factory)
    operation_id = service.issue_attachment_cleanup(attachment_id, policy=_policy()).operation_id
    claim_a = service.claim(operation_id, lease_seconds=30)
    assert claim_a is not None
    assert storage.delete(stored.storage_key).outcome == StorageDeleteOutcome.CONFIRMED_DELETED
    with postgres_session_factory.begin() as session:
        operation = session.get(StorageCleanupOperation, operation_id)
        assert operation is not None
        operation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    claim_b = service.claim(operation_id, lease_seconds=30)
    assert claim_b is not None
    assert service.finalize_success(operation_id, claim_a) is False
    assert service.execute(operation_id, storage, lease_seconds=30) == "not_claimed"
    assert service.finalize_success(operation_id, claim_b) is True

    with postgres_session_factory() as session:
        attachment = session.get(MessageAttachment, attachment_id)
        assert attachment is not None
        assert attachment.deletion_status == "deleted"
        assert attachment.storage_key is None


def test_postgres_unknown_delete_reconciles_with_head_before_success(
    postgres_session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证 unknown 删除先进入 reconcile，下一轮 HEAD 缺失后才成功。"""

    class UnknownDeleteStorage(LocalVolumeStorageProvider):
        """第一次删除返回 unknown，实际对象仍由测试显式控制。"""

        def delete(self, storage_key: str) -> StorageDeleteResult:
            """模拟网络超时导致的未知远端结果。"""
            return StorageDeleteResult(storage_key, StorageDeleteOutcome.UNKNOWN)

    storage = UnknownDeleteStorage(tmp_path)
    stored = storage.put(b"unknown-delete", suffix=".png")
    attachment_id = _seed_attachment(postgres_session_factory, storage_key=stored.storage_key)
    service = RetentionCleanupService(postgres_session_factory)
    operation_id = service.issue_attachment_cleanup(attachment_id, policy=_policy()).operation_id

    assert service.execute(operation_id, storage, lease_seconds=30) == "reconcile_required"
    assert storage.delete(stored.storage_key).outcome == StorageDeleteOutcome.UNKNOWN
    Path(tmp_path, stored.storage_key).unlink()
    with postgres_session_factory.begin() as session:
        operation = session.get(StorageCleanupOperation, operation_id)
        assert operation is not None
        operation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    assert service.execute(operation_id, storage, lease_seconds=30) == "succeeded"
