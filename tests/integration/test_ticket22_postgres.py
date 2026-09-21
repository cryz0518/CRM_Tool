"""T22 真实 PostgreSQL 并发、fencing 和远端删除恢复验证。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event, Lock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.media.providers import FakeFileScanProvider, MockASRProvider, MockOCRProvider
from app.media.retention import (
    RetentionCleanupScheduler,
    RetentionCleanupService,
    RetentionPolicy,
    StorageIngestRecoveryService,
)
from app.media.service import MediaAttachmentService, MediaValidator
from app.media.storage import (
    FakeStorageProvider,
    LocalVolumeStorageProvider,
    StorageDeleteOutcome,
    StorageDeleteResult,
)
from app.messaging.models import (
    IncomingMessage,
    MessageAttachment,
    SalesAuthorization,
    StorageCleanupOperation,
    StorageIngestOperation,
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


def _seed_message(session_factory: sessionmaker[Session], message_id: str) -> None:
    """写入媒体上传所需的来源消息。"""
    with session_factory.begin() as session:
        sales_user_id = f"t22-ingest-{uuid4().hex}"
        session.add(SalesAuthorization(wecom_user_id=sales_user_id, is_authorized=True))
        # 先落授权目录，确保随后写入消息时满足真实 PostgreSQL 外键约束。
        session.flush()
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id=sales_user_id,
                sequence=1,
                raw_payload={},
            )
        )


def test_postgres_ingest_intent_and_timeout_recovery(
    postgres_session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证真实 PostgreSQL 在 put 前持久化 intent，超时后按固定 key HEAD 收敛。"""
    _seed_message(postgres_session_factory, "message-ingest-pg")

    class TimeoutAfterWriteStorage(FakeStorageProvider):
        """写入对象后抛出未知结果，并统计 put 次数。"""

        def __init__(self, root: Path) -> None:
            """初始化 fake storage 和调用计数。"""
            super().__init__(root)
            self.put_calls = 0

        def put(self, content: bytes, **kwargs: object):  # type: ignore[no-untyped-def]
            """先写远端再模拟连接断开。"""
            self.put_calls += 1
            super().put(content, **kwargs)  # type: ignore[arg-type]
            raise TimeoutError("remote_outcome_unknown")

    storage = TimeoutAfterWriteStorage(tmp_path)
    service = MediaAttachmentService(
        postgres_session_factory,
        MediaValidator(image_mime_types=("image/png",), audio_mime_types=()),
        storage,
        FakeFileScanProvider("clean"),
        MockOCRProvider([]),
        MockASRProvider([]),
    )
    attachment_id = service.ingest(
        "message-ingest-pg",
        b"\x89PNG\r\n\x1a\nimage",
        media_kind="image",
        declared_mime_type="image/png",
    )
    with postgres_session_factory() as session:
        operation = session.scalar(
            select(StorageIngestOperation).where(
                StorageIngestOperation.message_id == "message-ingest-pg"
            )
        )
        assert operation is not None
        assert operation.attachment_id == attachment_id
        assert operation.status == "reconcile_required"
        operation_id = operation.id

    assert StorageIngestRecoveryService(postgres_session_factory).reconcile(
        operation_id, storage
    ) == ("succeeded")
    assert storage.put_calls == 1


def test_postgres_ingest_stale_claim_cannot_start_put(
    postgres_session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证 ingest stale Worker 失去 fencing 后不能启动第二次 put。"""
    _seed_message(postgres_session_factory, "message-ingest-fence")
    storage = FakeStorageProvider(tmp_path)
    service = MediaAttachmentService(
        postgres_session_factory,
        MediaValidator(image_mime_types=("image/png",), audio_mime_types=()),
        storage,
        FakeFileScanProvider("clean"),
        MockOCRProvider([]),
        MockASRProvider([]),
    )
    service.ingest(
        "message-ingest-fence",
        b"\x89PNG\r\n\x1a\nimage",
        media_kind="image",
        declared_mime_type="image/png",
    )
    with postgres_session_factory() as session:
        operation = session.scalar(
            select(StorageIngestOperation).where(
                StorageIngestOperation.message_id == "message-ingest-fence"
            )
        )
        assert operation is not None
        operation_id = operation.id
        attachment = session.get(MessageAttachment, operation.attachment_id)
        assert attachment is not None and attachment.storage_key is not None
        frozen_key = attachment.storage_key
    with postgres_session_factory.begin() as session:
        operation = session.get(StorageIngestOperation, operation_id)
        assert operation is not None
        operation.status = "pending"
        operation.storage_key = frozen_key
        operation.remote_outcome = "not_started"
    recovery = StorageIngestRecoveryService(postgres_session_factory)
    claim_a = recovery._claim(operation_id, lease_seconds=30)
    assert claim_a is not None
    with postgres_session_factory.begin() as session:
        operation = session.get(StorageIngestOperation, operation_id)
        assert operation is not None
        operation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    claim_b = recovery._claim(operation_id, lease_seconds=30)
    assert claim_b is not None and claim_a != claim_b
    assert recovery._mark_remote_started(operation_id, claim_a) is False


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
                    StorageCleanupOperation.operation_key == f"media:{attachment_id}"
                )
            )
            == 1
        )


def test_postgres_scheduler_requeues_expired_cleanup_lease(
    postgres_session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证 Worker 崩溃留下的过期 cleanup lease 会再次进入队列。"""
    storage = LocalVolumeStorageProvider(tmp_path)
    stored = storage.put(b"requeue-me", suffix=".png")
    attachment_id = _seed_attachment(postgres_session_factory, storage_key=stored.storage_key)
    service = RetentionCleanupService(postgres_session_factory)
    operation_id = service.issue_attachment_cleanup(attachment_id, policy=_policy()).operation_id
    claim = service.claim(operation_id, lease_seconds=30)
    assert claim is not None
    expired_at = datetime.now(UTC) - timedelta(seconds=1)
    with postgres_session_factory.begin() as session:
        operation = session.get(StorageCleanupOperation, operation_id)
        assert operation is not None
        operation.lease_expires_at = expired_at

    runnable = RetentionCleanupScheduler(postgres_session_factory).runnable_operation_ids(
        batch_size=10, now=datetime.now(UTC)
    )
    assert operation_id in runnable


def test_postgres_stale_worker_reconciles_confirmed_delete(
    postgres_session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证旧 Worker 失去租约后不能 finalize，新 Worker 先 HEAD 收敛删除事实。"""

    class BlockingDeleteStorage(LocalVolumeStorageProvider):
        """让第一个 Worker 在 remote delete 中阻塞，制造真实 takeover 时序。"""

        def __init__(self, root: Path) -> None:
            """初始化阻塞事件、调用顺序和线程安全计数。"""
            super().__init__(root)
            self.delete_started = Event()
            self.release_first_delete = Event()
            self.events: list[str] = []
            self._lock = Lock()

        def head(self, storage_key: str):  # type: ignore[no-untyped-def]
            """记录 HEAD 调用顺序后委托本地对象状态查询。"""
            with self._lock:
                self.events.append("head")
            return super().head(storage_key)

        def delete(self, storage_key: str):  # type: ignore[no-untyped-def]
            """第一次 delete 阻塞，释放后返回真实三态删除结果。"""
            with self._lock:
                first = "delete" not in self.events
                self.events.append("delete")
            if first:
                self.delete_started.set()
                assert self.release_first_delete.wait(timeout=10)
            return super().delete(storage_key)

    storage = BlockingDeleteStorage(tmp_path)
    stored = storage.put(b"delete-me", suffix=".png")
    attachment_id = _seed_attachment(postgres_session_factory, storage_key=stored.storage_key)
    service = RetentionCleanupService(postgres_session_factory)
    operation_id = service.issue_attachment_cleanup(attachment_id, policy=_policy()).operation_id
    with ThreadPoolExecutor(max_workers=2) as executor:
        worker_a = executor.submit(service.execute, operation_id, storage, lease_seconds=1)
        assert storage.delete_started.wait(timeout=10)
        with postgres_session_factory.begin() as session:
            operation = session.get(StorageCleanupOperation, operation_id)
            assert operation is not None
            operation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        worker_b = executor.submit(service.execute, operation_id, storage, lease_seconds=30)
        result_b = worker_b.result(timeout=10)
        storage.release_first_delete.set()
        result_a = worker_a.result(timeout=10)

    assert result_b == "succeeded"
    assert result_a == "stale_claim"
    assert storage.events[:2] == ["head", "delete"]

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
    with postgres_session_factory() as session:
        operation = session.get(StorageCleanupOperation, operation_id)
        assert operation is not None
        assert operation.remote_outcome == StorageDeleteOutcome.UNKNOWN.value
    Path(tmp_path, stored.storage_key).unlink()
    with postgres_session_factory.begin() as session:
        operation = session.get(StorageCleanupOperation, operation_id)
        assert operation is not None
        operation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    assert service.execute(operation_id, storage, lease_seconds=30) == "succeeded"
