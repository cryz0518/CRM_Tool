"""T22 focused fix 的 intent、扫描竞态、scrub 和 provider fail-closed 测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import Settings
from app.media.dependencies import get_media_scanner, get_media_storage_provider
from app.media.providers import FakeFileScanProvider, MockASRProvider
from app.media.retention import RetentionPayloadScrubService, RetentionPolicy
from app.media.service import MediaAttachmentService, MediaValidator
from app.media.storage import FakeStorageProvider
from app.messaging.models import (
    Base,
    IncomingMessage,
    MessageAttachment,
    NotificationRecord,
    OutboxEvent,
    SalesAuthorization,
    StorageIngestOperation,
)


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    """提供隔离 SQLite 结构，验证事务顺序而不替代 PostgreSQL 并发测试。"""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    try:
        yield sessionmaker(engine)
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def _seed_message(session_factory: sessionmaker[Session], message_id: str) -> None:
    """创建媒体测试所需的销售和来源消息。"""
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="t22-sales", is_authorized=True))
        session.add(
            IncomingMessage(
                message_id=message_id,
                sales_user_id="t22-sales",
                sequence=1,
                raw_payload={"body": "sensitive"},
                received_at=datetime.now(UTC) - timedelta(days=30),
            )
        )


def _media_service(
    session_factory: sessionmaker[Session],
    storage: FakeStorageProvider,
    scanner: FakeFileScanProvider,
    *,
    retention: RetentionPolicy | None = None,
    ocr: object | None = None,
) -> MediaAttachmentService:
    """构造使用 fake seam 的媒体服务。"""
    return MediaAttachmentService(
        session_factory,
        MediaValidator(image_mime_types=("image/png",), audio_mime_types=()),
        storage,
        scanner,
        ocr or MockASRProvider([]),  # type: ignore[arg-type]
        MockASRProvider([]),
        retention_policy=retention,
    )


def test_ingest_commits_intent_before_put_and_freezes_retention(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证 put 期间已经能从独立会话看到 intent、冻结 key 和 retention cutoff。"""
    _seed_message(session_factory, "message-intent")
    storage = FakeStorageProvider(tmp_path)
    observed: list[bool] = []

    class ObservedStorage(FakeStorageProvider):
        """在 put 内用独立会话验证 intent 已提交。"""

        def put(self, content: bytes, **kwargs: object):  # type: ignore[no-untyped-def]
            with session_factory() as session:
                observed.append(session.scalar(select(StorageIngestOperation)) is not None)
            return super().put(content, **kwargs)  # type: ignore[arg-type]

    storage = ObservedStorage(tmp_path)
    policy = RetentionPolicy("policy-a", 7, 30, 30)
    attachment_id = _media_service(
        session_factory, storage, FakeFileScanProvider("clean"), retention=policy
    ).ingest(
        "message-intent",
        b"\x89PNG\r\n\x1a\nimage",
        media_kind="image",
        declared_mime_type="image/png",
    )

    assert observed == [True]
    with session_factory() as session:
        attachment = session.get(MessageAttachment, attachment_id)
        operation = session.scalar(select(StorageIngestOperation))
        assert attachment is not None and operation is not None
        assert attachment.retention_policy_version == "policy-a"
        assert attachment.retention_expires_at is not None
        assert operation.storage_key is None
        assert attachment.storage_key is not None
        assert "message-intent" not in attachment.storage_key


def test_ingest_timeout_with_remote_success_reconciles_without_second_put(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证 put 超时但远端已落盘时 recovery 只 HEAD 固定 key。"""
    _seed_message(session_factory, "message-timeout")

    class TimeoutAfterWriteStorage(FakeStorageProvider):
        """写入对象后抛出未知结果异常，模拟网络断开。"""

        def __init__(self, root: Path) -> None:
            """初始化 put 调用计数。"""
            super().__init__(root)
            self.put_calls = 0

        def put(self, content: bytes, **kwargs: object):  # type: ignore[no-untyped-def]
            self.put_calls += 1
            super().put(content, **kwargs)  # type: ignore[arg-type]
            raise TimeoutError("remote_outcome_unknown")

    storage = TimeoutAfterWriteStorage(tmp_path)
    attachment_id = _media_service(session_factory, storage, FakeFileScanProvider("clean")).ingest(
        "message-timeout",
        b"\x89PNG\r\n\x1a\nimage",
        media_kind="image",
        declared_mime_type="image/png",
    )
    with session_factory() as session:
        operation = session.scalar(select(StorageIngestOperation))
        assert operation is not None
        operation_id = operation.id
    from app.media.retention import StorageIngestRecoveryService

    assert (
        StorageIngestRecoveryService(session_factory).reconcile(operation_id, storage)
        == "succeeded"
    )
    assert storage.put_calls == 1
    with session_factory() as session:
        assert session.get(MessageAttachment, attachment_id).processing_status == "pending"  # type: ignore[union-attr]


def test_scan_transition_and_clean_quarantine_race_block_finalize(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证非法扫描跃迁被拒绝，识别返回后变隔离不会写入正文。"""
    _seed_message(session_factory, "message-scan")
    storage = FakeStorageProvider(tmp_path)
    service_holder: list[MediaAttachmentService] = []

    class QuarantiningOCR:
        """在外部 OCR 返回前触发隔离，制造 TOCTOU。"""

        def recognize(self, content: bytes, *, mime_type: str, timeout_seconds: float) -> str:
            """先隔离附件再返回文本，验证 finalize 边界会再次检查状态。"""
            del content, mime_type, timeout_seconds
            with session_factory() as session:
                attachment_id = session.scalar(select(MessageAttachment.id))
            service_holder[0].apply_scan_result(attachment_id, "infected")
            return "不应写入"

    service = _media_service(
        session_factory,
        storage,
        FakeFileScanProvider("clean"),
        ocr=QuarantiningOCR(),
    )
    service_holder.append(service)
    attachment_id = service.ingest(
        "message-scan",
        b"\x89PNG\r\n\x1a\nimage",
        media_kind="image",
        declared_mime_type="image/png",
    )
    service.process_pending_for_message("message-scan")
    with pytest.raises(ValueError, match="scan_transition_invalid"):
        service.apply_scan_result(attachment_id, "clean")
    with session_factory() as session:
        attachment = session.get(MessageAttachment, attachment_id)
        assert attachment is not None
        assert attachment.scan_status == "infected"
        assert attachment.recognized_text is None


def test_active_message_and_notification_payloads_are_not_scrubbed(
    session_factory: sessionmaker[Session],
) -> None:
    """验证 pending Outbox、媒体任务和通知 sender lease 会保护 payload。"""
    _seed_message(session_factory, "message-active")
    with session_factory.begin() as session:
        session.add(
            OutboxEvent(
                message_id="message-active",
                sales_user_id="t22-sales",
                sequence=1,
                status="pending",
            )
        )
        session.add(
            NotificationRecord(
                notification_key="notification-active",
                sales_user_id="t22-sales",
                source_message_id="message-active",
                notification_type="media_text_input_required",
                content="仍需发送",
                payload={"msgtype": "text"},
                status="pending",
            )
        )
    policy = RetentionPolicy("policy-scrub", 7, 1, 1)
    assert RetentionPayloadScrubService(session_factory).scrub_expired_payloads(policy) == (0, 0)
    with session_factory() as session:
        message = session.get(IncomingMessage, "message-active")
        notice = session.get(NotificationRecord, "notification-active")
        assert message is not None and message.raw_payload != {"_retention": "scrubbed"}
        assert notice is not None and notice.payload is not None and notice.content is not None


def test_production_rejects_local_fake_storage_and_scanner(tmp_path: Path) -> None:
    """验证 production provider resolution 本身 fail closed，而非只把 readiness 标红。"""
    for provider in ("local", "fake"):
        with pytest.raises(RuntimeError, match="生产环境禁止"):
            get_media_storage_provider(
                Settings(_env_file=None, app_env="production", media_storage_provider=provider)
            )
    for provider in ("noop", "fake"):
        with pytest.raises(RuntimeError, match="生产环境禁止"):
            get_media_scanner(
                Settings(_env_file=None, app_env="production", media_scanner_provider=provider)
            )
