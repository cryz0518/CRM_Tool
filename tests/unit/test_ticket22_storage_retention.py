"""T22 生产存储、扫描、签名访问和清理操作的公共 Seam 测试。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.console.break_glass import (
    BreakGlassAccessError,
    BreakGlassAccessRequest,
    BreakGlassAccessService,
)
from app.console.models import BreakGlassAccessAudit
from app.core.config import Settings
from app.media.providers import FakeFileScanProvider, MockASRProvider, MockOCRProvider
from app.media.readiness import ProductionMediaReadinessChecker
from app.media.retention import (
    RetentionCleanupService,
    RetentionPolicy,
    StorageIngestRecoveryService,
)
from app.media.service import MediaAttachmentService, MediaValidator
from app.media.storage import (
    LocalVolumeStorageProvider,
    StorageDeleteOutcome,
)
from app.messaging.models import (
    Base,
    IncomingMessage,
    MessageAttachment,
    SalesAuthorization,
    StorageIngestOperation,
)


@pytest.fixture
def session_factory() -> sessionmaker[Session]:
    """提供隔离的 T22 单元测试数据库会话工厂。"""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(engine)
    try:
        yield factory
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_local_storage_exposes_metadata_and_idempotent_delete(tmp_path: Path) -> None:
    """验证本地开发存储也遵守 head 和三态删除契约。"""
    storage = LocalVolumeStorageProvider(tmp_path)

    stored = storage.put(b"opaque-content", suffix=".png", content_type="image/png")

    metadata = storage.head(stored.storage_key)
    assert metadata is not None
    assert metadata.size_bytes == len(b"opaque-content")
    assert storage.delete(stored.storage_key).outcome == StorageDeleteOutcome.CONFIRMED_DELETED
    assert storage.delete(stored.storage_key).outcome == StorageDeleteOutcome.NOT_FOUND


def test_production_readiness_fails_closed_without_real_capabilities() -> None:
    """验证生产不能因为本地存储和 Noop 扫描器存在就判定 ready。"""
    settings = Settings(_env_file=None, app_env="production")

    report = ProductionMediaReadinessChecker().check(settings)

    assert report.ready is False
    assert "生产对象存储未配置" in report.issues
    assert "生产文件扫描器未配置" in report.issues
    assert "媒体保留期策略未显式配置" in report.issues


def test_retention_policy_rejects_zero_and_negative_values() -> None:
    """验证 0 和负数不会被解释为隐含的永久保留或立即删除。"""
    with pytest.raises(ValueError, match="必须大于 0"):
        RetentionPolicy(
            version="policy-1",
            media_retention_days=0,
            message_payload_retention_days=7,
            notification_payload_retention_days=7,
        )
    with pytest.raises(ValueError, match="必须大于 0"):
        RetentionPolicy(
            version="policy-1",
            media_retention_days=7,
            message_payload_retention_days=-1,
            notification_payload_retention_days=7,
        )


def test_cleanup_issuance_is_stable_and_fenced(session_factory: sessionmaker[Session]) -> None:
    """验证重复签发只得到一个 operation，旧租约不能覆盖新 Worker。"""
    now = datetime.now(UTC)
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True))
        session.add(
            IncomingMessage(
                message_id="message-1",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={"body": "sensitive"},
            )
        )
        session.add(
            MessageAttachment(
                id="attachment-1",
                message_id="message-1",
                media_kind="image",
                storage_key="artifacts/opaque",
                scan_status="clean",
                retention_expires_at=now - timedelta(minutes=1),
            )
        )

    policy = RetentionPolicy(
        version="policy-1",
        media_retention_days=7,
        message_payload_retention_days=7,
        notification_payload_retention_days=7,
    )
    service = RetentionCleanupService(session_factory)
    first = service.issue_attachment_cleanup("attachment-1", policy=policy, now=now)
    second = service.issue_attachment_cleanup("attachment-1", policy=policy, now=now)

    assert first.operation_id == second.operation_id
    claim_a = service.claim(first.operation_id, now=now, lease_seconds=30)
    assert claim_a is not None
    with session_factory.begin() as session:
        operation = session.get(type(first.operation), first.operation_id)
        assert operation is not None
        operation.lease_expires_at = now - timedelta(seconds=1)
    claim_b = service.claim(first.operation_id, now=now, lease_seconds=30)
    assert claim_b is not None
    assert claim_a.claim_token != claim_b.claim_token
    assert service.finalize_success(first.operation_id, claim_a) is False
    assert service.finalize_success(first.operation_id, claim_b) is True


def test_break_glass_missing_object_never_calls_signer(
    session_factory: sessionmaker[Session],
) -> None:
    """验证对象已不存在时，授权审计成功也不能生成签名地址。"""

    class FakeStorage:
        """只返回对象不存在事实的测试存储边界。"""

        def head(self, storage_key: str) -> None:
            """返回缺失对象事实。"""
            del storage_key
            return None

    class FakeSigner:
        """记录签名调用次数的测试签名边界。"""

        def __init__(self) -> None:
            """初始化调用记录。"""
            self.calls = 0

        def create_signed_url(
            self, storage_key: str, *, expires_in_seconds: int, download: bool
        ) -> str:
            """不应在对象缺失时被调用。"""
            del storage_key, expires_in_seconds, download
            self.calls += 1
            return "https://storage.test/never"

    with session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-attachment",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
            )
        )
        session.add(
            MessageAttachment(
                id="attachment-1",
                message_id="message-attachment",
                media_kind="image",
                storage_key="artifacts/opaque",
                detected_mime_type="image/png",
                scan_status="clean",
            )
        )
    signer = FakeSigner()
    service = BreakGlassAccessService(
        session_factory,
        signed_url_provider=signer,
        storage_provider=FakeStorage(),  # type: ignore[arg-type]
    )

    with pytest.raises(BreakGlassAccessError, match="对象不存在"):
        service.access(
            BreakGlassAccessRequest(
                operator_subject="admin-1",
                operator_role="administrator",
                object_type="attachment",
                object_id="attachment-1",
                access_type="preview_attachment",
                reason="核对附件状态",
                request_id="request-1",
            )
        )

    assert signer.calls == 0
    with session_factory() as session:
        assert (
            session.query(BreakGlassAccessAudit)
            .filter(BreakGlassAccessAudit.request_id == "request-1")
            .count()
            == 0
        )


def test_signed_url_is_generated_only_after_grant_audit_commit(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证 signer 调用时授权审计已经提交，且 URL 不进入审计行。"""
    storage = LocalVolumeStorageProvider(tmp_path)
    stored = storage.put(b"safe", suffix=".png")
    with session_factory.begin() as session:
        session.add(
            IncomingMessage(
                message_id="message-signed",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
            )
        )
        session.add(
            MessageAttachment(
                id="attachment-signed",
                message_id="message-signed",
                media_kind="image",
                storage_key=stored.storage_key,
                scan_status="clean",
            )
        )

    class CommittedAuditSigner:
        """在签名时读取独立会话，验证 grant 已完成提交。"""

        def __init__(self) -> None:
            """初始化观察结果。"""
            self.audit_count_at_sign = 0

        def create_signed_url(
            self, storage_key: str, *, expires_in_seconds: int, download: bool
        ) -> str:
            """读取已提交的授权审计后返回测试地址。"""
            del storage_key, expires_in_seconds, download
            with session_factory() as session:
                self.audit_count_at_sign = session.query(BreakGlassAccessAudit).count()
            return "https://storage.test/signed/opaque"

    signer = CommittedAuditSigner()
    service = BreakGlassAccessService(
        session_factory,
        signed_url_provider=signer,
        storage_provider=storage,
        signed_url_ttl_seconds=120,
        signed_url_max_ttl_seconds=300,
    )
    result = service.access(
        BreakGlassAccessRequest(
            operator_subject="admin-1",
            operator_role="administrator",
            object_type="attachment",
            object_id="attachment-signed",
            access_type="preview_attachment",
            reason="验证审计顺序",
            request_id="request-signed",
        )
    )

    assert result.signed_url == "https://storage.test/signed/opaque"
    assert signer.audit_count_at_sign == 1
    with session_factory() as session:
        audits = session.scalars(select(BreakGlassAccessAudit)).all()
        assert all("storage.test" not in audit.reason for audit in audits)


def test_fake_scanner_has_explicit_non_clean_outcomes() -> None:
    """验证测试扫描器能表达 clean、infected、失败和超时，而非统一放行。"""
    assert FakeFileScanProvider("clean").scan(b"x", mime_type="image/png", timeout_seconds=1)
    assert FakeFileScanProvider("infected").scan(
        b"x", mime_type="image/png", timeout_seconds=1
    ) == "infected"
    assert FakeFileScanProvider("failed").scan(b"x", mime_type="image/png", timeout_seconds=1)
    with pytest.raises(RuntimeError, match="timeout"):
        FakeFileScanProvider("timeout").scan(b"x", mime_type="image/png", timeout_seconds=1)


def test_pending_scan_does_not_allow_ocr_or_asr(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证上传后尚未完成扫描的附件不会进入媒体识别下游。"""
    with session_factory.begin() as session:
        session.add(SalesAuthorization(wecom_user_id="sales-1", is_authorized=True))
        session.add(
            IncomingMessage(
                message_id="message-pending-scan",
                sales_user_id="sales-1",
                sequence=1,
                raw_payload={},
            )
        )
    ocr = MockOCRProvider(["不应被调用"])
    service = MediaAttachmentService(
        session_factory,
        MediaValidator(image_mime_types=("image/png",), audio_mime_types=()),
        LocalVolumeStorageProvider(tmp_path),
        FakeFileScanProvider("pending_scan"),
        ocr,
        MockASRProvider([]),
    )

    attachment_id = service.ingest(
        "message-pending-scan",
        b"\x89PNG\r\n\x1a\nimage",
        media_kind="image",
        declared_mime_type="image/png",
    )
    service.process_pending_for_message("message-pending-scan")

    with session_factory() as session:
        attachment = session.get(MessageAttachment, attachment_id)
        assert attachment is not None
        assert attachment.scan_status == "pending_scan"
        assert attachment.processing_status == "pending"
        assert attachment.recognized_text is None


def test_ingest_db_finalize_failure_leaves_recoverable_storage_fact(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    """验证对象已写入但 DB finalize 失败时不会静默遗留孤儿对象。"""

    class FirstFinalizeFails:
        """让第一次数据库事务失败，第二次事务用于保存 recovery fact。"""

        def __init__(self) -> None:
            """初始化一次性失败计数。"""
            self.calls = 0

        def begin(self):  # type: ignore[no-untyped-def]
            """第一次调用模拟 finalize 失败，后续委托真实 sessionmaker。"""
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("db_finalize_failed")
            return session_factory.begin()

    storage = LocalVolumeStorageProvider(tmp_path)
    service = MediaAttachmentService(
        FirstFinalizeFails(),  # type: ignore[arg-type]
        MediaValidator(image_mime_types=("image/png",), audio_mime_types=()),
        storage,
        FakeFileScanProvider("clean"),
        MockOCRProvider([]),
        MockASRProvider([]),
    )

    with pytest.raises(RuntimeError, match="db_finalize_failed"):
        service.ingest(
            "message-without-db-row",
            b"\x89PNG\r\n\x1a\nimage",
            media_kind="image",
            declared_mime_type="image/png",
        )

    with session_factory() as session:
        operation = session.scalar(select(StorageIngestOperation))
        assert operation is not None
        storage_key = operation.storage_key
        operation_id = operation.id
    assert storage.head(storage_key) is not None
    assert StorageIngestRecoveryService(session_factory).reconcile(operation_id, storage) == (
        "succeeded"
    )
    assert storage.head(storage_key) is None
