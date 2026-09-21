"""为 T22 增加媒体安全状态、清理恢复 operation 和保留策略元数据。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0023_ticket22_storage_retention"
down_revision = "0022_ticket18_wecom_actions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建 T22 production-safe storage、scan、retention 和 recovery 结构。"""
    # 附件行保留为 tombstone，避免普通媒体清理误删业务来源和审计关联。
    for column in (
        sa.Column("storage_provider", sa.String(length=32)),
        sa.Column("storage_encryption_mode", sa.String(length=64)),
        sa.Column("storage_etag", sa.String(length=256)),
        sa.Column("scan_started_at", sa.DateTime(timezone=True)),
        sa.Column("scan_completed_at", sa.DateTime(timezone=True)),
        sa.Column("quarantined_at", sa.DateTime(timezone=True)),
        sa.Column("retention_expires_at", sa.DateTime(timezone=True)),
        sa.Column("retention_policy_version", sa.String(length=64)),
        sa.Column("deletion_status", sa.String(length=32), nullable=False, server_default="active"),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.Column("deletion_reason", sa.String(length=128)),
        sa.Column("cleanup_operation_id", sa.String(length=36)),
    ):
        op.add_column("message_attachments", column)
    op.create_index(
        "ix_message_attachments_retention_expires_at",
        "message_attachments",
        ["retention_expires_at"],
    )
    op.create_index(
        "ix_message_attachments_cleanup_operation_id",
        "message_attachments",
        ["cleanup_operation_id"],
    )
    op.create_check_constraint(
        "ck_message_attachments_scan_status",
        "message_attachments",
        "scan_status IN ('pending', 'pending_scan', 'scanning', 'clean', 'infected', "
        "'scan_failed', 'quarantined', 'failed', 'not_required')",
    )
    op.create_check_constraint(
        "ck_message_attachments_deletion_status",
        "message_attachments",
        "deletion_status IN ('active', 'quarantined', 'deleting', 'deleted')",
    )
    op.add_column(
        "break_glass_access_audits",
        sa.Column("signed_url_ttl_seconds", sa.Integer(), nullable=True),
    )
    op.add_column(
        "break_glass_access_audits",
        sa.Column("signed_url_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        "storage_ingest_operations",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("operation_key", sa.String(length=128), nullable=False),
        sa.Column("attachment_id", sa.String(length=36), nullable=False),
        sa.Column("message_id", sa.String(length=128), nullable=False),
        sa.Column("storage_provider", sa.String(length=32), nullable=False),
        sa.Column("storage_key", sa.String(length=256)),
        sa.Column("storage_key_digest", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("failure_summary", sa.String(length=128)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("operation_key"),
        sa.CheckConstraint(
            "status IN ('reconcile_required', 'succeeded', 'failed_pending_review')",
            name="ck_storage_ingest_operation_status",
        ),
    )
    op.create_index(
        "ix_storage_ingest_operations_attachment_id",
        "storage_ingest_operations",
        ["attachment_id"],
    )
    op.create_index(
        "ix_storage_ingest_operations_message_id",
        "storage_ingest_operations",
        ["message_id"],
    )
    op.create_table(
        "storage_cleanup_operations",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("operation_key", sa.String(length=192), nullable=False),
        sa.Column("data_class", sa.String(length=64), nullable=False),
        sa.Column("target_type", sa.String(length=64), nullable=False),
        sa.Column("target_id", sa.String(length=128), nullable=False),
        sa.Column("storage_provider", sa.String(length=32)),
        sa.Column("storage_key", sa.String(length=256)),
        sa.Column("storage_key_digest", sa.String(length=64)),
        sa.Column("policy_version", sa.String(length=64), nullable=False),
        sa.Column("retention_cutoff", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("claim_token", sa.String(length=64)),
        sa.Column("generation", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processing_started_at", sa.DateTime(timezone=True)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("remote_outcome", sa.String(length=32)),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failure_kind", sa.String(length=64)),
        sa.Column("failure_summary", sa.String(length=256)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("operation_key"),
        sa.CheckConstraint(
            "status IN ('pending', 'processing', 'retrying', 'reconcile_required', "
            "'succeeded', 'failed_pending_review')",
            name="ck_storage_cleanup_operation_status",
        ),
        sa.CheckConstraint(
            "remote_outcome IS NULL OR remote_outcome IN "
            "('confirmed_deleted', 'not_found', 'unknown')",
            name="ck_storage_cleanup_remote_outcome",
        ),
    )
    op.create_index(
        "ix_storage_cleanup_operations_target_id",
        "storage_cleanup_operations",
        ["target_id"],
    )


def downgrade() -> None:
    """存在 T22 recovery/tombstone/签名审计事实时拒绝破坏性回退。"""
    connection = op.get_bind()
    for table_name in ("storage_cleanup_operations", "storage_ingest_operations"):
        if connection.execute(sa.text(f"SELECT 1 FROM {table_name} LIMIT 1")).first() is not None:
            raise RuntimeError("存在 T22 storage recovery 事实，禁止回退迁移")
    if connection.execute(
        sa.text(
            "SELECT 1 FROM message_attachments "
            "WHERE deletion_status <> 'active' OR cleanup_operation_id IS NOT NULL LIMIT 1"
        )
    ).first() is not None:
        raise RuntimeError("存在 T22 media tombstone 事实，禁止回退迁移")
    if connection.execute(
        sa.text(
            "SELECT 1 FROM break_glass_access_audits "
            "WHERE signed_url_ttl_seconds IS NOT NULL LIMIT 1"
        )
    ).first() is not None:
        raise RuntimeError("存在 T22 signed URL 审计事实，禁止回退迁移")

    op.drop_index(
        "ix_storage_cleanup_operations_target_id", table_name="storage_cleanup_operations"
    )
    op.drop_table("storage_cleanup_operations")
    op.drop_index(
        "ix_storage_ingest_operations_message_id", table_name="storage_ingest_operations"
    )
    op.drop_index(
        "ix_storage_ingest_operations_attachment_id", table_name="storage_ingest_operations"
    )
    op.drop_table("storage_ingest_operations")
    op.drop_constraint(
        "ck_message_attachments_deletion_status", "message_attachments", type_="check"
    )
    op.drop_constraint(
        "ck_message_attachments_scan_status", "message_attachments", type_="check"
    )
    op.drop_column("break_glass_access_audits", "signed_url_expires_at")
    op.drop_column("break_glass_access_audits", "signed_url_ttl_seconds")
    op.drop_index(
        "ix_message_attachments_cleanup_operation_id", table_name="message_attachments"
    )
    op.drop_index(
        "ix_message_attachments_retention_expires_at", table_name="message_attachments"
    )
    for column_name in (
        "cleanup_operation_id",
        "deletion_reason",
        "deleted_at",
        "deletion_status",
        "retention_policy_version",
        "retention_expires_at",
        "quarantined_at",
        "scan_completed_at",
        "scan_started_at",
        "storage_etag",
        "storage_encryption_mode",
        "storage_provider",
    ):
        op.drop_column("message_attachments", column_name)
