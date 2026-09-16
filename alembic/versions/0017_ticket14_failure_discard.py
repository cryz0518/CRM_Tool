"""增加 T14 失败分类、受保护补充重试和逻辑废弃协调事实。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0017_ticket14_failure_discard"
down_revision = "0016_crm_update_snapshot"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为消息任务、CRM 同步、失败补充和逻辑废弃建立持久化事实。"""
    op.add_column("outbox_events", sa.Column("failure_category", sa.String(length=32)))
    op.add_column("outbox_events", sa.Column("failure_summary", sa.String(length=128)))
    op.add_column("outbox_events", sa.Column("failed_at", sa.DateTime(timezone=True)))
    op.add_column("crm_sync_records", sa.Column("failure_category", sa.String(length=32)))
    op.add_column("crm_sync_records", sa.Column("failure_summary", sa.String(length=128)))
    op.add_column("crm_sync_records", sa.Column("failed_at", sa.DateTime(timezone=True)))
    op.create_table(
        "message_retry_attempts",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "message_id",
            sa.String(length=128),
            sa.ForeignKey("incoming_messages.message_id"),
            nullable=False,
        ),
        sa.Column("lead_id", sa.String(length=36), sa.ForeignKey("leads.id")),
        sa.Column(
            "operator_user_id",
            sa.String(length=128),
            sa.ForeignKey("sales_authorizations.wecom_user_id"),
            nullable=False,
        ),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="processing"),
        sa.Column("failure_category", sa.String(length=32)),
        sa.Column("error_summary", sa.String(length=128)),
        sa.Column("updated_fields", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("protected_fields", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("message_id", "attempt_number"),
    )
    op.create_index(
        "ix_message_retry_attempts_message_id", "message_retry_attempts", ["message_id"]
    )
    op.create_index(
        "ix_message_retry_attempts_lead_id", "message_retry_attempts", ["lead_id"]
    )
    op.create_table(
        "lead_discard_requests",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "lead_id",
            sa.String(length=36),
            sa.ForeignKey("leads.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "operator_user_id",
            sa.String(length=128),
            sa.ForeignKey("sales_authorizations.wecom_user_id"),
            nullable=False,
        ),
        sa.Column("operator_role", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )


def downgrade() -> None:
    """删除 T14 新增表和失败分类列，不修改历史迁移。"""
    op.drop_table("lead_discard_requests")
    op.drop_index("ix_message_retry_attempts_lead_id", table_name="message_retry_attempts")
    op.drop_index("ix_message_retry_attempts_message_id", table_name="message_retry_attempts")
    op.drop_table("message_retry_attempts")
    op.drop_column("crm_sync_records", "failed_at")
    op.drop_column("crm_sync_records", "failure_summary")
    op.drop_column("crm_sync_records", "failure_category")
    op.drop_column("outbox_events", "failed_at")
    op.drop_column("outbox_events", "failure_summary")
    op.drop_column("outbox_events", "failure_category")
