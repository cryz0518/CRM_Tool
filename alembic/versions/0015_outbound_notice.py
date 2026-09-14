"""保存可靠出站通知的脱敏正文。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0015_outbound_notice"
down_revision = "0014_crm_sync"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为通知记录增加持久化的脱敏发送正文。"""
    op.add_column("notification_records", sa.Column("content", sa.String(length=512)))
    op.add_column(
        "notification_records", sa.Column("processing_started_at", sa.DateTime(timezone=True))
    )
    op.add_column(
        "notification_records", sa.Column("processing_lease_expires_at", sa.DateTime(timezone=True))
    )
    op.create_index(
        "ix_notification_records_processing_lease",
        "notification_records",
        ["processing_lease_expires_at"],
    )


def downgrade() -> None:
    """移除出站通知正文列。"""
    op.drop_index("ix_notification_records_processing_lease", table_name="notification_records")
    op.drop_column("notification_records", "processing_lease_expires_at")
    op.drop_column("notification_records", "processing_started_at")
    op.drop_column("notification_records", "content")
