"""为 CRM create 传输失败增加持久化重试时间。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0037_crm_submission_retry_schedule"
down_revision = "0036_crm_cross_owner_submission"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """增加重试到期时间，并立即排入历史待重试 create 操作。"""
    op.add_column(
        "crm_sync_records",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_crm_sync_records_next_retry_at",
        "crm_sync_records",
        ["next_retry_at"],
    )
    # 历史 retrying create 已冻结请求和幂等键，迁移后可由同一自动重试流程接管。
    op.execute(
        "UPDATE crm_sync_records SET next_retry_at = CURRENT_TIMESTAMP "
        "WHERE status = 'retrying' AND operation = 'create' "
        "AND failure_category = 'transient'"
    )


def downgrade() -> None:
    """移除重试调度列和索引，不改动已保存的 CRM 请求记录。"""
    op.drop_index("ix_crm_sync_records_next_retry_at", table_name="crm_sync_records")
    op.drop_column("crm_sync_records", "next_retry_at")
