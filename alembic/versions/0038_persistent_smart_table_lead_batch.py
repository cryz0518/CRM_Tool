"""为智能表格线索批量写入保存可恢复的待提交字段。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0038_persistent_smart_table_lead_batch"
down_revision = "0037_crm_submission_retry_schedule"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """增加批量待写载荷列和调度索引。"""
    op.add_column("smart_table_syncs", sa.Column("batch_payload", sa.JSON(), nullable=True))
    op.create_index(
        "ix_smart_table_syncs_status_created_at",
        "smart_table_syncs",
        ["status", "created_at"],
    )


def downgrade() -> None:
    """移除批量载荷和调度索引，不改动已有智能表格记录。"""
    op.drop_index("ix_smart_table_syncs_status_created_at", table_name="smart_table_syncs")
    op.drop_column("smart_table_syncs", "batch_payload")
