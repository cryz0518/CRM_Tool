"""为 T13 CRM update 快照幂等增加数据库并发兜底。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0016_crm_update_snapshot"
down_revision = "0015_outbound_notice"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """确保同一线索的同一 update 规范快照只能形成一个逻辑操作。"""
    op.create_index(
        "uq_crm_sync_records_update_snapshot",
        "crm_sync_records",
        ["lead_id", "snapshot_hash"],
        unique=True,
        postgresql_where=sa.text("operation = 'update'"),
    )


def downgrade() -> None:
    """删除 T13 update 快照并发约束。"""
    op.drop_index("uq_crm_sync_records_update_snapshot", table_name="crm_sync_records")
