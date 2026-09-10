"""记录 Outbox processing 租约起始时间。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0008_processing_lease"
down_revision = "0007_message_processing_attempts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为 Outbox 增加 processing 租约时间列。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚事务。
    副作用：后续 Worker 可识别失联 processing 任务并让其成为失败检查点。
    """
    op.add_column("outbox_events", sa.Column("processing_started_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    """删除 processing 租约时间列。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出。
    副作用：显式回退时删除 processing 租约时间。
    """
    op.drop_column("outbox_events", "processing_started_at")
