"""补齐历史 T02 数据库缺失的通知重试元数据列。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0003_notification_retry_columns"
down_revision = "0002_reliable_message_outbox"
branch_labels = None
depends_on = None


def _notification_record_column_names() -> set[str]:
    """读取当前数据库 notification_records 表已有的列名。

    参数：无。
    返回值：当前表的列名集合。
    异常：表不存在或数据库连接异常时由 SQLAlchemy 抛出。
    副作用：仅读取数据库元数据。
    """
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns("notification_records")}


def upgrade() -> None:
    """以幂等方式补齐通知重试、提供商消息和发送时间列。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚。
    副作用：只向缺列的既有 notification_records 表新增模型要求的列。
    """
    # 历史环境可能已经在 0002 标记下缺列；新建环境则跳过已存在的列。
    column_names = _notification_record_column_names()
    if "attempts" not in column_names:
        op.add_column(
            "notification_records",
            sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        )
    if "provider_message_id" not in column_names:
        op.add_column(
            "notification_records",
            sa.Column("provider_message_id", sa.String(length=128), nullable=True),
        )
    if "sent_at" not in column_names:
        op.add_column(
            "notification_records",
            sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    """按存在性逆序移除本 migration 补齐的三项通知元数据列。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出。
    副作用：删除通知重试、提供商消息和发送时间数据，仅用于显式回退。
    """
    # 只删除实际存在的列，允许从部分修复状态安全执行显式回退。
    column_names = _notification_record_column_names()
    for column_name in ("sent_at", "provider_message_id", "attempts"):
        if column_name in column_names:
            op.drop_column("notification_records", column_name)
