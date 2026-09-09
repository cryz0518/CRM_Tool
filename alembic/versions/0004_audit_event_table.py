"""补齐历史 T02 数据库缺失的业务审计事件表。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0004_audit_event_table"
down_revision = "0003_notification_retry_columns"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    """读取当前数据库已存在的表名。

    参数：无。
    返回值：当前 schema 中的表名集合。
    异常：数据库连接或元数据读取失败时由 SQLAlchemy 抛出。
    副作用：仅读取数据库元数据。
    """
    return set(sa.inspect(op.get_bind()).get_table_names())


def upgrade() -> None:
    """创建 T02 原子接收链路要求的业务审计事件表。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚。
    副作用：仅当历史数据库缺表时创建审计表及其索引和唯一约束。
    """
    # 历史数据库存在版本标记但漏建该表时才修复；完整新环境不重复创建。
    if "business_audit_events" not in _table_names():
        op.create_table(
            "business_audit_events",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("message_id", sa.String(length=128), nullable=False),
            sa.Column("sales_user_id", sa.String(length=128), nullable=False),
            sa.Column("event_type", sa.String(length=64), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("message_id", "event_type"),
        )
        op.create_index(
            "ix_business_audit_events_sales_user_id",
            "business_audit_events",
            ["sales_user_id"],
            unique=False,
        )


def downgrade() -> None:
    """删除本增量迁移创建的审计表。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出。
    副作用：显式回退时删除业务审计事件及其历史数据。
    """
    # 仅在表仍存在时删除，避免已由管理员处理的环境回退时报错。
    if "business_audit_events" in _table_names():
        op.drop_table("business_audit_events")
