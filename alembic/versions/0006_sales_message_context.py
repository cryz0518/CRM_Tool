"""建立同销售消息归属与当前客户上下文持久化结构。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0006_sales_message_context"
down_revision = "0005_first_text_lead_workspace"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建销售当前线索上下文和消息归属结论表。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚事务。
    副作用：新增 T06 的上下文 TTL 和待归属消息持久化结构。
    """
    # 上下文按销售唯一保存，避免不同销售共用最近客户而发生串线。
    op.create_table(
        "sales_lead_contexts",
        sa.Column("sales_user_id", sa.String(length=128), primary_key=True),
        sa.Column("lead_id", sa.String(length=36), nullable=False),
        sa.Column("last_message_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["sales_user_id"], ["sales_authorizations.wecom_user_id"]),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
    )
    op.create_index("ix_sales_lead_contexts_lead_id", "sales_lead_contexts", ["lead_id"])
    # 每条消息只保留一个当前归属结论；待归属消息的 lead_id 必须为空。
    op.create_table(
        "lead_message_resolutions",
        sa.Column("message_id", sa.String(length=128), primary_key=True),
        sa.Column("lead_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["incoming_messages.message_id"]),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
    )
    op.create_index("ix_lead_message_resolutions_lead_id", "lead_message_resolutions", ["lead_id"])


def downgrade() -> None:
    """按依赖逆序删除 T06 的上下文和消息归属持久化结构。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出。
    副作用：显式回退时删除上下文和归属结论历史数据。
    """
    op.drop_index("ix_lead_message_resolutions_lead_id", table_name="lead_message_resolutions")
    op.drop_table("lead_message_resolutions")
    op.drop_index("ix_sales_lead_contexts_lead_id", table_name="sales_lead_contexts")
    op.drop_table("sales_lead_contexts")
