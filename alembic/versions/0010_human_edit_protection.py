"""建立 T09 人工编辑保护与显式确认持久化结构。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0010_human_edit_protection"
down_revision = "0009_multi_lead_message_resolution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为字段来源增加 AI/人工状态，并建立机器人确认事件表。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚。
    副作用：持久化 T09 的人工编辑保护与提交前确认审计事实。
    """
    # 既有 T05 来源记录不是 AI 写入，last_ai_synced_value 保持空值以避免误判销售编辑。
    op.add_column(
        "lead_field_provenances",
        sa.Column("last_ai_synced_value", sa.String(length=512)),
    )
    op.add_column(
        "lead_field_provenances",
        sa.Column("is_user_modified", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "lead_field_provenances",
        sa.Column("is_user_confirmed", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_table(
        "user_confirmation_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("lead_id", sa.String(length=36), nullable=False),
        sa.Column("field_name", sa.String(length=64), nullable=False),
        sa.Column("confirmed_value", sa.String(length=512), nullable=False),
        sa.Column("operator_sales_user_id", sa.String(length=128), nullable=False),
        sa.Column(
            "confirmation_source", sa.String(length=32), nullable=False, server_default="robot"
        ),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
        sa.ForeignKeyConstraint(["operator_sales_user_id"], ["sales_authorizations.wecom_user_id"]),
    )
    op.create_index("ix_user_confirmation_events_lead_id", "user_confirmation_events", ["lead_id"])


def downgrade() -> None:
    """按依赖逆序删除 T09 人工保护与确认持久化结构。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出。
    副作用：显式回退时删除 T09 新增的状态与确认审计。
    """
    op.drop_index("ix_user_confirmation_events_lead_id", table_name="user_confirmation_events")
    op.drop_table("user_confirmation_events")
    op.drop_column("lead_field_provenances", "is_user_confirmed")
    op.drop_column("lead_field_provenances", "is_user_modified")
    op.drop_column("lead_field_provenances", "last_ai_synced_value")
