"""标记新线索系统默认来源，不回填历史业务字段。"""

import sqlalchemy as sa

from alembic import op

revision = "0034_lead_system_defaults"
down_revision = "0033_lead_progress_sessions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """增加默认来源标记并允许受审计管理员补建无消息来源；DDL 失败回滚。"""
    op.add_column(
        "lead_field_provenances",
        sa.Column(
            "is_system_default",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.alter_column(
        "lead_field_provenances", "source_message_id", existing_type=sa.String(128), nullable=True
    )


def downgrade() -> None:
    """恢复历史约束并删除标记；存在无消息来源行时由数据库拒绝回滚，避免丢失审计。"""
    op.alter_column(
        "lead_field_provenances", "source_message_id", existing_type=sa.String(128), nullable=False
    )
    op.drop_column("lead_field_provenances", "is_system_default")
