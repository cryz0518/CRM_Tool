"""为业务审计事实增加独立 Smart Table 镜像 Outbox。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0030_audit_mirror_outbox"
down_revision = "0029_crm_create_generations"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    """读取当前数据库已有表名，兼容历史环境重复执行检查。"""
    return set(sa.inspect(op.get_bind()).get_table_names())


def upgrade() -> None:
    """创建审计镜像 Outbox，不改变既有业务审计事实。"""
    # 审计镜像任务与消息 Outbox 分离，避免改变销售消息顺序和主链路状态机。
    if "audit_mirror_outbox" not in _table_names():
        op.create_table(
            "audit_mirror_outbox",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("audit_event_id", sa.Integer(), nullable=False),
            sa.Column("mirror_key", sa.String(length=128), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column("attempts", sa.Integer(), nullable=False),
            sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("claim_token", sa.String(length=64), nullable=True),
            sa.Column("failure_category", sa.String(length=32), nullable=True),
            sa.Column("failure_code", sa.String(length=64), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(
                ["audit_event_id"], ["business_audit_events.id"], name="fk_audit_mirror_event"
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("audit_event_id"),
            sa.UniqueConstraint("mirror_key"),
        )


def downgrade() -> None:
    """删除审计镜像 Outbox，保留业务审计事实表。"""
    if "audit_mirror_outbox" in _table_names():
        op.drop_table("audit_mirror_outbox")
