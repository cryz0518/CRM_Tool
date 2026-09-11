"""支持一条消息拆分多个线索及其可审计的人工重新归属。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0009_multi_lead_message_resolution"
down_revision = "0008_processing_lease"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """将单消息归属改为分段归属，并创建人工重归属审计表。

    参数：无。
    返回值：无。
    异常：DDL 或历史数据迁移失败时由 Alembic 抛出并回滚。
    副作用：为 Lead 和同步事实增加分段标识，保留既有归属数据为分段零。
    """
    # T07 的完整 revision 超过 Alembic 默认版本列长度，先扩大列以保持迁移链路可识别。
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=32),
        type_=sa.String(length=64),
        existing_nullable=False,
    )
    # PostgreSQL 为未显式命名的唯一约束生成以下稳定名称；先解除单消息唯一限制。
    op.drop_constraint("leads_source_message_id_key", "leads", type_="unique")
    op.add_column(
        "leads", sa.Column("source_segment_index", sa.Integer(), server_default="0", nullable=False)
    )
    # 管理员通过授权目录中的显式标记获得跨销售重归属能力，普通销售默认没有该权限。
    op.add_column(
        "sales_authorizations",
        sa.Column("is_administrator", sa.Boolean(), server_default=sa.false(), nullable=False),
    )
    op.create_unique_constraint(
        "uq_leads_source_message_segment", "leads", ["source_message_id", "source_segment_index"]
    )
    op.drop_constraint(
        "smart_table_syncs_source_message_id_key", "smart_table_syncs", type_="unique"
    )
    op.add_column(
        "smart_table_syncs",
        sa.Column("source_segment_index", sa.Integer(), server_default="0", nullable=False),
    )
    op.create_unique_constraint(
        "uq_smart_table_syncs_source_message_segment",
        "smart_table_syncs",
        ["source_message_id", "source_segment_index"],
    )

    # 原表的 message_id 是主键，须重建为可保存同一消息多个分段的归属记录。
    # PostgreSQL 重命名表时不会重命名索引，先删除旧索引才能让新表复用稳定索引名称。
    op.drop_index("ix_lead_message_resolutions_lead_id", table_name="lead_message_resolutions")
    op.rename_table("lead_message_resolutions", "lead_message_resolutions_legacy")
    op.create_table(
        "lead_message_resolutions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("message_id", sa.String(length=128), nullable=False),
        sa.Column("segment_index", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lead_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["incoming_messages.message_id"]),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
        sa.UniqueConstraint(
            "message_id", "segment_index", name="uq_lead_message_resolution_segment"
        ),
    )
    op.execute(
        "INSERT INTO lead_message_resolutions "
        "(message_id, segment_index, lead_id, status, created_at) "
        "SELECT message_id, 0, lead_id, status, created_at FROM lead_message_resolutions_legacy"
    )
    op.create_index(
        "ix_lead_message_resolutions_message_id", "lead_message_resolutions", ["message_id"]
    )
    op.create_index("ix_lead_message_resolutions_lead_id", "lead_message_resolutions", ["lead_id"])
    op.drop_table("lead_message_resolutions_legacy")
    op.create_table(
        "message_reassignment_audits",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("message_id", sa.String(length=128), nullable=False),
        sa.Column("segment_index", sa.Integer(), nullable=False),
        sa.Column("previous_lead_id", sa.String(length=36), nullable=True),
        sa.Column("new_lead_id", sa.String(length=36), nullable=False),
        sa.Column("operator_user_id", sa.String(length=128), nullable=False),
        sa.Column("operator_role", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="processing"),
        sa.Column("error_summary", sa.String(length=256), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["incoming_messages.message_id"]),
        sa.ForeignKeyConstraint(["previous_lead_id"], ["leads.id"]),
        sa.ForeignKeyConstraint(["new_lead_id"], ["leads.id"]),
        sa.ForeignKeyConstraint(["operator_user_id"], ["sales_authorizations.wecom_user_id"]),
    )


def downgrade() -> None:
    """回退多分段归属结构，并拒绝含额外分段的不可逆数据。

    参数：无。
    返回值：无。
    异常：存在非零分段时抛出 RuntimeError，避免静默丢失多客户来源事实。
    副作用：安全时恢复 T06 的单消息归属表和唯一约束。
    """
    connection = op.get_bind()
    extra_segments = connection.execute(
        sa.text("SELECT COUNT(*) FROM lead_message_resolutions WHERE segment_index <> 0")
    ).scalar_one()
    if extra_segments:
        raise RuntimeError("存在多客户分段归属，不能安全回退 T07")
    op.drop_table("message_reassignment_audits")
    op.drop_index("ix_lead_message_resolutions_lead_id", table_name="lead_message_resolutions")
    op.drop_index("ix_lead_message_resolutions_message_id", table_name="lead_message_resolutions")
    op.rename_table("lead_message_resolutions", "lead_message_resolutions_t07")
    op.create_table(
        "lead_message_resolutions",
        sa.Column("message_id", sa.String(length=128), primary_key=True),
        sa.Column("lead_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["incoming_messages.message_id"]),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
    )
    op.execute(
        "INSERT INTO lead_message_resolutions (message_id, lead_id, status, created_at) "
        "SELECT message_id, lead_id, status, created_at FROM lead_message_resolutions_t07"
    )
    op.create_index("ix_lead_message_resolutions_lead_id", "lead_message_resolutions", ["lead_id"])
    op.drop_table("lead_message_resolutions_t07")
    op.drop_constraint(
        "uq_smart_table_syncs_source_message_segment", "smart_table_syncs", type_="unique"
    )
    op.drop_column("smart_table_syncs", "source_segment_index")
    op.create_unique_constraint(
        "smart_table_syncs_source_message_id_key", "smart_table_syncs", ["source_message_id"]
    )
    op.drop_constraint("uq_leads_source_message_segment", "leads", type_="unique")
    op.drop_column("leads", "source_segment_index")
    op.drop_column("sales_authorizations", "is_administrator")
    op.create_unique_constraint("leads_source_message_id_key", "leads", ["source_message_id"])
    # 旧 revision 均不超过默认长度，回退后恢复 Alembic 的原始版本列定义。
    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=64),
        type_=sa.String(length=32),
        existing_nullable=False,
    )
