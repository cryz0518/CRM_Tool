"""保存可追溯的线索补充信息，供 T09 确定性备注生成使用。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0012_lead_enrichment_values"
down_revision = "0011_media_artifacts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为线索草稿增加仅后台使用的补充信息 JSON 字段。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚。
    副作用：在 leads 表新增 enrichment_values，既有记录初始化为空对象。
    """
    op.add_column(
        "leads",
        sa.Column("enrichment_values", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
    )
    op.alter_column("leads", "enrichment_values", server_default=None)
    # 冻结模板可携带较长的需求和特殊要求，来源审计不能以 512 字符截断或阻塞同步。
    op.alter_column(
        "lead_field_provenances",
        "value",
        existing_type=sa.String(length=512),
        type_=sa.Text(),
    )
    op.alter_column(
        "lead_field_provenances",
        "last_ai_synced_value",
        existing_type=sa.String(length=512),
        type_=sa.Text(),
    )


def downgrade() -> None:
    """删除仅供备注生成使用的后台补充信息字段。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚。
    副作用：显式回退时移除 leads.enrichment_values。
    """
    op.alter_column(
        "lead_field_provenances",
        "last_ai_synced_value",
        existing_type=sa.Text(),
        type_=sa.String(length=512),
    )
    op.alter_column(
        "lead_field_provenances",
        "value",
        existing_type=sa.Text(),
        type_=sa.String(length=512),
    )
    op.drop_column("leads", "enrichment_values")
