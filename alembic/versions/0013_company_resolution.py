"""建立 T10 公司核验、临时线索和销售内去重持久化结构。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0013_company_resolution"
down_revision = "0012_lead_enrichment_values"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为线索增加公司审计事实和销售内标准名唯一约束。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出并回滚。
    副作用：扩展 leads 表并建立销售范围的公司名称去重索引。
    """
    # enrichment_values 已由 0012 建立；NULL 标准名允许多条临时线索。
    op.add_column("leads", sa.Column("standard_company_name", sa.String(length=512)))
    op.add_column(
        "leads",
        sa.Column("company_region", sa.String(length=32), nullable=False, server_default="unknown"),
    )
    op.add_column(
        "leads",
        sa.Column(
            "company_verification_status",
            sa.String(length=64),
            nullable=False,
            server_default="incomplete_company",
        ),
    )
    op.add_column("leads", sa.Column("qcc_company_id", sa.String(length=128)))
    op.add_column(
        "leads", sa.Column("qcc_candidates", sa.JSON(), nullable=False, server_default="[]")
    )
    op.add_column(
        "leads",
        sa.Column(
            "company_confirmed_by_user", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
    )
    op.create_index("ix_leads_standard_company_name", "leads", ["standard_company_name"])
    op.create_unique_constraint(
        "uq_leads_sales_standard_company",
        "leads",
        ["smart_table_owner_user_id", "standard_company_name"],
    )


def downgrade() -> None:
    """按依赖逆序移除 T10 公司解析列和销售内唯一约束。

    参数：无。
    返回值：无。
    异常：DDL 执行失败时由 Alembic 抛出。
    副作用：删除 T10 保存的公司核验事实；保留 0012 建立的补充信息字段。
    """
    op.drop_constraint("uq_leads_sales_standard_company", "leads", type_="unique")
    op.drop_index("ix_leads_standard_company_name", table_name="leads")
    op.drop_column("leads", "company_confirmed_by_user")
    op.drop_column("leads", "qcc_candidates")
    op.drop_column("leads", "qcc_company_id")
    op.drop_column("leads", "company_verification_status")
    op.drop_column("leads", "company_region")
    op.drop_column("leads", "standard_company_name")
