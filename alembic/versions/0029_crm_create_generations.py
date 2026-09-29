"""为 CRM create 增加不可覆盖的 generation 链。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0029_crm_create_generations"
down_revision = "0028_remove_smart_table_company_dedup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为历史 create 回填 generation 1，并允许 abandoned 后追加新 generation。

    参数：无，Alembic 通过当前数据库连接执行 DDL。
    返回值：无。
    异常：已有表结构或索引不符合基线时由数据库抛出。
    副作用：新增 generation 链字段并替换 Lead 级 create 唯一索引，不改写历史幂等键。
    """
    op.add_column(
        "crm_sync_records",
        sa.Column("generation", sa.Integer(), nullable=True),
    )
    op.add_column(
        "crm_sync_records",
        sa.Column("supersedes_sync_record_id", sa.Integer(), nullable=True),
    )
    # 既有 create 都代表第一代；非 create 保持 NULL，避免伪造 generation 语义。
    op.execute(
        sa.text(
            "UPDATE crm_sync_records SET generation = 1 "
            "WHERE operation = 'create' AND generation IS NULL"
        )
    )
    op.create_foreign_key(
        "fk_crm_sync_records_supersedes",
        "crm_sync_records",
        "crm_sync_records",
        ["supersedes_sync_record_id"],
        ["id"],
    )
    op.create_check_constraint(
        "ck_crm_sync_records_create_generation",
        "crm_sync_records",
        "operation != 'create' OR (generation IS NOT NULL AND generation >= 1)",
    )
    op.drop_index("uq_crm_sync_records_one_create_per_lead", table_name="crm_sync_records")
    op.create_index(
        "uq_crm_sync_records_create_generation",
        "crm_sync_records",
        ["lead_id", "generation"],
        unique=True,
        postgresql_where=sa.text("operation = 'create'"),
        sqlite_where=sa.text("operation = 'create'"),
    )
    # 普通 UNIQUE 索引允许多个 NULL，正好满足 generation 1 无 predecessor 的情况。
    op.create_index(
        "uq_crm_sync_records_successor",
        "crm_sync_records",
        ["supersedes_sync_record_id"],
        unique=True,
    )


def downgrade() -> None:
    """仅在不存在 generation 大于 1 时恢复旧 create 唯一索引。

    参数：无。
    返回值：无。
    异常：存在后续 generation 时显式失败，禁止删除历史提交事实。
    副作用：删除本迁移新增结构，并恢复历史 Lead 级 create 唯一索引。
    """
    connection = op.get_bind()
    later_generations = connection.scalar(
        sa.text(
            "SELECT COUNT(*) FROM crm_sync_records "
            "WHERE operation = 'create' AND generation > 1"
        )
    )
    if later_generations:
        raise RuntimeError(
            "禁止降级：crm_sync_records 已存在 generation > 1，不能删除历史提交事实"
        )

    op.drop_index("uq_crm_sync_records_successor", table_name="crm_sync_records")
    op.drop_index("uq_crm_sync_records_create_generation", table_name="crm_sync_records")
    op.drop_constraint(
        "ck_crm_sync_records_create_generation", "crm_sync_records", type_="check"
    )
    op.drop_constraint(
        "fk_crm_sync_records_supersedes", "crm_sync_records", type_="foreignkey"
    )
    op.drop_column("crm_sync_records", "supersedes_sync_record_id")
    op.drop_column("crm_sync_records", "generation")
    op.create_index(
        "uq_crm_sync_records_one_create_per_lead",
        "crm_sync_records",
        ["lead_id"],
        unique=True,
        postgresql_where=sa.text("operation = 'create'"),
        sqlite_where=sa.text("operation = 'create'"),
    )
