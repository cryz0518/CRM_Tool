"""修复历史开发数据库中企业微信动作 outbox 的不完整字段。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0025_repair_wecom_action_outbox_schema"
down_revision = "0024_tyc_company_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """幂等补齐企业微信动作 outbox 缺失的派发与领域操作字段。

    参数：无，Alembic 通过当前数据库连接执行迁移。
    返回值：无。
    异常：目标表不存在或数据库无法修改结构时由 Alembic 向上抛出。
    重要副作用：只新增缺失的可空字段、索引和状态约束，不删除或改写已有业务数据。
    """

    # 历史开发卷可能已标记完成 0022，但实际表结构仍是旧版；先按真实列名检查再补齐。
    connection = op.get_bind()
    existing_columns = {
        column["name"] for column in sa.inspect(connection).get_columns("wecom_action_outbox")
    }
    missing_columns = (
        ("dispatch_claimed_at", sa.DateTime(timezone=True)),
        ("dispatch_lease_expires_at", sa.DateTime(timezone=True)),
        ("claim_token", sa.String(length=64)),
        ("domain_started_at", sa.DateTime(timezone=True)),
        ("domain_operation_key", sa.String(length=128)),
        ("domain_operation_payload", sa.JSON()),
        ("remote_effect_status", sa.String(length=32)),
        ("remote_effect_at", sa.DateTime(timezone=True)),
    )
    for column_name, column_type in missing_columns:
        # 所有补齐字段都允许为空，避免对历史 outbox 记录做猜测性回填。
        if column_name not in existing_columns:
            op.add_column(
                "wecom_action_outbox",
                sa.Column(column_name, column_type, nullable=True),
            )

    # 旧表可能也缺少扫描索引；索引不存在时才创建，避免重复迁移失败。
    existing_indexes = {
        index["name"] for index in sa.inspect(connection).get_indexes("wecom_action_outbox")
    }
    if "ix_wecom_action_outbox_dispatch_scan" not in existing_indexes:
        op.create_index(
            "ix_wecom_action_outbox_dispatch_scan",
            "wecom_action_outbox",
            ["status", "dispatch_lease_expires_at", "processing_lease_expires_at"],
        )

    # 旧表没有状态约束时补上与 ORM 和 0022 迁移一致的合法状态范围。
    existing_constraints = {
        constraint["name"]
        for constraint in sa.inspect(connection).get_check_constraints("wecom_action_outbox")
    }
    if "ck_wecom_action_outbox_status" not in existing_constraints:
        op.create_check_constraint(
            "ck_wecom_action_outbox_status",
            "wecom_action_outbox",
            "status IN ('pending', 'processing', 'succeeded', 'failed')",
        )


def downgrade() -> None:
    """保留补齐字段和索引，不对可能已写入的动作执行破坏性回退。

    参数：无。
    返回值：无。
    异常：无。
    重要副作用：无，避免删除历史 outbox 审计和领域操作数据。
    """

    # 该迁移用于修复不可逆的历史结构漂移，禁止自动删除新增列或索引。
    return None
