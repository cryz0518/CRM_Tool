"""为 T16 管理员补建、负责人转交和 Console 管理审计增加持久化事实。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0021_ticket16_admin_maintenance"
down_revision = "0020_ticket17_crm_user_mapping"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """允许管理员补建不依赖虚假消息，并记录可恢复负责人转交 operation。"""

    # 管理员补建没有 IncomingMessage；source_message_id 必须表达真实来源缺失，而不是伪造消息。
    op.alter_column(
        "leads",
        "source_message_id",
        existing_type=sa.String(length=128),
        nullable=True,
    )
    # T16 Console retry 的 request_id 需与消息、分段、attempt 一起可追溯；历史重试允许为空。
    op.add_column("message_retry_attempts", sa.Column("request_id", sa.String(length=128)))
    op.create_index(
        "ix_message_retry_attempts_request_id", "message_retry_attempts", ["request_id"]
    )
    op.create_table(
        "admin_lead_creation_operations",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("lead_id", sa.String(length=36), nullable=False),
        sa.Column("smart_table_record_id", sa.String(length=128), nullable=True),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("operator_subject", sa.String(length=128), nullable=False),
        sa.Column("operator_role", sa.String(length=64), nullable=False),
        sa.Column("auth_source", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=False),
        sa.Column("original_capturing_sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("smart_table_owner_user_id", sa.String(length=128), nullable=False),
        sa.Column("remote_update_state", sa.String(length=32), nullable=False),
        sa.Column("final_status", sa.String(length=64), nullable=False),
        sa.Column("failure_code", sa.String(length=64), nullable=True),
        sa.Column("failure_summary", sa.String(length=256), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("lead_id"),
        sa.UniqueConstraint("request_id"),
    )
    op.create_table(
        "smart_table_owner_transfer_operations",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("lead_id", sa.String(length=36), nullable=False),
        sa.Column("smart_table_record_id", sa.String(length=128), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("operator_subject", sa.String(length=128), nullable=False),
        sa.Column("operator_role", sa.String(length=64), nullable=False),
        sa.Column("auth_source", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=False),
        sa.Column("old_owner_user_id", sa.String(length=128), nullable=False),
        sa.Column("new_owner_user_id", sa.String(length=128), nullable=False),
        sa.Column("original_capturing_sales_user_id", sa.String(length=128), nullable=False),
        sa.Column("crm_lead_owner_user_id", sa.String(length=128), nullable=True),
        sa.Column("remote_update_state", sa.String(length=32), nullable=False),
        sa.Column("permission_verification_state", sa.String(length=32), nullable=False),
        sa.Column("final_status", sa.String(length=64), nullable=False),
        sa.Column("failure_code", sa.String(length=64), nullable=True),
        sa.Column("failure_summary", sa.String(length=256), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["lead_id"], ["leads.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("request_id"),
    )
    op.create_index(
        "ix_smart_table_owner_transfer_operations_lead_id",
        "smart_table_owner_transfer_operations",
        ["lead_id"],
    )
    op.create_table(
        "console_maintenance_audits",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("operation_id", sa.String(length=36), nullable=True),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("operation_type", sa.String(length=64), nullable=False),
        sa.Column("object_type", sa.String(length=64), nullable=False),
        sa.Column("object_id", sa.String(length=128), nullable=False),
        sa.Column("operator_subject", sa.String(length=128), nullable=False),
        sa.Column("operator_role", sa.String(length=64), nullable=False),
        sa.Column("auth_source", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.String(length=512), nullable=True),
        sa.Column("before_state", sa.JSON(), nullable=False),
        sa.Column("after_state", sa.JSON(), nullable=False),
        sa.Column("result", sa.String(length=64), nullable=False),
        sa.Column("failure_code", sa.String(length=64), nullable=True),
        sa.Column("failure_summary", sa.String(length=256), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("request_id"),
    )
    op.create_index(
        "ix_console_maintenance_audits_operation_id",
        "console_maintenance_audits",
        ["operation_id"],
    )


def downgrade() -> None:
    """仅在没有 T16 operation 或审计事实时回退，避免丢失管理事实。"""

    connection = op.get_bind()
    for table_name in (
        "console_maintenance_audits",
        "smart_table_owner_transfer_operations",
        "admin_lead_creation_operations",
    ):
        if connection.execute(sa.text(f"SELECT 1 FROM {table_name} LIMIT 1")).first() is not None:
            raise RuntimeError("存在 T16 管理维护事实，禁止回退 T16 迁移")
    op.drop_index(
        "ix_console_maintenance_audits_operation_id", table_name="console_maintenance_audits"
    )
    op.drop_table("console_maintenance_audits")
    op.drop_index(
        "ix_smart_table_owner_transfer_operations_lead_id",
        table_name="smart_table_owner_transfer_operations",
    )
    op.drop_table("smart_table_owner_transfer_operations")
    op.drop_table("admin_lead_creation_operations")
    op.drop_index("ix_message_retry_attempts_request_id", table_name="message_retry_attempts")
    op.drop_column("message_retry_attempts", "request_id")
    op.alter_column(
        "leads",
        "source_message_id",
        existing_type=sa.String(length=128),
        nullable=False,
    )
