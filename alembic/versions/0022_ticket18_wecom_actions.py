"""为 T18 企业微信确定性卡片动作和 callback 传输证据增加持久化。"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0022_ticket18_wecom_actions"
down_revision = "0021_ticket16_admin_maintenance"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建 T18 动作、执行 outbox 和 callback delivery evidence 表。

    参数：无，Alembic 通过当前连接执行迁移。
    返回值：无。
    异常：数据库结构创建失败时由 Alembic 向上抛出。
    副作用：新增动作幂等、传输证据和通知 payload 持久化结构。
    """

    # 卡片通知需要完整的 template_card body；普通文本通知继续使用 content。
    op.add_column("notification_records", sa.Column("payload", sa.JSON(), nullable=True))
    op.add_column("notification_records", sa.Column("processing_claim_token", sa.String(length=64)))
    op.add_column("user_confirmation_events", sa.Column("operation_id", sa.String(length=128)))
    op.create_unique_constraint(
        "uq_user_confirmation_events_operation_id",
        "user_confirmation_events",
        ["operation_id"],
    )
    op.add_column("lead_discard_requests", sa.Column("operation_id", sa.String(length=128)))
    op.create_unique_constraint(
        "uq_lead_discard_requests_operation_id", "lead_discard_requests", ["operation_id"]
    )
    op.add_column(
        "message_reassignment_audits", sa.Column("operation_id", sa.String(length=128))
    )
    op.create_unique_constraint(
        "uq_message_reassignment_audits_operation_id",
        "message_reassignment_audits",
        ["operation_id"],
    )
    op.create_table(
        "wecom_actions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("task_id", sa.String(length=128), nullable=False),
        sa.Column("issuance_key", sa.String(length=128), nullable=True),
        sa.Column("action_type", sa.String(length=64), nullable=False),
        sa.Column("bound_actor_wecom_user_id", sa.String(length=128), nullable=False),
        sa.Column("target_type", sa.String(length=64), nullable=False),
        sa.Column("target_id", sa.String(length=128), nullable=False),
        sa.Column("expected_action_key", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_code", sa.String(length=64), nullable=True),
        sa.Column("result_summary", sa.String(length=256), nullable=True),
        sa.Column("context", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["bound_actor_wecom_user_id"], ["sales_authorizations.wecom_user_id"]
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id"),
        sa.UniqueConstraint("issuance_key"),
    )
    op.create_index("ix_wecom_actions_bound_actor", "wecom_actions", ["bound_actor_wecom_user_id"])
    op.create_table(
        "wecom_action_outbox",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("action_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("dispatch_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatch_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claim_token", sa.String(length=64), nullable=True),
        sa.Column("domain_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("domain_operation_key", sa.String(length=128), nullable=True),
        sa.Column("domain_operation_payload", sa.JSON(), nullable=True),
        sa.Column("remote_effect_status", sa.String(length=32), nullable=True),
        sa.Column("remote_effect_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["action_id"], ["wecom_actions.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("action_id"),
        sa.CheckConstraint(
            "status IN ('pending', 'processing', 'succeeded', 'failed')",
            name="ck_wecom_action_outbox_status",
        ),
    )
    op.create_index(
        "ix_wecom_action_outbox_dispatch_scan",
        "wecom_action_outbox",
        ["status", "dispatch_lease_expires_at", "processing_lease_expires_at"],
    )
    op.create_table(
        "wecom_callback_deliveries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("action_id", sa.String(length=36), nullable=True),
        sa.Column("provider_msgid", sa.String(length=128), nullable=False),
        sa.Column("req_id", sa.String(length=128), nullable=False),
        sa.Column("actor_user_id", sa.String(length=128), nullable=False),
        sa.Column("event_key", sa.String(length=128), nullable=False),
        sa.Column("task_id", sa.String(length=128), nullable=False),
        sa.Column("processing_status", sa.String(length=32), nullable=False),
        sa.Column("result_code", sa.String(length=64), nullable=True),
        sa.Column("transport_stage", sa.String(length=64), nullable=True),
        sa.Column("transport_status", sa.String(length=32), nullable=True),
        sa.Column("transport_failure_code", sa.String(length=64), nullable=True),
        sa.Column("transport_failure_summary", sa.String(length=128), nullable=True),
        sa.Column("transport_failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["action_id"], ["wecom_actions.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("provider_msgid"),
        sa.CheckConstraint(
            "processing_status IN ('received', 'claimed', 'duplicated', 'rejected', 'completed')",
            name="ck_wecom_callback_delivery_status",
        ),
    )
    op.create_check_constraint(
        "ck_wecom_actions_status",
        "wecom_actions",
        "status IN ('pending', 'processing', 'succeeded', 'denied', 'expired', 'failed', "
        "'pending_recovery')",
    )
    op.create_index(
        "ix_wecom_callback_deliveries_action_id",
        "wecom_callback_deliveries",
        ["action_id"],
    )
    op.create_index(
        "ix_wecom_callback_deliveries_task_id",
        "wecom_callback_deliveries",
        ["task_id"],
    )


def downgrade() -> None:
    """仅在没有 T18 action/callback 不可变事实时回退，避免静默丢失审计。

    参数：无，Alembic 通过当前连接检查迁移事实。
    返回值：无。
    异常：存在任一 T18 事实时抛出 RuntimeError，拒绝破坏性回退。
    副作用：无事实时删除 T18 表、索引及通知 payload 列。
    """

    connection = op.get_bind()
    for table_name in (
        "notification_records",
        "wecom_callback_deliveries",
        "wecom_action_outbox",
        "wecom_actions",
    ):
        if connection.execute(sa.text(f"SELECT 1 FROM {table_name} LIMIT 1")).first() is not None:
            if table_name == "notification_records":
                payload_exists = connection.execute(
                    sa.text("SELECT 1 FROM notification_records WHERE payload IS NOT NULL LIMIT 1")
                ).first()
                if payload_exists is not None:
                    raise RuntimeError("存在 T18 notification payload 事实，禁止回退 T18 迁移")
                continue
            raise RuntimeError("存在 T18 企业微信动作或 callback 事实，禁止回退 T18 迁移")

    for table_name, column_name in (
        ("user_confirmation_events", "operation_id"),
        ("lead_discard_requests", "operation_id"),
        ("message_reassignment_audits", "operation_id"),
    ):
        if connection.execute(
            sa.text(f"SELECT 1 FROM {table_name} WHERE {column_name} IS NOT NULL LIMIT 1")
        ).first() is not None:
            raise RuntimeError("存在 T18 domain operation 事实，禁止回退 T18 迁移")

    op.drop_index("ix_wecom_callback_deliveries_task_id", table_name="wecom_callback_deliveries")
    op.drop_index("ix_wecom_callback_deliveries_action_id", table_name="wecom_callback_deliveries")
    op.drop_table("wecom_callback_deliveries")
    op.drop_index("ix_wecom_action_outbox_dispatch_scan", table_name="wecom_action_outbox")
    op.drop_table("wecom_action_outbox")
    op.drop_index("ix_wecom_actions_bound_actor", table_name="wecom_actions")
    op.drop_table("wecom_actions")
    op.drop_constraint(
        "uq_message_reassignment_audits_operation_id",
        "message_reassignment_audits",
        type_="unique",
    )
    op.drop_column("message_reassignment_audits", "operation_id")
    op.drop_constraint(
        "uq_lead_discard_requests_operation_id", "lead_discard_requests", type_="unique"
    )
    op.drop_column("lead_discard_requests", "operation_id")
    op.drop_constraint(
        "uq_user_confirmation_events_operation_id",
        "user_confirmation_events",
        type_="unique",
    )
    op.drop_column("user_confirmation_events", "operation_id")
    op.drop_column("notification_records", "processing_claim_token")
    op.drop_column("notification_records", "payload")
