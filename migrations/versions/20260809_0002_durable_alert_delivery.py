"""Add crash-safe alert routing state and stale-outbox recovery support.

Revision ID: 20260809_0002
Revises: 20260809_0001
Create Date: 2026-08-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260809_0002"
down_revision: str | None = "20260809_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "alerts",
        "notified_at",
        existing_type=sa.DateTime(timezone=True),
        server_default=None,
        nullable=True,
    )
    op.add_column("alerts", sa.Column("route_status", sa.String(length=20)))
    op.add_column(
        "alerts",
        sa.Column("route_due_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "alerts",
        sa.Column("delivery_lease_until", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "alerts",
        sa.Column(
            "delivery_attempts",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column("alerts", sa.Column("last_delivery_error", sa.Text(), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE alerts AS alert
            SET route_status = CASE
                    WHEN rule.action_type = 'MUTE' THEN 'SUPPRESSED'
                    WHEN rule.action_type = 'DIGEST' THEN 'PENDING'
                    WHEN alert.alert_status = 'TRIGGERED' THEN 'DELIVERED'
                    ELSE 'PENDING'
                END,
                route_due_at = CASE
                    WHEN rule.action_type = 'DIGEST' THEN
                        (date_trunc('day', now() AT TIME ZONE 'UTC') + INTERVAL '1 day')
                        AT TIME ZONE 'UTC'
                    WHEN rule.action_type = 'NOTIFY'
                         AND alert.alert_status <> 'TRIGGERED' THEN now()
                    ELSE NULL
                END
            FROM alert_rules AS rule
            WHERE rule.id = alert.rule_id
            """
        )
    )
    op.execute(
        sa.text(
            "UPDATE alerts SET notified_at = NULL WHERE route_status <> 'DELIVERED'"
        )
    )
    op.alter_column(
        "alerts",
        "route_status",
        existing_type=sa.String(length=20),
        server_default=sa.text("'PENDING'"),
        nullable=False,
    )
    op.create_check_constraint(
        "chk_alerts_route_status",
        "alerts",
        "route_status IN ('PENDING', 'DELIVERED', 'SUPPRESSED', 'FAILED')",
    )
    op.create_check_constraint(
        "chk_alerts_delivery_attempts_nonnegative",
        "alerts",
        "delivery_attempts >= 0",
    )
    op.create_index(
        "idx_alerts_pending_route",
        "alerts",
        ["route_status", "route_due_at", "delivery_lease_until"],
        unique=False,
        postgresql_where=sa.text("route_status = 'PENDING'"),
    )
    op.create_index(
        "idx_outbox_stale_dispatched",
        "telemetry_outbox",
        ["dispatch_time"],
        unique=False,
        postgresql_where=sa.text("status = 'DISPATCHED'"),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_outbox_stale_dispatched",
        table_name="telemetry_outbox",
        postgresql_where=sa.text("status = 'DISPATCHED'"),
    )
    op.drop_index(
        "idx_alerts_pending_route",
        table_name="alerts",
        postgresql_where=sa.text("route_status = 'PENDING'"),
    )
    op.drop_constraint(
        "chk_alerts_delivery_attempts_nonnegative",
        "alerts",
        type_="check",
    )
    op.drop_constraint("chk_alerts_route_status", "alerts", type_="check")
    op.execute(sa.text("UPDATE alerts SET notified_at = COALESCE(notified_at, now())"))
    op.alter_column(
        "alerts",
        "notified_at",
        existing_type=sa.DateTime(timezone=True),
        server_default=sa.text("now()"),
        nullable=False,
    )
    op.drop_column("alerts", "last_delivery_error")
    op.drop_column("alerts", "delivery_attempts")
    op.drop_column("alerts", "delivery_lease_until")
    op.drop_column("alerts", "route_due_at")
    op.drop_column("alerts", "route_status")
