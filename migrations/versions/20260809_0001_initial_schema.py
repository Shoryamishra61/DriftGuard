"""Create the canonical DriftGuard relational schema.

Revision ID: 20260809_0001
Revises:
Create Date: 2026-08-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260809_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "projects",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("api_key_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "api_key_hash ~ '^[0-9a-f]{64}$'",
            name="chk_projects_api_key_hash_sha256",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_projects"),
        sa.UniqueConstraint("api_key_hash", name="uq_projects_api_key_hash"),
    )

    op.create_table(
        "telemetry_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("project_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("session_id", sa.String(length=100), nullable=False),
        sa.Column("prompt_text", sa.Text(), nullable=False),
        sa.Column("output_text", sa.Text(), nullable=False),
        sa.Column(
            "raw_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=50),
            server_default=sa.text("'queued'"),
            nullable=False,
        ),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(raw_metadata) = 'object'",
            name="chk_telemetry_runs_metadata_object",
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'processing', 'completed', 'failed')",
            name="chk_telemetry_runs_status",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_telemetry_runs_project_id_projects",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_telemetry_runs"),
    )
    op.create_index(
        "idx_telemetry_project_time",
        "telemetry_runs",
        ["project_id", sa.text("ingested_at DESC")],
        unique=False,
    )
    op.create_index(
        "idx_telemetry_queued_runs",
        "telemetry_runs",
        ["status"],
        unique=False,
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.create_index(
        "idx_telemetry_session_id",
        "telemetry_runs",
        ["session_id"],
        unique=False,
    )

    op.create_table(
        "evaluations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("drift_distance", sa.Double(), nullable=True),
        sa.Column("matched_baseline_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("evaluation_latency_ms", sa.Integer(), nullable=True),
        sa.Column(
            "is_anomaly",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "evaluated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "drift_distance IS NULL OR (drift_distance >= 0.0 AND drift_distance <= 2.0)",
            name="chk_evaluations_drift_range",
        ),
        sa.CheckConstraint(
            "evaluation_latency_ms IS NULL OR evaluation_latency_ms >= 0",
            name="chk_evaluations_latency_nonnegative",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["telemetry_runs.id"],
            name="fk_evaluations_run_id_telemetry_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_evaluations"),
        sa.UniqueConstraint("run_id", name="uq_evaluations_run_id"),
    )
    op.create_index(
        "idx_evaluations_lookup",
        "evaluations",
        ["run_id"],
        unique=False,
    )

    op.create_table(
        "alert_rules",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("project_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("rule_name", sa.String(length=100), nullable=False),
        sa.Column(
            "threshold",
            sa.Double(),
            server_default=sa.text("0.15"),
            nullable=False,
        ),
        sa.Column("action_type", sa.String(length=50), nullable=False),
        sa.Column("notification_target", sa.String(length=255), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "action_type IN ('NOTIFY', 'DIGEST', 'MUTE')",
            name="chk_alert_rules_action_type",
        ),
        sa.CheckConstraint(
            "threshold >= 0.0 AND threshold <= 2.0",
            name="chk_alert_rules_threshold_range",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_alert_rules_project_id_projects",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_alert_rules"),
    )
    op.create_index(
        "idx_alert_rules_project_active",
        "alert_rules",
        ["project_id", "is_active"],
        unique=False,
    )

    op.create_table(
        "alerts",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("evaluation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("rule_id", sa.Integer(), nullable=False),
        sa.Column(
            "alert_status",
            sa.String(length=50),
            server_default=sa.text("'TRIGGERED'"),
            nullable=False,
        ),
        sa.Column(
            "notified_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "alert_status IN ('TRIGGERED', 'RESOLVED', 'SNOOZED')",
            name="chk_alerts_status",
        ),
        sa.ForeignKeyConstraint(
            ["evaluation_id"],
            ["evaluations.id"],
            name="fk_alerts_evaluation_id_evaluations",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["rule_id"],
            ["alert_rules.id"],
            name="fk_alerts_rule_id_alert_rules",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_alerts"),
        sa.UniqueConstraint("evaluation_id", "rule_id", name="uq_alerts_evaluation_rule"),
    )
    op.create_index(
        "idx_alerts_searchable",
        "alerts",
        ["rule_id", "alert_status"],
        unique=False,
    )

    op.create_table(
        "telemetry_outbox",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=50),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column(
            "retry_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("dispatch_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name="chk_telemetry_outbox_payload_object",
        ),
        sa.CheckConstraint(
            "retry_count >= 0",
            name="chk_telemetry_outbox_retry_count_nonnegative",
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'DISPATCHED', 'FAILED')",
            name="chk_telemetry_outbox_status",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["telemetry_runs.id"],
            name="fk_telemetry_outbox_run_id_telemetry_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_telemetry_outbox"),
        sa.UniqueConstraint("run_id", name="uq_telemetry_outbox_run_id"),
    )
    op.create_index(
        "idx_outbox_pending",
        "telemetry_outbox",
        ["status", "next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status = 'PENDING'"),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_outbox_pending",
        table_name="telemetry_outbox",
        postgresql_where=sa.text("status = 'PENDING'"),
    )
    op.drop_table("telemetry_outbox")

    op.drop_index("idx_alerts_searchable", table_name="alerts")
    op.drop_table("alerts")

    op.drop_index("idx_alert_rules_project_active", table_name="alert_rules")
    op.drop_table("alert_rules")

    op.drop_index("idx_evaluations_lookup", table_name="evaluations")
    op.drop_table("evaluations")

    op.drop_index("idx_telemetry_session_id", table_name="telemetry_runs")
    op.drop_index(
        "idx_telemetry_queued_runs",
        table_name="telemetry_runs",
        postgresql_where=sa.text("status = 'queued'"),
    )
    op.drop_index("idx_telemetry_project_time", table_name="telemetry_runs")
    op.drop_table("telemetry_runs")

    op.drop_table("projects")
