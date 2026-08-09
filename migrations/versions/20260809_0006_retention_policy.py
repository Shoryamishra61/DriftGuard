"""Add legal holds and bounded retention indexes.

Revision ID: 20260809_0006
Revises: 20260809_0005
Create Date: 2026-08-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260809_0006"
down_revision: str | None = "20260809_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "legal_holds",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("project_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("starts_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ends_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason", sa.String(length=500), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "ends_at IS NULL OR ends_at >= starts_at",
            name="chk_legal_holds_time_range",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name="fk_legal_holds_project_id_projects",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_legal_holds"),
    )
    op.create_index(
        "idx_legal_holds_active_project_time",
        "legal_holds",
        ["project_id", "starts_at", "ends_at"],
        unique=False,
        postgresql_where=sa.text("released_at IS NULL"),
    )
    op.create_index(
        "idx_telemetry_retention_candidates",
        "telemetry_runs",
        ["ingested_at", "id"],
        unique=False,
        postgresql_where=sa.text("status IN ('completed', 'failed')"),
    )
    op.create_index(
        "idx_outbox_retention_candidates",
        "telemetry_outbox",
        ["dispatch_time", "id"],
        unique=False,
        postgresql_where=sa.text("status = 'DISPATCHED'"),
    )
    op.create_table(
        "retention_vector_outbox",
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "status",
            sa.String(length=20),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column(
            "attempts",
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
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('PENDING', 'COMPLETED')",
            name="chk_retention_vector_outbox_status",
        ),
        sa.CheckConstraint(
            "attempts >= 0",
            name="chk_retention_vector_outbox_attempts_nonnegative",
        ),
        sa.PrimaryKeyConstraint("run_id", name="pk_retention_vector_outbox"),
    )
    op.create_index(
        "idx_retention_vector_outbox_pending",
        "retention_vector_outbox",
        ["next_attempt_at", "run_id"],
        unique=False,
        postgresql_where=sa.text("status = 'PENDING'"),
    )


def downgrade() -> None:
    op.drop_index(
        "idx_retention_vector_outbox_pending",
        table_name="retention_vector_outbox",
        postgresql_where=sa.text("status = 'PENDING'"),
    )
    op.drop_table("retention_vector_outbox")
    op.drop_index(
        "idx_outbox_retention_candidates",
        table_name="telemetry_outbox",
        postgresql_where=sa.text("status = 'DISPATCHED'"),
    )
    op.drop_index(
        "idx_telemetry_retention_candidates",
        table_name="telemetry_runs",
        postgresql_where=sa.text("status IN ('completed', 'failed')"),
    )
    op.drop_index(
        "idx_legal_holds_active_project_time",
        table_name="legal_holds",
        postgresql_where=sa.text("released_at IS NULL"),
    )
    op.drop_table("legal_holds")
