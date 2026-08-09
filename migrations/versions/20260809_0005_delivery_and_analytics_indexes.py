"""Index delivery fencing and time-window analytics paths.

Revision ID: 20260809_0005
Revises: 20260809_0004
Create Date: 2026-08-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260809_0005"
down_revision: str | None = "20260809_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_alerts_delivery_lease_token",
        "alerts",
        ["delivery_lease_token"],
        unique=False,
        postgresql_where=sa.text("delivery_lease_token IS NOT NULL"),
    )
    op.create_index(
        "idx_evaluations_evaluated_at_run",
        "evaluations",
        [sa.text("evaluated_at DESC"), "run_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_evaluations_evaluated_at_run",
        table_name="evaluations",
    )
    op.drop_index(
        "idx_alerts_delivery_lease_token",
        table_name="alerts",
    )
