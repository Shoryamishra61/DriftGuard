"""Track the active project baseline set.

Revision ID: 20260809_0003
Revises: 20260809_0002
Create Date: 2026-08-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260809_0003"
down_revision: str | None = "20260809_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "projects",
        sa.Column("active_baseline_set", sa.String(length=100), nullable=True),
    )
    op.create_check_constraint(
        "chk_projects_active_baseline_set",
        "projects",
        "active_baseline_set IS NULL OR "
        "active_baseline_set ~ '^[A-Za-z0-9._-]{1,100}$'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "chk_projects_active_baseline_set",
        "projects",
        type_="check",
    )
    op.drop_column("projects", "active_baseline_set")
