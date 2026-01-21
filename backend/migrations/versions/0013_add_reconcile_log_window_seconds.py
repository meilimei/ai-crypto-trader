"""add log_window_seconds to paper_reconcile_policies

Revision ID: 0013_add_reconcile_log_window_seconds
Revises: 0012_add_paper_reconcile_policies
Create Date: 2025-12-27
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0013_add_reconcile_log_window_seconds"
down_revision = "0012_add_paper_reconcile_policies"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "paper_reconcile_policies",
        sa.Column("log_window_seconds", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("paper_reconcile_policies", "log_window_seconds")
