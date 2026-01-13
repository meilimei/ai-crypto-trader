"""add dedupe_key to admin_actions

Revision ID: 0008_add_admin_actions_dedupe_key
Revises: 0007_set_default_initial_cash_usd
Create Date: 2025-12-26
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0008_add_admin_actions_dedupe_key"
down_revision = "0007_set_default_initial_cash_usd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("admin_actions", sa.Column("dedupe_key", sa.String(length=255), nullable=True))
    op.create_index("ix_admin_actions_dedupe_key", "admin_actions", ["dedupe_key"])


def downgrade() -> None:
    op.drop_index("ix_admin_actions_dedupe_key", table_name="admin_actions")
    op.drop_column("admin_actions", "dedupe_key")
