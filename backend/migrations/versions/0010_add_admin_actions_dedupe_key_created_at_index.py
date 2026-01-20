"""add admin_actions dedupe_key created_at index

Revision ID: 0010_add_admin_actions_dedupe_key_created_at_index
Revises: 0009_add_paper_policy_tables
Create Date: 2025-12-27
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0010_add_admin_actions_dedupe_key_created_at_index"
down_revision = "0009_add_paper_policy_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_admin_actions_dedupe_key_created_at",
        "admin_actions",
        ["dedupe_key", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_admin_actions_dedupe_key_created_at", table_name="admin_actions")
