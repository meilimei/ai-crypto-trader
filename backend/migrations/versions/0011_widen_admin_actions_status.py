"""widen admin_actions status length

Revision ID: 0011_widen_admin_actions_status
Revises: 0010_add_admin_actions_dedupe_key_created_at_index
Create Date: 2025-12-27
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0011_widen_admin_actions_status"
down_revision = "0010_add_admin_actions_dedupe_key_created_at_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "admin_actions",
        "status",
        existing_type=sa.String(length=20),
        type_=sa.String(length=64),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "admin_actions",
        "status",
        existing_type=sa.String(length=64),
        type_=sa.String(length=20),
        existing_nullable=False,
    )
