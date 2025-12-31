"""add admin actions table

Revision ID: 0003_add_admin_actions
Revises: 0002_fix_paper_tables_id_identity
Create Date: 2025-12-22
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0003_add_admin_actions"
down_revision = "0002_fix_paper_tables_id_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "admin_actions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("action", sa.String(length=50), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("meta", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )
    op.create_index("ix_admin_actions_created_at", "admin_actions", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_admin_actions_created_at", table_name="admin_actions")
    op.drop_table("admin_actions")
