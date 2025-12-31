"""ensure admin_actions table exists

Revision ID: 0004_ensure_admin_actions_table
Revises: 0003_add_admin_actions
Create Date: 2025-12-23
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0004_ensure_admin_actions_table"
down_revision = "0003_add_admin_actions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if not insp.has_table("admin_actions"):
        op.create_table(
            "admin_actions",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("action", sa.String(length=64), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column("message", sa.Text(), nullable=True),
            sa.Column("meta", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column(
                "created_at",
                sa.TIMESTAMP(timezone=True),
                server_default=sa.text("CURRENT_TIMESTAMP"),
                nullable=False,
            ),
        )
        op.create_index("ix_admin_actions_created_at", "admin_actions", ["created_at"])
    else:
        # Ensure index exists even if table already created by a prior migration.
        existing_indexes = {ix["name"] for ix in insp.get_indexes("admin_actions")}
        if "ix_admin_actions_created_at" not in existing_indexes:
            op.create_index("ix_admin_actions_created_at", "admin_actions", ["created_at"])


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)
    if insp.has_table("admin_actions"):
        existing_indexes = {ix["name"] for ix in insp.get_indexes("admin_actions")}
        if "ix_admin_actions_created_at" in existing_indexes:
            op.drop_index("ix_admin_actions_created_at", table_name="admin_actions")
        op.drop_table("admin_actions")
