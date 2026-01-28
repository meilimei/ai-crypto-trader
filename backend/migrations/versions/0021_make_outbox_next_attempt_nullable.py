"""make notifications_outbox next_attempt_at nullable

Revision ID: 0021_make_outbox_next_attempt_nullable
Revises: 0020_update_notifications_outbox_dedupe
Create Date: 2026-01-28
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0021_make_outbox_next_attempt_nullable"
down_revision = "0020_update_notifications_outbox_dedupe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "notifications_outbox",
        "next_attempt_at",
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "notifications_outbox",
        "next_attempt_at",
        existing_type=sa.TIMESTAMP(timezone=True),
        nullable=False,
    )
