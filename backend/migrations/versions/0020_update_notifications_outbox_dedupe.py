"""update notifications outbox dedupe key

Revision ID: 0020_update_notifications_outbox_dedupe
Revises: 0019_add_notifications_outbox
Create Date: 2026-01-28
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0020_update_notifications_outbox_dedupe"
down_revision = "0019_add_notifications_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE notifications_outbox "
        "DROP CONSTRAINT IF EXISTS uq_notifications_outbox_channel_dedupe_key"
    )
    op.execute("DROP INDEX IF EXISTS ux_notifications_outbox_channel_dedupe_key")
    op.create_index(
        "ix_notifications_outbox_channel_dedupe_key",
        "notifications_outbox",
        ["channel", "dedupe_key"],
    )
    op.create_index(
        "ix_notifications_outbox_status_next_attempt",
        "notifications_outbox",
        ["status", "next_attempt_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_notifications_outbox_status_next_attempt", table_name="notifications_outbox")
    op.drop_index("ix_notifications_outbox_channel_dedupe_key", table_name="notifications_outbox")
