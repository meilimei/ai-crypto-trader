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
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_notifications_outbox_channel_dedupe_key "
        "ON public.notifications_outbox (channel, dedupe_key)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_notifications_outbox_status_next_attempt "
        "ON public.notifications_outbox (status, next_attempt_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS public.ix_notifications_outbox_channel_dedupe_key")
