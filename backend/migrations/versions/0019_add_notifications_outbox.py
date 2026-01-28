"""add notifications outbox table

Revision ID: 0019_add_notifications_outbox
Revises: 0018_add_equity_risk_policy_fields
Create Date: 2026-01-28
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0019_add_notifications_outbox"
down_revision = "0018_add_equity_risk_policy_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notifications_outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Text(),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "channel",
            sa.Text(),
            server_default=sa.text("'noop'"),
            nullable=False,
        ),
        sa.Column(
            "admin_action_id",
            sa.BigInteger(),
            sa.ForeignKey("admin_actions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("dedupe_key", sa.Text(), nullable=True),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "next_attempt_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.UniqueConstraint("admin_action_id", name="uq_notifications_outbox_admin_action_id"),
    )
    op.create_index(
        "ix_notifications_outbox_status_next_attempt",
        "notifications_outbox",
        ["status", "next_attempt_at"],
    )
    op.create_index(
        "ix_notifications_outbox_dedupe_key",
        "notifications_outbox",
        ["dedupe_key"],
    )


def downgrade() -> None:
    op.drop_index("ix_notifications_outbox_dedupe_key", table_name="notifications_outbox")
    op.drop_index("ix_notifications_outbox_status_next_attempt", table_name="notifications_outbox")
    op.drop_table("notifications_outbox")
