"""add versioned risk/position policies and strategy bindings

Revision ID: 0021_add_policy_v2_tables
Revises: 0020_update_notifications_outbox_dedupe
Create Date: 2026-02-03
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0021_add_policy_v2_tables"
down_revision = "0020_update_notifications_outbox_dedupe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("risk_policies", sa.Column("name", sa.Text(), nullable=True))
    op.add_column("risk_policies", sa.Column("version", sa.Integer(), nullable=True))
    op.add_column("risk_policies", sa.Column("status", sa.Text(), nullable=True, server_default=sa.text("'active'")))
    op.add_column(
        "risk_policies",
        sa.Column(
            "params",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column("risk_policies", sa.Column("notes", sa.Text(), nullable=True))

    op.execute("UPDATE risk_policies SET name = 'legacy-' || id WHERE name IS NULL")
    op.execute("UPDATE risk_policies SET version = 1 WHERE version IS NULL")
    op.execute("UPDATE risk_policies SET status = 'active' WHERE status IS NULL")
    op.execute("UPDATE risk_policies SET params = '{}'::jsonb WHERE params IS NULL")

    op.alter_column("risk_policies", "name", nullable=False)
    op.alter_column("risk_policies", "version", nullable=False)
    op.alter_column("risk_policies", "status", nullable=False, server_default=None)
    op.alter_column("risk_policies", "params", nullable=False, server_default=None)

    op.create_unique_constraint("ux_risk_policies_name_version", "risk_policies", ["name", "version"])
    op.create_index("ix_risk_policies_name", "risk_policies", ["name"])
    op.create_index("ix_risk_policies_status", "risk_policies", ["status"])

    op.create_table(
        "position_policies",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
        sa.Column(
            "params",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
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
        sa.Column("notes", sa.Text(), nullable=True),
        sa.UniqueConstraint("name", "version", name="ux_position_policies_name_version"),
    )
    op.create_index("ix_position_policies_name", "position_policies", ["name"])
    op.create_index("ix_position_policies_status", "position_policies", ["status"])

    op.create_table(
        "strategy_policy_bindings",
        sa.Column(
            "strategy_config_id",
            sa.Integer(),
            sa.ForeignKey("strategy_configs.id", ondelete="CASCADE"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "risk_policy_id",
            sa.Integer(),
            sa.ForeignKey("risk_policies.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "position_policy_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("position_policies.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("notes", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_strategy_policy_bindings_risk_policy_id",
        "strategy_policy_bindings",
        ["risk_policy_id"],
    )
    op.create_index(
        "ix_strategy_policy_bindings_position_policy_id",
        "strategy_policy_bindings",
        ["position_policy_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_strategy_policy_bindings_position_policy_id", table_name="strategy_policy_bindings")
    op.drop_index("ix_strategy_policy_bindings_risk_policy_id", table_name="strategy_policy_bindings")
    op.drop_table("strategy_policy_bindings")

    op.drop_index("ix_position_policies_status", table_name="position_policies")
    op.drop_index("ix_position_policies_name", table_name="position_policies")
    op.drop_table("position_policies")

    op.drop_index("ix_risk_policies_status", table_name="risk_policies")
    op.drop_index("ix_risk_policies_name", table_name="risk_policies")
    op.drop_constraint("ux_risk_policies_name_version", "risk_policies", type_="unique")
    op.drop_column("risk_policies", "notes")
    op.drop_column("risk_policies", "params")
    op.drop_column("risk_policies", "status")
    op.drop_column("risk_policies", "version")
    op.drop_column("risk_policies", "name")
