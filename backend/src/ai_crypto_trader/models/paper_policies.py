from __future__ import annotations

from datetime import datetime
import uuid
from decimal import Decimal

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, Numeric, TIMESTAMP, Text, Uuid, text
from sqlalchemy.orm import Mapped, mapped_column

from ai_crypto_trader.common.models import Base, utc_now


class PaperRiskPolicy(Base):
    __tablename__ = "paper_risk_policies"
    __table_args__ = (Index("ix_paper_risk_policies_account_id", "account_id", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False)
    max_drawdown_pct: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True)
    max_daily_loss_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    min_equity_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    max_order_notional_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class PaperPositionPolicy(Base):
    __tablename__ = "paper_position_policies"
    __table_args__ = (Index("ix_paper_position_policies_account_id", "account_id", unique=True),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False)
    min_qty: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    min_order_notional_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    max_position_notional_per_symbol_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    max_total_notional_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class PaperReconcilePolicy(Base):
    __tablename__ = "paper_reconcile_policies"
    __table_args__ = (Index("ix_paper_reconcile_policies_account_id", "account_id", unique=True),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False)
    mode: Mapped[str] = mapped_column(
        Text,
        server_default=text("'strict'"),
        default="strict",
        nullable=False,
    )
    baseline_cash_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    ok_usdt_diff: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    alert_usdt_diff: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    log_window_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class PaperRiskPolicyOverride(Base):
    __tablename__ = "paper_risk_policy_overrides"
    __table_args__ = (Index("ux_paper_risk_policy_overrides_account_strategy", "account_id", "strategy_id", unique=True),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False)
    strategy_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    max_order_notional_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class PaperPositionPolicyOverride(Base):
    __tablename__ = "paper_position_policy_overrides"
    __table_args__ = (
        Index("ux_paper_position_policy_overrides_account_strategy", "account_id", "strategy_id", unique=True),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False)
    strategy_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    min_qty: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    min_order_notional_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    max_position_notional_per_symbol_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    max_total_notional_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class PaperSymbolLimit(Base):
    __tablename__ = "paper_symbol_limits"
    __table_args__ = (
        Index("ux_paper_symbol_limits_account_strategy_symbol", "account_id", "strategy_id", "symbol", unique=True),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("paper_accounts.id", ondelete="CASCADE"), nullable=False)
    strategy_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    max_order_qty: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    max_position_qty: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    max_position_notional_usdt: Mapped[Decimal | None] = mapped_column(Numeric(24, 10), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )
