from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.models.paper_policies import (
    PaperPositionPolicyOverride,
    PaperRiskPolicyOverride,
)
from ai_crypto_trader.services.paper_trader.policy_store import get_or_create_default_policies


def _to_decimal(value: object | None) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _normalize_strategy_id(strategy_id: UUID | str | None) -> UUID | None:
    if strategy_id is None:
        return None
    if isinstance(strategy_id, UUID):
        return strategy_id
    try:
        return UUID(str(strategy_id))
    except Exception:
        return None


@dataclass(frozen=True)
class EffectiveRiskPolicy:
    max_drawdown_pct: Optional[Decimal]
    max_daily_loss_usdt: Optional[Decimal]
    min_equity_usdt: Optional[Decimal]
    max_order_notional_usdt: Optional[Decimal]
    source: str
    strategy_id: Optional[UUID]


@dataclass(frozen=True)
class EffectivePositionPolicy:
    min_qty: Optional[Decimal]
    min_order_notional_usdt: Optional[Decimal]
    max_position_notional_per_symbol_usdt: Optional[Decimal]
    max_total_notional_usdt: Optional[Decimal]
    source: str
    strategy_id: Optional[UUID]


async def get_effective_risk_policy(
    session: AsyncSession,
    account_id: int,
    strategy_id: UUID | None,
) -> EffectiveRiskPolicy:
    strategy_id_norm = _normalize_strategy_id(strategy_id)
    base_risk, _ = await get_or_create_default_policies(session, account_id)
    source = "account_default"

    max_drawdown_pct = _to_decimal(base_risk.max_drawdown_pct)
    max_daily_loss_usdt = _to_decimal(base_risk.max_daily_loss_usdt)
    min_equity_usdt = _to_decimal(base_risk.min_equity_usdt)
    max_order_notional_usdt = _to_decimal(getattr(base_risk, "max_order_notional_usdt", None))

    if strategy_id_norm is not None:
        override = await session.scalar(
            select(PaperRiskPolicyOverride)
            .where(
                PaperRiskPolicyOverride.account_id == account_id,
                PaperRiskPolicyOverride.strategy_id == strategy_id_norm,
            )
            .limit(1)
        )
        if override is not None:
            source = "strategy_override"
            if override.max_order_notional_usdt is not None:
                max_order_notional_usdt = _to_decimal(override.max_order_notional_usdt)

    return EffectiveRiskPolicy(
        max_drawdown_pct=max_drawdown_pct,
        max_daily_loss_usdt=max_daily_loss_usdt,
        min_equity_usdt=min_equity_usdt,
        max_order_notional_usdt=max_order_notional_usdt,
        source=source,
        strategy_id=strategy_id_norm,
    )


async def get_effective_position_policy(
    session: AsyncSession,
    account_id: int,
    strategy_id: UUID | None,
) -> EffectivePositionPolicy:
    strategy_id_norm = _normalize_strategy_id(strategy_id)
    _, base_pos = await get_or_create_default_policies(session, account_id)
    source = "account_default"

    min_qty = _to_decimal(base_pos.min_qty)
    min_order_notional_usdt = _to_decimal(base_pos.min_order_notional_usdt)
    max_position_notional_per_symbol_usdt = _to_decimal(base_pos.max_position_notional_per_symbol_usdt)
    max_total_notional_usdt = _to_decimal(base_pos.max_total_notional_usdt)

    if strategy_id_norm is not None:
        override = await session.scalar(
            select(PaperPositionPolicyOverride)
            .where(
                PaperPositionPolicyOverride.account_id == account_id,
                PaperPositionPolicyOverride.strategy_id == strategy_id_norm,
            )
            .limit(1)
        )
        if override is not None:
            source = "strategy_override"
            if override.min_qty is not None:
                min_qty = _to_decimal(override.min_qty)
            if override.min_order_notional_usdt is not None:
                min_order_notional_usdt = _to_decimal(override.min_order_notional_usdt)
            if override.max_position_notional_per_symbol_usdt is not None:
                max_position_notional_per_symbol_usdt = _to_decimal(override.max_position_notional_per_symbol_usdt)
            if override.max_total_notional_usdt is not None:
                max_total_notional_usdt = _to_decimal(override.max_total_notional_usdt)

    return EffectivePositionPolicy(
        min_qty=min_qty,
        min_order_notional_usdt=min_order_notional_usdt,
        max_position_notional_per_symbol_usdt=max_position_notional_per_symbol_usdt,
        max_total_notional_usdt=max_total_notional_usdt,
        source=source,
        strategy_id=strategy_id_norm,
    )
