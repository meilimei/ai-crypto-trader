from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.models.paper_policies import PaperSymbolLimit
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol


@dataclass(frozen=True)
class SymbolLimit:
    account_id: int
    strategy_id: Optional[UUID]
    symbol: str
    max_order_qty: Optional[Decimal]
    max_position_qty: Optional[Decimal]
    max_position_notional_usdt: Optional[Decimal]
    source: str


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


async def _fetch_limit(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_id: UUID | None,
    symbol_norm: str,
) -> PaperSymbolLimit | None:
    return await session.scalar(
        select(PaperSymbolLimit)
        .where(
            PaperSymbolLimit.account_id == account_id,
            PaperSymbolLimit.strategy_id == strategy_id,
            PaperSymbolLimit.symbol == symbol_norm,
        )
        .limit(1)
    )


async def get_symbol_limit(
    session: AsyncSession,
    account_id: int,
    strategy_id: UUID | None,
    symbol: str,
) -> SymbolLimit | None:
    symbol_norm = normalize_symbol(symbol)
    if not symbol_norm:
        return None
    strategy_id_norm = _normalize_strategy_id(strategy_id)

    row = None
    source = "account"
    if strategy_id_norm is not None:
        row = await _fetch_limit(
            session,
            account_id=account_id,
            strategy_id=strategy_id_norm,
            symbol_norm=symbol_norm,
        )
        if row is not None:
            source = "strategy"
    if row is None:
        row = await _fetch_limit(
            session,
            account_id=account_id,
            strategy_id=None,
            symbol_norm=symbol_norm,
        )
        if row is None:
            return None

    return SymbolLimit(
        account_id=account_id,
        strategy_id=strategy_id_norm,
        symbol=symbol_norm,
        max_order_qty=_to_decimal(row.max_order_qty),
        max_position_qty=_to_decimal(row.max_position_qty),
        max_position_notional_usdt=_to_decimal(row.max_position_notional_usdt),
        source=source,
    )
