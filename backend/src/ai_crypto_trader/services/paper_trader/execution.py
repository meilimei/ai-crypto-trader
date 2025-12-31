from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ai_crypto_trader.common.models import PaperAccount, PaperBalance, PaperOrder, PaperPosition, PaperTrade
from ai_crypto_trader.services.paper_trader.accounting import apply_fill_to_state, normalize_symbol, QTY_EXP, MONEY_EXP, PRICE_EXP


@dataclass
class ExecutionResult:
    order: PaperOrder
    trade: PaperTrade
    position: PaperPosition
    balance: PaperBalance
    costs: Dict[str, str]


def _quantize(val: Decimal, exp: str) -> Decimal:
    return Decimal(val).quantize(Decimal(exp), rounding=ROUND_DOWN)


async def _get_balance(session: AsyncSession, account: PaperAccount) -> PaperBalance:
    balance = await session.scalar(
        select(PaperBalance)
        .where(PaperBalance.account_id == account.id, PaperBalance.ccy == account.base_ccy)
        .with_for_update()
    )
    if balance:
        return balance
    balance = PaperBalance(account_id=account.id, ccy=account.base_ccy, available=Decimal("0"))
    session.add(balance)
    await session.flush()
    return balance


async def _get_position(session: AsyncSession, account_id: int, symbol: str) -> PaperPosition:
    symbol_norm = normalize_symbol(symbol)
    position = await session.scalar(
        select(PaperPosition)
        .where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol_norm)
        .with_for_update()
    )
    if not position and symbol_norm != symbol:
        position = await session.scalar(
            select(PaperPosition)
            .where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol)
            .with_for_update()
        )
        if position:
            position.symbol = symbol_norm
    if position:
        return position
    position = PaperPosition(account_id=account_id, symbol=symbol_norm, side="long", qty=Decimal("0"), avg_entry_price=Decimal("0"))
    session.add(position)
    await session.flush()
    return position


async def execute_market_order_with_costs(
    session: AsyncSession,
    account_id: int,
    symbol: str,
    side: str,
    qty: Decimal,
    mid_price: Decimal,
    fee_bps: Decimal,
    slippage_bps: Decimal,
    meta: Optional[Dict[str, str]] = None,
) -> ExecutionResult:
    if qty <= 0:
        raise ValueError("Quantity must be positive")

    side_norm = side.lower().strip()
    symbol_norm = normalize_symbol(symbol)
    slip_mult = Decimal("1") + (slippage_bps / Decimal("10000"))
    if side_norm == "sell":
        slip_mult = Decimal("1") - (slippage_bps / Decimal("10000"))

    fill_price = _quantize(mid_price * slip_mult, PRICE_EXP)
    notional = _quantize(qty * fill_price, MONEY_EXP)
    fee_usd = _quantize(notional * (fee_bps / Decimal("10000")), MONEY_EXP)

    # Use a savepoint if we're already inside a transaction (e.g., request-scoped session).
    if session.in_transaction():
        ctx = session.begin_nested()
    else:
        ctx = session.begin()

    async with ctx:
        account = await session.get(PaperAccount, account_id, with_for_update=True)
        if not account:
            raise ValueError("Paper account not found")

        balance = await _get_balance(session, account)
        position = await _get_position(session, account_id, symbol_norm)

        # Apply accounting helper for consistent state updates
        state = {
            "positions_by_symbol": {
                symbol_norm: {"qty": Decimal(str(position.qty or 0)), "avg": Decimal(str(position.avg_entry_price or 0))}
            },
            "balances_by_asset": {balance.ccy: Decimal(str(balance.available))},
            "fees_usd": Decimal("0"),
        }
        updated = apply_fill_to_state(state, symbol_norm, side_norm, qty, fill_price, fee_usd)
        pos_state = updated["positions_by_symbol"].get(symbol_norm, {"qty": Decimal("0"), "avg": Decimal("0")})
        balance_state = updated["balances_by_asset"].get(balance.ccy, balance.available)
        position.qty = pos_state["qty"]
        position.avg_entry_price = pos_state["avg"]
        position.side = "long"
        balance.available = balance_state

        position.updated_at = position.updated_at  # no-op to appease linters

        order = PaperOrder(
            account_id=account_id,
            symbol=symbol_norm,
            side=side_norm,
            type="market",
            status="filled",
            requested_qty=qty,
            filled_qty=qty,
            avg_fill_price=fill_price,
            fee_paid=fee_usd,
        )
        trade = PaperTrade(
            account_id=account_id,
            symbol=symbol_norm,
            side=side_norm,
            qty=qty,
            price=fill_price,
            fee=fee_usd,
            realized_pnl=None,
        )
        session.add_all([order, trade])
        await session.flush()

    return ExecutionResult(
        order=order,
        trade=trade,
        position=position,
        balance=balance,
        costs={
            "fill_price": str(fill_price),
            "notional_usd": str(notional),
            "fee_usd": str(fee_usd),
            "slippage_bps": str(slippage_bps),
            "fee_bps": str(fee_bps),
        },
    )


async def apply_market_order(
    session: AsyncSession,
    account_id: int,
    symbol: str,
    side: str,
    qty: Optional[Decimal] = None,
    price: Optional[Decimal] = None,
    mid_price: Optional[Decimal] = None,
    fee_bps: Decimal = Decimal("0"),
    slippage_bps: Decimal = Decimal("0"),
    meta: Optional[Dict[str, str]] = None,
    **kwargs,
) -> ExecutionResult:
    """
    Backward-compatible wrapper that calls execute_market_order_with_costs.

    Supports legacy shape with qty+price or new shape with mid_price and optional notional_usd.
    """
    side_norm = (side or "").lower().strip()
    mid = mid_price if mid_price is not None else price
    if mid is None:
        raise ValueError("price or mid_price is required")

    qty_dec: Optional[Decimal] = Decimal(str(qty)) if qty is not None else None
    if qty_dec is None:
        notional = kwargs.get("notional_usd")
        if notional is None:
            raise ValueError("qty or notional_usd must be provided")
        qty_dec = _quantize(Decimal(str(notional)) / Decimal(str(mid)), "0.00000001")

    return await execute_market_order_with_costs(
        session=session,
        account_id=account_id,
        symbol=symbol,
        side=side_norm,
        qty=qty_dec,
        mid_price=Decimal(str(mid)),
        fee_bps=Decimal(str(fee_bps)),
        slippage_bps=Decimal(str(slippage_bps)),
        meta=meta,
    )
