from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN


def _quant(val: Decimal, exp: Decimal) -> Decimal:
    return Decimal(val).quantize(exp, rounding=ROUND_DOWN)


@dataclass(frozen=True)
class ExecutionCosts:
    market_price: Decimal
    effective_price: Decimal
    notional_usdt: Decimal
    fee_usdt: Decimal
    slippage_usdt: Decimal


def compute_execution_costs(
    *,
    side: str,
    qty: Decimal,
    market_price: Decimal,
    fee_rate: Decimal,
    slippage_bps: Decimal,
    fee_min_usdt: Decimal | None = None,
    quantize_usdt: Decimal = Decimal("0.01"),
    quantize_price: Decimal = Decimal("0.00000001"),
) -> ExecutionCosts:
    side_norm = (side or "").lower().strip()
    if side_norm not in {"buy", "sell"}:
        raise ValueError("Side must be buy or sell")

    qty_dec = Decimal(str(qty))
    if not qty_dec.is_finite() or qty_dec <= 0:
        raise ValueError("Quantity must be positive")

    price_dec = Decimal(str(market_price))
    if not price_dec.is_finite() or price_dec <= 0:
        raise ValueError("Price must be positive")

    slippage_rate = Decimal(str(slippage_bps)) / Decimal("10000")
    mult = Decimal("1") + slippage_rate
    if side_norm == "sell":
        mult = Decimal("1") - slippage_rate

    effective_price = _quant(price_dec * mult, Decimal(quantize_price))
    if not effective_price.is_finite() or effective_price <= 0:
        raise ValueError("Effective price must be positive")

    qty_abs = qty_dec.copy_abs()
    notional = _quant(qty_abs * effective_price, Decimal(quantize_usdt))
    slippage_usdt = _quant((qty_abs * (effective_price - price_dec)).copy_abs(), Decimal(quantize_usdt))

    fee_rate_dec = Decimal(str(fee_rate))
    if fee_rate_dec < 0:
        raise ValueError("Fee rate must be non-negative")
    fee_usdt = _quant(qty_abs * effective_price * fee_rate_dec, Decimal(quantize_usdt))
    min_fee = Decimal(str(fee_min_usdt)) if fee_min_usdt is not None else Decimal("0")
    if fee_usdt < min_fee:
        fee_usdt = _quant(min_fee, Decimal(quantize_usdt))

    return ExecutionCosts(
        market_price=price_dec,
        effective_price=effective_price,
        notional_usdt=notional,
        fee_usdt=fee_usdt,
        slippage_usdt=slippage_usdt,
    )
