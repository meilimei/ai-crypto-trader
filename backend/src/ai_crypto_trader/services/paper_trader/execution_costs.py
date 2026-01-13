from decimal import Decimal, ROUND_DOWN

from ai_crypto_trader.services.paper_trader.accounting import PRICE_EXP, MONEY_EXP


def _quant(val: Decimal, exp: str) -> Decimal:
    return Decimal(val).quantize(Decimal(exp), rounding=ROUND_DOWN)


def apply_slippage(price: Decimal, side: str, slippage_bps: Decimal) -> Decimal:
    side_norm = (side or "").lower().strip()
    if side_norm not in {"buy", "sell"}:
        raise ValueError("Side must be buy or sell")
    mult = Decimal("1") + (Decimal(str(slippage_bps)) / Decimal("10000"))
    if side_norm == "sell":
        mult = Decimal("1") - (Decimal(str(slippage_bps)) / Decimal("10000"))
    exec_price = _quant(Decimal(str(price)) * mult, PRICE_EXP)
    if not exec_price.is_finite() or exec_price <= 0:
        raise ValueError("Price must be positive")
    return exec_price


def calc_fee(notional: Decimal, fee_bps: Decimal) -> Decimal:
    fee = _quant(Decimal(str(notional)) * (Decimal(str(fee_bps)) / Decimal("10000")), MONEY_EXP)
    if not fee.is_finite() or fee < 0:
        raise ValueError("Invalid fee")
    return fee


def compute_execution(
    market_price: Decimal,
    side: str,
    qty: Decimal,
    fee_bps: Decimal,
    slippage_bps: Decimal,
) -> dict:
    exec_price = apply_slippage(market_price, side, Decimal(str(slippage_bps)))
    notional = _quant(Decimal(str(qty)) * exec_price, MONEY_EXP)
    fee = calc_fee(notional, Decimal(str(fee_bps)))
    return {
        "exec_price": exec_price,
        "notional": notional,
        "fee": fee,
    }
