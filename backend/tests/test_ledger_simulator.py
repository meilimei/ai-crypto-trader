from decimal import Decimal
from types import SimpleNamespace

from ai_crypto_trader.services.paper_trader.ledger import simulate_positions_and_cash


def _t(symbol: str, side: str, qty: str, price: str, fee: str = "0", realized: str | None = None):
    return SimpleNamespace(
        symbol=symbol,
        side=side,
        qty=Decimal(qty),
        price=Decimal(price),
        fee=Decimal(fee),
        realized_pnl=Decimal(realized) if realized is not None else None,
    )


def test_long_add_reduce():
    trades = [
        _t("ETH/USDT", "buy", "1", "100", "1"),
        _t("ETH/USDT", "buy", "1", "110", "1"),
        _t("ETH/USDT", "sell", "0.5", "120", "0.5"),
    ]
    result = simulate_positions_and_cash(trades, Decimal("10000"))
    pos = result["positions"]["ETHUSDT"]
    assert pos["qty"] == Decimal("1.50000000")
    assert pos["avg_entry"] == Decimal("105.00000000")
    assert result["cash"]["USDT"]["free"] == Decimal("9847.50")
    assert result["stats"]["realized_pnl_usd"] == Decimal("7.5")


def test_short_add_close():
    trades = [
        _t("ETH/USDT", "sell", "1", "100", "1"),
        _t("ETH/USDT", "sell", "1", "90", "1"),
        _t("ETH/USDT", "buy", "2", "80", "2"),
    ]
    result = simulate_positions_and_cash(trades, Decimal("10000"))
    pos = result["positions"]["ETHUSDT"]
    assert pos["qty"] == Decimal("0")
    assert pos["avg_entry"] == Decimal("0")
    assert result["cash"]["USDT"]["free"] == Decimal("10026.00")
    assert result["stats"]["realized_pnl_usd"] == Decimal("30")


def test_flip_long_to_short():
    trades = [
        _t("ETH/USDT", "buy", "1", "100"),
        _t("ETH/USDT", "sell", "2", "90"),
    ]
    result = simulate_positions_and_cash(trades, Decimal("10000"))
    pos = result["positions"]["ETHUSDT"]
    assert pos["qty"] == Decimal("-1.00000000")
    assert pos["avg_entry"] == Decimal("90.00000000")
    assert result["stats"]["realized_pnl_usd"] == Decimal("-10")


def test_negative_qty_uses_sign():
    trades = [
        _t("ETH/USDT", "buy", "-1", "100"),
    ]
    result = simulate_positions_and_cash(trades, Decimal("10000"))
    pos = result["positions"]["ETHUSDT"]
    assert pos["qty"] == Decimal("-1.00000000")
    assert result["cash"]["USDT"]["free"] == Decimal("10100.00")
