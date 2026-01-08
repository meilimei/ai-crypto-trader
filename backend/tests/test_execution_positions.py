from decimal import Decimal

from ai_crypto_trader.services.paper_trader.execution import _apply_trade_to_position


def test_apply_trade_to_position_long_add_reduce():
    qty, avg, side = _apply_trade_to_position(
        Decimal("0"),
        Decimal("0"),
        Decimal("1"),
        Decimal("100"),
        "buy",
    )
    assert qty == Decimal("1.00000000")
    assert avg == Decimal("100.00000000")
    assert side == "long"

    qty, avg, side = _apply_trade_to_position(qty, avg, Decimal("1"), Decimal("110"), "buy")
    assert qty == Decimal("2.00000000")
    assert avg == Decimal("105.00000000")
    assert side == "long"

    qty, avg, side = _apply_trade_to_position(qty, avg, Decimal("0.5"), Decimal("120"), "sell")
    assert qty == Decimal("1.50000000")
    assert avg == Decimal("105.00000000")
    assert side == "long"


def test_apply_trade_to_position_flip_and_short_reduce():
    qty, avg, side = _apply_trade_to_position(
        Decimal("1"),
        Decimal("100"),
        Decimal("2"),
        Decimal("90"),
        "sell",
    )
    assert qty == Decimal("-1.00000000")
    assert avg == Decimal("90.00000000")
    assert side == "short"

    qty, avg, side = _apply_trade_to_position(qty, avg, Decimal("0.4"), Decimal("95"), "buy")
    assert qty == Decimal("-0.60000000")
    assert avg == Decimal("90.00000000")
    assert side == "short"
