from decimal import Decimal

from ai_crypto_trader.exchange.binance_spot import quantize_qty_up, validate_notional_bounds


def test_validate_notional_bounds_min_violation_with_suggested_qty() -> None:
    violation = validate_notional_bounds(
        qty=Decimal("0.001"),
        reference_price=Decimal("2931"),
        min_notional=Decimal("10"),
        max_notional=None,
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
    )
    assert violation is not None
    assert violation["violated"] == "min"
    assert str(violation["suggested_min_qty"]) == "0.004"


def test_validate_notional_bounds_ok_when_above_min() -> None:
    violation = validate_notional_bounds(
        qty=Decimal("0.01"),
        reference_price=Decimal("2931"),
        min_notional=Decimal("10"),
        max_notional=None,
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
    )
    assert violation is None


def test_quantize_qty_up_respects_min_qty_and_step() -> None:
    suggested = quantize_qty_up(
        raw_suggested_qty=Decimal("0.0031"),
        min_qty=Decimal("0.005"),
        step_size=Decimal("0.001"),
    )
    assert suggested == Decimal("0.005")


def test_quantize_qty_up_rounds_notional_suggestion_to_step() -> None:
    suggested = quantize_qty_up(
        raw_suggested_qty=Decimal("0.0025854223"),
        min_qty=None,
        step_size=Decimal("0.00001"),
    )
    assert suggested == Decimal("0.00259")
