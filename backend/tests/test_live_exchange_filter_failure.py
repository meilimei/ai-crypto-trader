from ai_crypto_trader.exchange.base import ExchangeError
from ai_crypto_trader.exchange.binance_spot import _extract_filter_type


def test_extract_filter_type_from_exchange_message() -> None:
    assert _extract_filter_type("Binance order failed: Filter failure: NOTIONAL") == "NOTIONAL"
    assert _extract_filter_type("Filter failure: LOT_SIZE") == "LOT_SIZE"
    assert _extract_filter_type("invalid quantity") is None


def test_extract_filter_type_with_exchange_error_message() -> None:
    exc = ExchangeError(
        "Binance order failed: Filter failure: LOT_SIZE",
        retriable=False,
    )
    assert _extract_filter_type(str(exc)) == "LOT_SIZE"
