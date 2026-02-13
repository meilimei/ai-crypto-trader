import os

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/db")

from ai_crypto_trader.exchange.base import ExchangeError
from ai_crypto_trader.services.live_exchange.order_service import _classify_exchange_error


def test_local_precheck_filter_failure_does_not_increment_attempts() -> None:
    exc = ExchangeError(
        "Filter failure: NOTIONAL",
        retriable=False,
        error_code=-4000,
        details={
            "source": "local_precheck",
            "filter_type": "NOTIONAL",
        },
    )
    decision = _classify_exchange_error(exc, current_attempts=0, max_attempts=8)
    assert decision.target_status == "rejected"
    assert decision.local_precheck is True
    assert decision.attempts == 0


def test_retriable_error_increments_attempts_and_retries() -> None:
    exc = ExchangeError(
        "Binance request failed: timeout",
        retriable=True,
        error_code=-1003,
    )
    decision = _classify_exchange_error(exc, current_attempts=1, max_attempts=8)
    assert decision.target_status == "pending"
    assert decision.retriable is True
    assert decision.attempts == 2
