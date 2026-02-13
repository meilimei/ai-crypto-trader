import os

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/db")

from ai_crypto_trader.exchange.base import ExchangeError
from ai_crypto_trader.services.live_exchange.order_service import (
    DEFAULT_LIVE_EXCHANGE_POLICY,
    _calc_backoff_seconds,
    _classify_exchange_error,
    _is_terminal_status,
)


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


def test_backoff_uses_policy_defaults_and_caps() -> None:
    delay0 = _calc_backoff_seconds(attempts=0, policy=DEFAULT_LIVE_EXCHANGE_POLICY)
    delay2 = _calc_backoff_seconds(attempts=2, policy=DEFAULT_LIVE_EXCHANGE_POLICY)
    delay9 = _calc_backoff_seconds(attempts=9, policy=DEFAULT_LIVE_EXCHANGE_POLICY)
    assert delay0 >= DEFAULT_LIVE_EXCHANGE_POLICY.base_backoff_seconds
    assert delay2 >= DEFAULT_LIVE_EXCHANGE_POLICY.base_backoff_seconds * (2**2)
    assert delay9 <= DEFAULT_LIVE_EXCHANGE_POLICY.max_backoff_seconds


def test_terminal_status_detection() -> None:
    assert _is_terminal_status("filled") is True
    assert _is_terminal_status("accepted") is True
    assert _is_terminal_status("failed") is True
    assert _is_terminal_status("pending") is False
