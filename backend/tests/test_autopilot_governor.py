from __future__ import annotations

from datetime import datetime, timedelta, timezone

from ai_crypto_trader.services.autopilot.governor import (
    AUTOPILOT_STATUS_PAUSED,
    AUTOPILOT_STATUS_RUNNING,
    is_autopilot_paused,
)


def test_is_autopilot_paused_with_no_timeout() -> None:
    assert is_autopilot_paused(status=AUTOPILOT_STATUS_PAUSED, paused_until=None)


def test_is_autopilot_paused_future_timeout() -> None:
    now = datetime.now(timezone.utc)
    assert is_autopilot_paused(
        status=AUTOPILOT_STATUS_PAUSED,
        paused_until=now + timedelta(seconds=30),
        now_utc=now,
    )


def test_is_autopilot_paused_expired_timeout() -> None:
    now = datetime.now(timezone.utc)
    assert not is_autopilot_paused(
        status=AUTOPILOT_STATUS_PAUSED,
        paused_until=now - timedelta(seconds=1),
        now_utc=now,
    )


def test_is_autopilot_paused_running_state() -> None:
    assert not is_autopilot_paused(status=AUTOPILOT_STATUS_RUNNING, paused_until=None)
