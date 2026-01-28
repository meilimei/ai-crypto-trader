from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import importlib
import sys
from types import SimpleNamespace
from typing import Any, Iterable
import types

import pytest

from ai_crypto_trader.common.models import AdminAction


@dataclass
class _ScalarFirst:
    value: Any

    def first(self) -> Any:
        return self.value


@dataclass
class _ExecuteResult:
    rows: Iterable[Any] | None = None
    alert: AdminAction | None = None

    def all(self) -> list[Any]:
        return list(self.rows or [])

    def scalars(self) -> _ScalarFirst:
        return _ScalarFirst(self.alert)


class _FakeSession:
    def __init__(self, account_id: int, strategy_id: str, symbol: str) -> None:
        self.info: dict[str, Any] = {}
        self._pair = (account_id, strategy_id, symbol)
        self.alerts: list[AdminAction] = []
        self.writes: list[dict[str, Any]] = []
        self.last_trade_at: datetime | None = None
        self.last_activity_at: datetime | None = None
        self.first_seen_at: datetime | None = None
        self.current_now: datetime | None = None
        self._scalar_idx = 0
        self._execute_idx = 0

    def reset_for_tick(self, now: datetime) -> None:
        self.current_now = now
        self._scalar_idx = 0
        self._execute_idx = 0

    async def scalar(self, stmt) -> Any:  # type: ignore[no-untyped-def]
        # maybe_alert_strategy_stalls calls scalar three times per tick:
        # last_trade_at, last_activity_at, first_seen_at.
        self._scalar_idx += 1
        mod = (self._scalar_idx - 1) % 3
        if mod == 0:
            return self.last_trade_at
        if mod == 1:
            return self.last_activity_at
        return self.first_seen_at

    async def execute(self, stmt) -> _ExecuteResult:  # type: ignore[no-untyped-def]
        # Per tick with one pair, execute is called twice:
        # 1) pair discovery, 2) latest alert lookup.
        self._execute_idx += 1
        mod = (self._execute_idx - 1) % 2
        if mod == 0:
            acct, strat, sym = self._pair
            rows = [SimpleNamespace(account_id=acct, strategy_id=strat, symbol=sym)]
            return _ExecuteResult(rows=rows)
        latest_alert = self.alerts[-1] if self.alerts else None
        return _ExecuteResult(alert=latest_alert)


@pytest.mark.asyncio
async def test_strategy_stalls_state_machine_transitions(monkeypatch: pytest.MonkeyPatch) -> None:
    # Avoid importing the real admin_actions package (it initializes the DB engine).
    admin_pkg = types.ModuleType("ai_crypto_trader.services.admin_actions")
    admin_pkg.__path__ = []  # mark as package
    throttled_mod = types.ModuleType("ai_crypto_trader.services.admin_actions.throttled")

    async def _noop_write_admin_action_throttled(*args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    throttled_mod.write_admin_action_throttled = _noop_write_admin_action_throttled  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "ai_crypto_trader.services.admin_actions", admin_pkg)
    monkeypatch.setitem(sys.modules, "ai_crypto_trader.services.admin_actions.throttled", throttled_mod)

    stalls = importlib.import_module("ai_crypto_trader.services.monitoring.strategy_stalls")

    # Ensure env overrides do not interfere with explicit thresholds used in this test.
    monkeypatch.delenv("STRATEGY_STALL_SECONDS", raising=False)
    monkeypatch.delenv("STRATEGY_STALL_ACTIVITY_GRACE_SECONDS", raising=False)
    monkeypatch.delenv("STRATEGY_STALL_THROTTLE_SECONDS", raising=False)
    monkeypatch.delenv("PAPER_STALL_SECONDS", raising=False)
    monkeypatch.delenv("PAPER_ACTIVITY_GRACE_SECONDS", raising=False)
    monkeypatch.delenv("PAPER_STALL_THROTTLE_SECONDS", raising=False)

    session = _FakeSession(account_id=5, strategy_id="strategy-1", symbol="ETHUSDT")

    async def _fake_write_admin_action_throttled(
        db_session,
        *,
        action_type: str,
        account_id: int,
        symbol: str | None,
        status: str,
        payload: dict[str, Any],
        window_seconds: int,
        dedupe_key: str,
    ) -> None:
        db_session.writes.append(
            {
                "action": action_type,
                "account_id": account_id,
                "status": status,
                "payload": payload,
                "window_seconds": window_seconds,
                "dedupe_key": dedupe_key,
                "created_at": db_session.current_now,
            }
        )
        if action_type == "STRATEGY_ALERT":
            db_session.alerts.append(
                AdminAction(
                    action=action_type,
                    status=status,
                    meta=payload,
                    created_at=db_session.current_now,
                    dedupe_key=dedupe_key,
                )
            )

    monkeypatch.setattr(stalls, "write_admin_action_throttled", _fake_write_admin_action_throttled)

    t0 = datetime(2026, 1, 27, tzinfo=timezone.utc)
    prior_effective = (t0 - timedelta(minutes=10)).isoformat()
    session.alerts.append(
        AdminAction(
            action="STRATEGY_ALERT",
            status="NO_TRADE_STALL",
            meta={
                "account_id": 5,
                "strategy_id": "strategy-1",
                "symbol": "ETHUSDT",
                "effective_last_trade_at": prior_effective,
            },
            created_at=t0,
            dedupe_key="seed:NO_TRADE_STALL",
        )
    )

    # 1) Healthy after a new trade -> emit exactly one STALL_RECOVERED.
    now1 = t0 + timedelta(seconds=10)
    session.last_trade_at = now1
    session.last_activity_at = now1
    session.first_seen_at = None
    session.reset_for_tick(now1)
    await stalls.maybe_alert_strategy_stalls(
        session,
        now_utc=now1,
        stall_seconds=20,
        activity_grace_seconds=5,
        throttle_seconds=60,
    )
    recovered_alerts = [a for a in session.alerts if a.status == "STALL_RECOVERED"]
    assert len(recovered_alerts) == 1

    # 2) Another trade while already healthy -> no additional recovery alert.
    now2 = now1 + timedelta(seconds=5)
    session.last_trade_at = now2
    session.last_activity_at = now2
    session.reset_for_tick(now2)
    await stalls.maybe_alert_strategy_stalls(
        session,
        now_utc=now2,
        stall_seconds=20,
        activity_grace_seconds=5,
        throttle_seconds=60,
    )
    recovered_alerts = [a for a in session.alerts if a.status == "STALL_RECOVERED"]
    assert len(recovered_alerts) == 1

    # 3) Stall again after time passes -> emit a new NO_TRADE_STALL.
    now3 = now2 + timedelta(seconds=120)
    session.last_trade_at = now2
    session.last_activity_at = now2
    session.reset_for_tick(now3)
    await stalls.maybe_alert_strategy_stalls(
        session,
        now_utc=now3,
        stall_seconds=20,
        activity_grace_seconds=5,
        throttle_seconds=60,
    )
    stall_alerts = [a for a in session.alerts if a.status == "NO_TRADE_STALL"]
    assert len(stall_alerts) == 2
