from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from fastapi.responses import JSONResponse

os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from ai_crypto_trader.api import admin_paper_trader as admin_api


@dataclass
class _DummySession:
    committed: int = 0
    rolled_back: int = 0

    async def commit(self) -> None:
        self.committed += 1

    async def rollback(self) -> None:
        self.rolled_back += 1


def test_smoke_trade_paused_precedes_engine_not_running(monkeypatch) -> None:
    now_utc = datetime.now(timezone.utc)
    paused_until = now_utc + timedelta(minutes=5)

    class _State:
        pass

    paused_state = _State()
    paused_state.id = 1
    paused_state.account_id = 5
    paused_state.strategy_config_id = 5
    paused_state.status = "paused"
    paused_state.reason = "manual_test"
    paused_state.meta = {"source": "test"}
    paused_state.created_at = now_utc
    paused_state.updated_at = now_utc
    paused_state.paused_until = paused_until

    async def _fake_get_or_create(*args, **kwargs):  # type: ignore[no-untyped-def]
        return paused_state

    async def _noop_async(*args, **kwargs):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr(admin_api, "get_or_create_autopilot_state", _fake_get_or_create)
    monkeypatch.setattr(admin_api, "is_autopilot_paused", lambda **kwargs: True)
    monkeypatch.setattr(
        admin_api,
        "serialize_autopilot_state",
        lambda state: {
            "status": "paused",
            "reason": "manual_test",
            "paused_until": paused_until.isoformat(),
            "meta": {"source": "test"},
            "updated_at": now_utc.isoformat(),
        },
    )
    monkeypatch.setattr(admin_api, "emit_trade_decision", _noop_async)
    monkeypatch.setattr(admin_api, "log_order_rejected_throttled", _noop_async)

    payload = admin_api.SmokeTradeRequest(
        account_id=5,
        strategy_config_id=5,
        symbol="ETHUSDT",
        side="buy",
        qty=Decimal("0.001"),
    )
    session = _DummySession()

    async def _run() -> None:
        response = await admin_api.smoke_trade(payload, session)
        assert isinstance(response, JSONResponse)

        body = json.loads(response.body.decode("utf-8"))
        assert body["ok"] is False
        assert body["reject"]["code"] == "AUTOPILOT_PAUSED"
        assert body["reject"]["reason"] == "Autopilot is paused"
        assert body["reject"]["details"]["status"] == "paused"

    asyncio.run(_run())
