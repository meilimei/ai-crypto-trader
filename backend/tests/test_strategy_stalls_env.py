from __future__ import annotations

import importlib
import sys
import types

import pytest


def _import_strategy_stalls(monkeypatch: pytest.MonkeyPatch):
    admin_pkg = types.ModuleType("ai_crypto_trader.services.admin_actions")
    admin_pkg.__path__ = []
    throttled_mod = types.ModuleType("ai_crypto_trader.services.admin_actions.throttled")

    async def _noop_write_admin_action_throttled(*args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    throttled_mod.write_admin_action_throttled = _noop_write_admin_action_throttled  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "ai_crypto_trader.services.admin_actions", admin_pkg)
    monkeypatch.setitem(sys.modules, "ai_crypto_trader.services.admin_actions.throttled", throttled_mod)

    module_name = "ai_crypto_trader.services.monitoring.strategy_stalls"
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


def test_strategy_stalls_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("STRATEGY_STALL_SECONDS", raising=False)
    monkeypatch.delenv("STRATEGY_ACTIVITY_GRACE_SECONDS", raising=False)
    monkeypatch.delenv("STRATEGY_STALL_ALERT_THROTTLE_SECONDS", raising=False)

    stalls = _import_strategy_stalls(monkeypatch)

    assert stalls.STALL_SECONDS == 900
    assert stalls.ACTIVITY_GRACE_SECONDS == 300
    assert stalls.ALERT_THROTTLE_SECONDS == 1800


def test_strategy_stalls_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STRATEGY_STALL_SECONDS", "42")
    monkeypatch.setenv("STRATEGY_ACTIVITY_GRACE_SECONDS", "bad")
    monkeypatch.setenv("STRATEGY_STALL_ALERT_THROTTLE_SECONDS", "123")

    stalls = _import_strategy_stalls(monkeypatch)

    assert stalls.STALL_SECONDS == 42
    assert stalls.ACTIVITY_GRACE_SECONDS == 300
    assert stalls.ALERT_THROTTLE_SECONDS == 123
