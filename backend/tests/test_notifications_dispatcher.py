from __future__ import annotations

from types import SimpleNamespace

from ai_crypto_trader.services.notifications.dispatcher import _resolve_channel


def test_resolve_channel_prefers_row_channel() -> None:
    row = SimpleNamespace(channel=" NOOP ", payload={"channel": "log"})
    assert _resolve_channel(row) == "NOOP"


def test_resolve_channel_falls_back_to_payload() -> None:
    row = SimpleNamespace(channel="", payload={"channel": "log"})
    assert _resolve_channel(row) == "log"


def test_resolve_channel_empty_when_missing() -> None:
    row = SimpleNamespace(channel=None, payload={})
    assert _resolve_channel(row) == ""
