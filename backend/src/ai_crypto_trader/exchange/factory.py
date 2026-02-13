from __future__ import annotations

from ai_crypto_trader.exchange.base import BaseExchangeClient, ExchangeError
from ai_crypto_trader.exchange.binance_spot import BinanceSpotClient

_client_cache: dict[str, BaseExchangeClient] = {}


def get_exchange_client(exchange: str) -> BaseExchangeClient:
    key = (exchange or "").strip().lower()
    cached = _client_cache.get(key)
    if cached is not None:
        return cached
    if key == "binance_spot_testnet":
        client = BinanceSpotClient.from_env()
        _client_cache[key] = client
        return client
    raise ExchangeError(
        f"Unsupported exchange: {exchange}",
        retriable=False,
    )
