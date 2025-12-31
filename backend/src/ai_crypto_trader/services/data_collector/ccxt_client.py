import logging
from typing import Any, List

import ccxt.async_support as ccxt

from ai_crypto_trader.services.data_collector.exchange_client import ExchangeClient, OHLCV

logger = logging.getLogger(__name__)


class CcxtClient(ExchangeClient):
    def __init__(self, exchange_name: str) -> None:
        exchange_cls = getattr(ccxt, exchange_name, None)
        if exchange_cls is None:
            raise ValueError(f"Exchange {exchange_name} not supported by ccxt")
        self.exchange: ccxt.Exchange = exchange_cls({"enableRateLimit": True})

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, since: int | None = None, limit: int | None = None
    ) -> List[OHLCV]:
        logger.debug(
            "Fetching OHLCV", extra={"symbol": symbol, "timeframe": timeframe, "since": since, "limit": limit}
        )
        data: List[Any] = await self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        return data  # type: ignore

    async def close(self) -> None:
        try:
            await self.exchange.close()
        except Exception:
            logger.exception("Failed to close ccxt client")
