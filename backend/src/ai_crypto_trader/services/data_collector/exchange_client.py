from typing import List, Protocol

OHLCV = List[float]  # [timestamp(ms), open, high, low, close, volume]


class ExchangeClient(Protocol):
    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, since: int | None = None, limit: int | None = None
    ) -> List[OHLCV]:
        ...

    async def close(self) -> None:
        ...
