import os
from dataclasses import dataclass
from typing import List


def _split_symbols(raw: str) -> List[str]:
    return [sym.strip() for sym in raw.split(",") if sym.strip()]


@dataclass
class CollectorConfig:
    exchange_name: str
    symbols: List[str]
    timeframe: str
    poll_seconds: int
    lookback_limit: int

    @classmethod
    def from_env(cls) -> "CollectorConfig":
        return cls(
            exchange_name=os.getenv("EXCHANGE_NAME", "binance").lower(),
            symbols=_split_symbols(os.getenv("DATA_SYMBOLS", "BTC/USDT,ETH/USDT")),
            timeframe=os.getenv("DATA_TIMEFRAME", "1m"),
            poll_seconds=int(os.getenv("DATA_POLL_SECONDS", "30")),
            lookback_limit=int(os.getenv("DATA_LOOKBACK_LIMIT", "500")),
        )
