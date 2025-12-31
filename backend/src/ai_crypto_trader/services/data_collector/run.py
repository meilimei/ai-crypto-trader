"""
CLI entrypoint for the market data collector.

Usage:
    export DATABASE_URL=sqlite+aiosqlite:///./dev.db
    export EXCHANGE_NAME=binance
    export DATA_SYMBOLS=BTC/USDT,ETH/USDT
    export DATA_TIMEFRAME=1m
    export DATA_POLL_SECONDS=30
    export DATA_LOOKBACK_LIMIT=500
    poetry run python -m ai_crypto_trader.services.data_collector.run
"""

import asyncio
import logging

from ai_crypto_trader.common.database import engine
from ai_crypto_trader.common.env import init_env
from ai_crypto_trader.services.data_collector.ccxt_client import CcxtClient
from ai_crypto_trader.services.data_collector.collector import DataCollector
from ai_crypto_trader.services.data_collector.config import CollectorConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


async def main() -> None:
    init_env()
    config = CollectorConfig.from_env()
    client = CcxtClient(config.exchange_name)
    collector = DataCollector(config, client)

    try:
        await collector.collect_forever()
    finally:
        await client.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
from ai_crypto_trader.common.env import init_env

init_env()
