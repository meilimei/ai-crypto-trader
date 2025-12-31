"""
CLI entrypoint for the paper trading engine.

Usage:
    export DATABASE_URL=sqlite+aiosqlite:///./dev.db
    poetry run python -m ai_crypto_trader.services.paper_trader.run
"""

import asyncio
import logging

from ai_crypto_trader.common.env import init_env

init_env()

from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig  # noqa: E402
from ai_crypto_trader.services.paper_trader.engine import PaperTradingEngine  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


async def main() -> None:
    config = PaperTraderConfig.from_env()
    engine = PaperTradingEngine(config)
    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
