"""
Development helper to create all tables against the configured DATABASE_URL.

Usage (SQLite example):
    export DATABASE_URL=sqlite+aiosqlite:///./dev.db
    poetry run python -m ai_crypto_trader.init_db

For production, prefer Alembic migrations to manage schema changes.
"""

import asyncio

from pathlib import Path
from dotenv import load_dotenv

from ai_crypto_trader.common.database import engine
from ai_crypto_trader.common.models import Base

load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env", override=False)


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


if __name__ == "__main__":
    asyncio.run(init_db())
