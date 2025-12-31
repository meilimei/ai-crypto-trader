import logging
import os
from typing import AsyncGenerator, Dict

from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from ai_crypto_trader.common.models import Base

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. Please set DATABASE_URL in backend/.env")

rendered_url = make_url(DATABASE_URL).render_as_string(hide_password=True)
logger.info("Initializing database engine", extra={"database_url": rendered_url})


def _get_connect_args(url: str) -> Dict[str, object]:
    return {"check_same_thread": False} if url.startswith("sqlite") else {}


engine = create_async_engine(
    DATABASE_URL,
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",
    future=True,
    pool_pre_ping=True,
    connect_args=_get_connect_args(DATABASE_URL),
)
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that provides an async SQLAlchemy session.
    """
    async with AsyncSessionLocal() as session:
        yield session


__all__ = ["Base", "engine", "AsyncSessionLocal", "get_db_session"]
