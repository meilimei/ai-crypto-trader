import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Sequence

from sqlalchemy import select
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import AsyncSessionLocal
from ai_crypto_trader.common.models import Candle, Exchange
from ai_crypto_trader.services.data_collector.config import CollectorConfig
from ai_crypto_trader.services.data_collector.exchange_client import ExchangeClient, OHLCV

logger = logging.getLogger(__name__)


def timeframe_to_millis(timeframe: str) -> int:
    """
    Convert CCXT timeframe strings to milliseconds. Supports common intervals; defaults to 1m.
    """
    mapping = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "1d": 86400,
    }
    seconds = mapping.get(timeframe, 60)
    return seconds * 1000


class DataCollector:
    def __init__(self, config: CollectorConfig, client: ExchangeClient) -> None:
        self.config = config
        self.client = client

    async def collect_forever(self) -> None:
        while True:
            try:
                await self.collect_once()
            except Exception:
                logger.exception("Collector cycle failed")
            await asyncio.sleep(self.config.poll_seconds)

    async def collect_once(self) -> None:
        async with AsyncSessionLocal() as session:
            exchange_id = await self._ensure_exchange(session, self.config.exchange_name)
            for symbol in self.config.symbols:
                await self._sync_symbol(session, exchange_id, symbol)
            await session.commit()

    async def _ensure_exchange(self, session: AsyncSession, slug: str) -> int:
        result = await session.execute(select(Exchange).where(Exchange.slug == slug))
        existing = result.scalar_one_or_none()
        if existing:
            return existing.id
        exchange = Exchange(name=slug.upper(), slug=slug)
        session.add(exchange)
        await session.flush()
        return exchange.id

    async def _sync_symbol(self, session: AsyncSession, exchange_id: int, symbol: str) -> None:
        timeframe = self.config.timeframe
        exchange_name = self.config.exchange_name
        latest_open_time = await self._get_latest_open_time(session, exchange_id, symbol, timeframe)

        since = None
        limit = self.config.lookback_limit
        if latest_open_time:
            since = int(latest_open_time.timestamp() * 1000) + timeframe_to_millis(timeframe)
            limit = None

        retries = 0
        backoff = 2
        max_retries = 3
        ohlcv: List[OHLCV] = []
        while retries <= max_retries:
            try:
                ohlcv = await self.client.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
                break
            except Exception:
                retries += 1
                if retries > max_retries:
                    logger.exception("Failed fetching OHLCV", extra={"symbol": symbol})
                    return
                await asyncio.sleep(backoff)
                backoff *= 2

        if not ohlcv:
            logger.info(
                "Candle sync exchange=%s symbol=%s timeframe=%s fetched=%s inserted=%s latest=%s",
                exchange_name,
                symbol,
                timeframe,
                0,
                0,
                latest_open_time.isoformat() if latest_open_time else None,
            )
            return

        candles = self._to_candles(exchange_id, symbol, timeframe, ohlcv)
        inserted = await self._dedup_and_insert(session, candles)
        latest_ts = candles[-1].open_time.isoformat()
        logger.info(
            "Candle sync exchange=%s symbol=%s timeframe=%s fetched=%s inserted=%s latest=%s",
            exchange_name,
            symbol,
            timeframe,
            len(candles),
            inserted,
            latest_ts,
        )

    async def _get_latest_open_time(
        self, session: AsyncSession, exchange_id: int, symbol: str, timeframe: str
    ) -> datetime | None:
        stmt = (
            select(Candle.open_time)
            .where(
                Candle.exchange_id == exchange_id,
                Candle.symbol == symbol,
                Candle.timeframe == timeframe,
            )
            .order_by(Candle.open_time.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    def _to_candles(
        self, exchange_id: int, symbol: str, timeframe: str, ohlcv: Sequence[OHLCV]
    ) -> List[Candle]:
        candles: List[Candle] = []
        tf_ms = timeframe_to_millis(timeframe)
        for entry in ohlcv:
            ts_ms, open_, high, low, close, volume = entry
            open_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            close_time = open_time + timedelta(milliseconds=tf_ms)
            candles.append(
                Candle(
                    exchange_id=exchange_id,
                    symbol=symbol,
                    timeframe=timeframe,
                    open_time=open_time,
                    close_time=close_time,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                )
            )
        return candles

    async def _dedup_and_insert(self, session: AsyncSession, candles: Iterable[Candle]) -> int:
        candles_list = list(candles)
        if not candles_list:
            return 0

        # De-dupe within the batch first
        deduped: Dict[tuple, Candle] = {}
        for c in candles_list:
            key = (c.exchange_id, c.symbol, c.timeframe, c.open_time)
            if key not in deduped:
                deduped[key] = c

        values = [
            {
                "exchange_id": c.exchange_id,
                "symbol": c.symbol,
                "timeframe": c.timeframe,
                "open_time": c.open_time,
                "close_time": c.close_time,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
            }
            for c in deduped.values()
        ]
        if not values:
            return 0

        bind = session.get_bind()
        dialect_name = bind.dialect.name if bind else ""

        if dialect_name == "sqlite":
            stmt = sqlite.insert(Candle).values(values).prefix_with("OR IGNORE")
        elif dialect_name == "postgresql":
            stmt = (
                postgresql.insert(Candle)
                .values(values)
                .on_conflict_do_nothing(
                    index_elements=[
                        Candle.exchange_id,
                        Candle.symbol,
                        Candle.timeframe,
                        Candle.open_time,
                    ]
                )
            )
        else:
            stmt = Candle.__table__.insert().values(values)

        result = await session.execute(stmt)
        return result.rowcount or 0
