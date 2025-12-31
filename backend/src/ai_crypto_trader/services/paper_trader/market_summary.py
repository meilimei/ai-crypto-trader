import math
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import Candle, Exchange


def timeframe_seconds(timeframe: str) -> int:
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
    return mapping.get(timeframe, 60)


@dataclass
class MarketSummary:
    symbol: str
    timeframe: str
    last_close: Decimal
    return_24h: Decimal
    volatility_24h: Decimal
    trend: str


class MarketSummaryBuilder:
    def __init__(self, session: AsyncSession, exchange_slug: str, timeframe: str) -> None:
        self.session = session
        self.exchange_slug = exchange_slug
        self.timeframe = timeframe

    async def build(self, symbol: str) -> Optional[MarketSummary]:
        exchange = await self.session.scalar(select(Exchange).where(Exchange.slug == self.exchange_slug))
        if not exchange:
            return None

        tf_seconds = timeframe_seconds(self.timeframe)
        candles_needed = max(1, math.ceil(24 * 3600 / tf_seconds))

        stmt = (
            select(Candle)
            .where(
                Candle.exchange_id == exchange.id,
                Candle.symbol == symbol,
                Candle.timeframe == self.timeframe,
            )
            .order_by(Candle.open_time.desc())
            .limit(candles_needed)
        )
        result = await self.session.execute(stmt)
        rows: List[Candle] = list(reversed(result.scalars().all()))
        if not rows:
            return None

        closes = [Decimal(c.close) for c in rows]
        last_close = closes[-1]
        return_24h = Decimal("0")
        volatility = Decimal("0")
        if len(closes) > 1:
            return_24h = (last_close - closes[0]) / closes[0] if closes[0] != 0 else Decimal("0")
            mean = sum(closes) / Decimal(len(closes))
            variance = sum((c - mean) ** 2 for c in closes) / Decimal(len(closes))
            volatility = variance.sqrt() if variance >= 0 else Decimal("0")

        trend = "trending_up" if return_24h > Decimal("0.01") else "trending_down" if return_24h < Decimal("-0.01") else "choppy"

        return MarketSummary(
            symbol=symbol,
            timeframe=self.timeframe,
            last_close=last_close,
            return_24h=return_24h,
            volatility_24h=volatility,
            trend=trend,
        )
