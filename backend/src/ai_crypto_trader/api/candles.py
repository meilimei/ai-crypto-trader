from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import Candle

router = APIRouter(prefix="/candles", tags=["market"])


@router.get("/latest")
async def latest_candles(
    symbol: str,
    timeframe: str = "1m",
    limit: int = 50,
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    """
    Return the latest candles ordered descending in DB but ascending in the response.
    """
    limit = max(1, min(limit, 500))

    stmt = (
        select(Candle)
        .where(
            Candle.symbol == symbol,
            Candle.timeframe == timeframe,
        )
        .order_by(Candle.open_time.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    candles = list(reversed(result.scalars().all()))
    if not candles:
        raise HTTPException(status_code=404, detail="No candles found for given parameters")

    return {
        "candles": [
            {
                "open_time": c.open_time.isoformat(),
                "close_time": c.close_time.isoformat(),
                "open": float(c.open),
                "high": float(c.high),
                "low": float(c.low),
                "close": float(c.close),
                "volume": float(c.volume),
                "symbol": c.symbol,
                "timeframe": c.timeframe,
                "exchange_id": c.exchange_id,
            }
            for c in candles
        ]
    }
