from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import AsyncSessionLocal
from ai_crypto_trader.models.paper_policies import PaperOrderRequestEvent

logger = logging.getLogger(__name__)


def _lock_key(account_id: int, strategy_id: UUID | None, symbol: str) -> str:
    return f"order_rate:{account_id}:{strategy_id or ''}:{symbol}"


async def check_and_record_rate_limit(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_id: UUID | None,
    symbol: str,
    max_requests: int,
    window_seconds: int,
) -> Tuple[bool, int]:
    """
    Return (allowed, count) after recording an order request event.

    Uses an advisory lock to avoid races; inserts an event when allowed.
    """
    _ = session
    if max_requests <= 0 or window_seconds <= 0:
        return True, 0

    key = _lock_key(account_id, strategy_id, symbol)
    window_start = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)

    try:
        async with AsyncSessionLocal() as db_session:
            try:
                await db_session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": key}
                )

                count = await db_session.scalar(
                    select(func.count())
                    .select_from(PaperOrderRequestEvent)
                    .where(
                        PaperOrderRequestEvent.account_id == account_id,
                        PaperOrderRequestEvent.symbol == symbol,
                        PaperOrderRequestEvent.created_at >= window_start,
                        PaperOrderRequestEvent.strategy_id.is_not_distinct_from(strategy_id),
                    )
                )
                count_int = int(count or 0)
                if count_int >= max_requests:
                    await db_session.rollback()
                    return False, count_int

                db_session.add(
                    PaperOrderRequestEvent(
                        account_id=account_id,
                        strategy_id=strategy_id,
                        symbol=symbol,
                    )
                )
                await db_session.commit()
                return True, count_int + 1
            except Exception:
                await db_session.rollback()
                raise
    except Exception:
        logger.exception("Rate limit check failed")
        return True, 0
