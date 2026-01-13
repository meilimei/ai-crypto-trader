from __future__ import annotations

from decimal import Decimal
from typing import Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.models.paper_policies import PaperPositionPolicy, PaperRiskPolicy


DEFAULT_MIN_QTY = Decimal("0.00000001")
DEFAULT_MIN_ORDER_NOTIONAL_USDT = Decimal("0.01")


async def get_or_create_default_policies(
    session: AsyncSession,
    account_id: int,
) -> Tuple[PaperRiskPolicy, PaperPositionPolicy]:
    """
    Ensure a policy row exists for the account.

    Defaults are minimal safety thresholds (min_qty/min_order_notional_usdt) with other fields left NULL.
    """
    risk_policy = await session.scalar(
        select(PaperRiskPolicy).where(PaperRiskPolicy.account_id == account_id).limit(1)
    )
    if risk_policy is None:
        risk_policy = PaperRiskPolicy(account_id=account_id)
        session.add(risk_policy)

    position_policy = await session.scalar(
        select(PaperPositionPolicy).where(PaperPositionPolicy.account_id == account_id).limit(1)
    )
    if position_policy is None:
        position_policy = PaperPositionPolicy(
            account_id=account_id,
            min_qty=DEFAULT_MIN_QTY,
            min_order_notional_usdt=DEFAULT_MIN_ORDER_NOTIONAL_USDT,
        )
        session.add(position_policy)

    await session.flush()
    return risk_policy, position_policy
