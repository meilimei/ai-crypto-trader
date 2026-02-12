from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Optional, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction
from ai_crypto_trader.services.paper_trader.execution import ExecutionResult, execute_market_order_with_costs
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)


class Broker(Protocol):
    async def place_order(
        self,
        *,
        session: AsyncSession,
        account_id: int,
        symbol: str,
        side: str,
        qty: Decimal,
        mid_price: Decimal,
        fee_bps: Decimal,
        slippage_bps: Decimal,
        meta: Optional[Dict[str, str]] = None,
    ) -> ExecutionResult:
        ...


@dataclass
class PaperBroker:
    async def place_order(
        self,
        *,
        session: AsyncSession,
        account_id: int,
        symbol: str,
        side: str,
        qty: Decimal,
        mid_price: Decimal,
        fee_bps: Decimal,
        slippage_bps: Decimal,
        meta: Optional[Dict[str, str]] = None,
    ) -> ExecutionResult:
        return await execute_market_order_with_costs(
            session=session,
            account_id=account_id,
            symbol=symbol,
            side=side,
            qty=qty,
            mid_price=mid_price,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            meta=meta,
        )


@dataclass
class MockLiveBroker:
    async def place_order(
        self,
        *,
        session: AsyncSession,
        account_id: int,
        symbol: str,
        side: str,
        qty: Decimal,
        mid_price: Decimal,
        fee_bps: Decimal,
        slippage_bps: Decimal,
        meta: Optional[Dict[str, str]] = None,
    ) -> ExecutionResult:
        execution = await execute_market_order_with_costs(
            session=session,
            account_id=account_id,
            symbol=symbol,
            side=side,
            qty=qty,
            mid_price=mid_price,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            meta=meta,
        )
        session.add(
            AdminAction(
                action="BROKER_MOCK_LIVE_ORDER",
                status="accepted",
                message="Mock live broker accepted order",
                meta=json_safe(
                    {
                        "account_id": str(account_id),
                        "symbol": symbol,
                        "side": side,
                        "qty": str(qty),
                        "mid_price": str(mid_price),
                        "order_id": execution.order.id,
                        "trade_id": execution.trade.id,
                        "meta": meta or {},
                    }
                ),
            )
        )
        return execution


def get_broker(*, mode: str | None = None) -> Broker:
    mode_value = (mode or os.getenv("BROKER_MODE") or "paper").strip().lower()
    if mode_value == "mock_live":
        return MockLiveBroker()
    if mode_value != "paper":
        logger.warning("Unknown BROKER_MODE, falling back to paper", extra={"broker_mode": mode_value})
    return PaperBroker()
