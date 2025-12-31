from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, field_serializer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import (
    EquitySnapshot,
    PaperAccount,
    PaperBalance,
    PaperOrder,
    PaperPosition,
    PaperTrade,
)

router = APIRouter(prefix="/paper-trader", tags=["paper"])


class _DecimalModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    @staticmethod
    def _serialize_decimal(value: Optional[Decimal]) -> Optional[float]:
        return float(value) if value is not None else None


class DashboardAccount(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    base_ccy: str
    created_at: datetime


class DashboardBalance(_DecimalModel):
    ccy: str
    available: Decimal
    updated_at: datetime

    @field_serializer("available")
    def _ser_decimal(self, value: Decimal) -> Optional[float]:
        return self._serialize_decimal(value)


class DashboardPosition(_DecimalModel):
    symbol: str
    side: str
    qty: Decimal
    avg_price: Decimal
    updated_at: datetime

    @field_serializer("qty", "avg_price")
    def _ser_decimal(self, value: Decimal) -> Optional[float]:
        return self._serialize_decimal(value)


class DashboardOrder(_DecimalModel):
    id: int
    account_id: int
    symbol: str
    side: str
    type: str
    status: str
    requested_qty: Decimal
    filled_qty: Decimal
    avg_fill_price: Optional[Decimal] = None
    fee_paid: Optional[Decimal] = None
    created_at: datetime

    @field_serializer("requested_qty", "filled_qty", "avg_fill_price", "fee_paid")
    def _ser_decimal(self, value: Optional[Decimal]) -> Optional[float]:
        return self._serialize_decimal(value)


class DashboardTrade(_DecimalModel):
    id: int
    account_id: int
    symbol: str
    side: str
    qty: Decimal
    price: Decimal
    fee: Optional[Decimal] = None
    realized_pnl: Optional[Decimal] = None
    created_at: datetime

    @field_serializer("qty", "price", "fee", "realized_pnl")
    def _ser_decimal(self, value: Optional[Decimal]) -> Optional[float]:
        return self._serialize_decimal(value)


class DashboardEquityPoint(_DecimalModel):
    id: int
    equity: Decimal
    balance: Decimal
    unrealized_pnl: Decimal
    created_at: datetime

    @field_serializer("equity", "balance", "unrealized_pnl")
    def _ser_decimal(self, value: Decimal) -> Optional[float]:
        return self._serialize_decimal(value)


class DashboardResponse(BaseModel):
    account: DashboardAccount
    balances: List[DashboardBalance]
    positions: List[DashboardPosition]
    recent_orders: List[DashboardOrder]
    recent_trades: List[DashboardTrade]
    equity: List[DashboardEquityPoint]


@router.get("/dashboard", response_model=DashboardResponse)
async def paper_trader_dashboard(session: AsyncSession = Depends(get_db_session)) -> DashboardResponse:
    account = await session.scalar(select(PaperAccount).order_by(PaperAccount.created_at).limit(1))
    if account is None:
        raise HTTPException(
            status_code=404,
            detail="No paper trading account found. Start the paper trader to initialize data.",
        )

    balances = (
        await session.scalars(
            select(PaperBalance).where(PaperBalance.account_id == account.id).order_by(PaperBalance.ccy)
        )
    ).all()
    positions = (
        await session.scalars(
            select(PaperPosition)
            .where(PaperPosition.account_id == account.id)
            .order_by(PaperPosition.updated_at.desc())
        )
    ).all()
    orders = (
        await session.scalars(
            select(PaperOrder)
            .where(PaperOrder.account_id == account.id)
            .order_by(PaperOrder.created_at.desc())
            .limit(20)
        )
    ).all()
    trades = (
        await session.scalars(
            select(PaperTrade)
            .where(PaperTrade.account_id == account.id)
            .order_by(PaperTrade.created_at.desc())
            .limit(20)
        )
    ).all()
    equity = (
        await session.scalars(
            select(EquitySnapshot)
            .where(EquitySnapshot.account_id == account.id)
            .order_by(EquitySnapshot.created_at.desc())
            .limit(100)
        )
    ).all()

    return DashboardResponse(
        account=DashboardAccount.model_validate(account),
        balances=[DashboardBalance.model_validate(b) for b in balances],
        positions=[
            DashboardPosition(
                symbol=p.symbol,
                side=p.side,
                qty=p.qty,
                avg_price=p.avg_entry_price,
                updated_at=p.updated_at,
            )
            for p in positions
        ],
        recent_orders=[DashboardOrder.model_validate(o) for o in orders],
        recent_trades=[DashboardTrade.model_validate(t) for t in trades],
        equity=[DashboardEquityPoint.model_validate(e) for e in equity],
    )
