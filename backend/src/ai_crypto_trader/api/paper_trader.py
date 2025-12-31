from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, field_serializer
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import (
    EquitySnapshot,
    PaperAccount,
    PaperOrder,
    PaperPosition,
)

router = APIRouter(prefix="/paper", tags=["paper"])


class _DecimalModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    @staticmethod
    def _serialize_decimal(value: Optional[Decimal]) -> Optional[float]:
        return float(value) if value is not None else None


class PaperAccountResponse(_DecimalModel):
    id: int
    name: str
    base_ccy: str
    created_at: datetime
    current_equity: Optional[Decimal] = None

    @field_serializer("current_equity")
    def _ser_decimal(self, value: Optional[Decimal]) -> Optional[float]:
        return self._serialize_decimal(value)


class PaperPositionResponse(_DecimalModel):
    id: int
    account_id: int
    symbol: str
    side: str
    qty: Decimal
    avg_entry_price: Decimal
    unrealized_pnl: Decimal
    updated_at: datetime

    @field_serializer("qty", "avg_entry_price", "unrealized_pnl")
    def _ser_decimal(self, value: Decimal) -> Optional[float]:
        return self._serialize_decimal(value)


class PaperOrderResponse(_DecimalModel):
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


class EquitySnapshotResponse(_DecimalModel):
    id: int
    account_id: int
    equity: Decimal
    balance: Decimal
    unrealized_pnl: Decimal
    created_at: datetime

    @field_serializer("equity", "balance", "unrealized_pnl")
    def _ser_decimal(self, value: Decimal) -> Optional[float]:
        return self._serialize_decimal(value)


@router.get("/accounts", response_model=List[PaperAccountResponse])
async def list_accounts(
    account_id: Optional[int] = None,
    session: AsyncSession = Depends(get_db_session),
) -> List[PaperAccountResponse]:
    stmt = select(PaperAccount)
    if account_id is not None:
        stmt = stmt.where(PaperAccount.id == account_id)
    stmt = stmt.order_by(PaperAccount.id)

    accounts = (await session.scalars(stmt)).all()
    account_ids = [acct.id for acct in accounts]

    snapshots_map: dict[int, EquitySnapshot] = {}
    if account_ids:
        latest_snapshot_subq = (
            select(
                EquitySnapshot.account_id,
                func.max(EquitySnapshot.created_at).label("latest_created_at"),
            )
            .where(EquitySnapshot.account_id.in_(account_ids))
            .group_by(EquitySnapshot.account_id)
            .subquery()
        )
        snapshot_stmt = (
            select(EquitySnapshot)
            .join(
                latest_snapshot_subq,
                (EquitySnapshot.account_id == latest_snapshot_subq.c.account_id)
                & (EquitySnapshot.created_at == latest_snapshot_subq.c.latest_created_at),
            )
        )
        snapshot_results = await session.scalars(snapshot_stmt)
        for snap in snapshot_results.all():
            snapshots_map[snap.account_id] = snap

    return [
        PaperAccountResponse(
            id=acct.id,
            name=acct.name,
            base_ccy=acct.base_ccy,
            created_at=acct.created_at,
            current_equity=snapshots_map.get(acct.id).equity if snapshots_map.get(acct.id) else None,
        )
        for acct in accounts
    ]


@router.get("/positions", response_model=List[PaperPositionResponse])
async def list_positions(
    account_id: int = Query(...),
    symbol: Optional[str] = None,
    open_only: bool = True,
    session: AsyncSession = Depends(get_db_session),
) -> List[PaperPositionResponse]:
    stmt = select(PaperPosition).where(PaperPosition.account_id == account_id)
    if symbol:
        stmt = stmt.where(PaperPosition.symbol == symbol)
    if open_only:
        stmt = stmt.where(PaperPosition.qty != Decimal("0"))
    stmt = stmt.order_by(PaperPosition.symbol)

    positions = (await session.scalars(stmt)).all()
    return [PaperPositionResponse.model_validate(p) for p in positions]


@router.get("/orders", response_model=List[PaperOrderResponse])
async def list_orders(
    account_id: int = Query(...),
    symbol: Optional[str] = None,
    limit: int = 100,
    session: AsyncSession = Depends(get_db_session),
) -> List[PaperOrderResponse]:
    limit = max(1, min(limit, 500))

    stmt = select(PaperOrder).where(PaperOrder.account_id == account_id)
    if symbol:
        stmt = stmt.where(PaperOrder.symbol == symbol)
    stmt = stmt.order_by(PaperOrder.created_at.desc()).limit(limit)

    orders = (await session.scalars(stmt)).all()
    return [PaperOrderResponse.model_validate(o) for o in orders]


@router.get("/equity", response_model=List[EquitySnapshotResponse])
async def equity_history(
    account_id: int = Query(...),
    limit: int = 200,
    session: AsyncSession = Depends(get_db_session),
) -> List[EquitySnapshotResponse]:
    limit = max(1, min(limit, 2000))

    stmt = (
        select(EquitySnapshot)
        .where(EquitySnapshot.account_id == account_id)
        .order_by(EquitySnapshot.created_at.asc())
        .limit(limit)
    )
    snapshots = (await session.scalars(stmt)).all()
    return [EquitySnapshotResponse.model_validate(s) for s in snapshots]
