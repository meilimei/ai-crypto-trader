from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, Field, validator
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import RiskPolicy, StrategyConfig
from ai_crypto_trader.services.paper_trader.config import ActiveConfigMissingError, get_active_bundle

router = APIRouter(prefix="/admin/config", tags=["admin"], dependencies=[Depends(require_admin_token)])


def _dec(value: Decimal | float | int) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def serialize_risk_policy(policy: RiskPolicy) -> Dict[str, Any]:
    return {
        "id": policy.id,
        "name": getattr(policy, "name", None),
        "version": getattr(policy, "version", None),
        "status": getattr(policy, "status", None),
        "params": getattr(policy, "params", None),
        "notes": getattr(policy, "notes", None),
        "max_loss_per_trade_usd": str(policy.max_loss_per_trade_usd),
        "max_loss_per_day_usd": str(policy.max_loss_per_day_usd),
        "max_position_usd": str(policy.max_position_usd),
        "max_open_positions": policy.max_open_positions,
        "cooldown_seconds": policy.cooldown_seconds,
        "fee_bps": str(policy.fee_bps),
        "slippage_bps": str(policy.slippage_bps),
        "is_active": policy.is_active,
        "created_at": policy.created_at.isoformat() if policy.created_at else None,
        "updated_at": policy.updated_at.isoformat() if policy.updated_at else None,
    }


def serialize_strategy_config(strategy: StrategyConfig) -> Dict[str, Any]:
    return {
        "id": strategy.id,
        "symbols": strategy.symbols,
        "timeframe": strategy.timeframe,
        "thresholds": strategy.thresholds,
        "order_type": strategy.order_type,
        "allow_short": strategy.allow_short,
        "min_notional_usd": str(strategy.min_notional_usd),
        "risk_policy_id": strategy.risk_policy_id,
        "is_active": strategy.is_active,
        "created_at": strategy.created_at.isoformat() if strategy.created_at else None,
        "updated_at": strategy.updated_at.isoformat() if strategy.updated_at else None,
    }


class RiskPolicyCreate(BaseModel):
    max_loss_per_trade_usd: Decimal = Field(..., gt=0)
    max_loss_per_day_usd: Decimal = Field(..., gt=0)
    max_position_usd: Decimal = Field(..., gt=0)
    max_open_positions: int = Field(..., ge=1)
    cooldown_seconds: int = Field(0, ge=0)
    fee_bps: Decimal = Field(0, ge=0)
    slippage_bps: Decimal = Field(0, ge=0)
    is_active: bool = False

    @validator("fee_bps", "slippage_bps", pre=True)
    def cast_decimal(cls, v: Any) -> Decimal:
        return _dec(v)

    @validator("max_loss_per_trade_usd", "max_loss_per_day_usd", "max_position_usd", pre=True)
    def cast_required_decimal(cls, v: Any) -> Decimal:
        return _dec(v)


class StrategyConfigCreate(BaseModel):
    symbols: List[str] = Field(..., min_items=1)
    timeframe: str = Field(..., min_length=1)
    thresholds: Dict[str, Any] = Field(default_factory=dict)
    order_type: str = Field("market", min_length=1)
    allow_short: bool = False
    min_notional_usd: Decimal = Field(0, ge=0)
    risk_policy_id: Optional[int] = None
    is_active: bool = False

    @validator("min_notional_usd", pre=True)
    def cast_min_notional(cls, v: Any) -> Decimal:
        return _dec(v)

    @validator("symbols")
    def strip_symbols(cls, v: List[str]) -> List[str]:
        return [s.strip() for s in v if s.strip()]


@router.get("/active")
async def get_active_config(session: AsyncSession = Depends(get_db_session)) -> dict:
    try:
        return await get_active_bundle(session)
    except ActiveConfigMissingError as e:
        raise HTTPException(status_code=409, detail=e.reason)


@router.post("/risk-policies")
async def create_risk_policy(payload: RiskPolicyCreate, session: AsyncSession = Depends(get_db_session)) -> dict:
    name = "legacy"
    max_version = await session.scalar(select(func.max(RiskPolicy.version)).where(RiskPolicy.name == name))
    version = int(max_version or 0) + 1
    status = "active" if payload.is_active else "draft"
    policy = RiskPolicy(
        name=name,
        version=version,
        status=status,
        params={},
        notes=None,
        max_loss_per_trade_usd=_dec(payload.max_loss_per_trade_usd),
        max_loss_per_day_usd=_dec(payload.max_loss_per_day_usd),
        max_position_usd=_dec(payload.max_position_usd),
        max_open_positions=payload.max_open_positions,
        cooldown_seconds=payload.cooldown_seconds,
        fee_bps=_dec(payload.fee_bps),
        slippage_bps=_dec(payload.slippage_bps),
        is_active=payload.is_active,
    )
    session.add(policy)
    await session.commit()
    await session.refresh(policy)
    return {"risk_policy": serialize_risk_policy(policy)}


@router.post("/strategy-configs")
async def create_strategy_config(payload: StrategyConfigCreate, session: AsyncSession = Depends(get_db_session)) -> dict:
    if payload.risk_policy_id:
        risk = await session.get(RiskPolicy, payload.risk_policy_id)
        if not risk:
            raise HTTPException(status_code=404, detail="Risk policy not found")
    strategy = StrategyConfig(
        symbols=payload.symbols,
        timeframe=payload.timeframe,
        thresholds=payload.thresholds,
        order_type=payload.order_type,
        allow_short=payload.allow_short,
        min_notional_usd=_dec(payload.min_notional_usd),
        risk_policy_id=payload.risk_policy_id,
        is_active=payload.is_active,
    )
    session.add(strategy)
    await session.commit()
    await session.refresh(strategy)
    return {"strategy_config": serialize_strategy_config(strategy)}


@router.put("/strategy-configs/{config_id}/activate")
async def activate_strategy_config(
    config_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    strategy = await session.get(StrategyConfig, config_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy config not found")

    await session.execute(update(StrategyConfig).values(is_active=False))
    strategy.is_active = True
    await session.commit()
    await session.refresh(strategy)
    await session.refresh(strategy, attribute_names=["risk_policy"])

    return {
        "strategy_config": serialize_strategy_config(strategy),
        "risk_policy": serialize_risk_policy(strategy.risk_policy) if strategy.risk_policy else None,
    }


@router.put("/risk-policies/{policy_id}/activate")
async def activate_risk_policy(
    policy_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    policy = await session.get(RiskPolicy, policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail="Risk policy not found")

    await session.execute(update(RiskPolicy).values(is_active=False))
    policy.is_active = True
    if getattr(policy, "status", None):
        policy.status = "active"
    await session.commit()
    await session.refresh(policy)

    return {"risk_policy": serialize_risk_policy(policy)}
