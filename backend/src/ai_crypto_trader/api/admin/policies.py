from __future__ import annotations

from typing import Any, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy import Text, and_, cast, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import (
    AdminAction,
    PositionPolicyConfig,
    RiskPolicy,
    StrategyConfig,
    StrategyPolicyBinding,
)
from ai_crypto_trader.services.admin.pagination import clamp_limit_offset, fetch_page
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.paper_trader.policy_bindings import (
    activate_position_policy_version,
    activate_risk_policy_version,
    bind_strategy_policy,
    create_position_policy_version,
    create_risk_policy_version,
    get_effective_binding_context,
    get_position_policy_by_name_version,
    get_risk_policy_by_name_version,
    get_strategy_policy_binding,
)
from ai_crypto_trader.services.policies.effective import resolve_effective_policy_snapshot

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin_token)])


class PolicyCreate(BaseModel):
    name: str = Field(..., min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    status: Literal["draft", "active", "archived"] = "draft"
    activate: bool = False
    notes: Optional[str] = None


class PolicyBindingUpdate(BaseModel):
    risk_policy_id: Optional[int] = None
    position_policy_id: Optional[UUID] = None
    notes: Optional[str] = None


class PolicyRollbackRequest(BaseModel):
    policy_type: Literal["risk", "position"]
    to_version: int = Field(..., ge=1)
    notes: Optional[str] = None


class PolicySymbolLimitsUpsert(BaseModel):
    symbol: str = Field(..., min_length=1)
    limits: Any = Field(default_factory=dict)


def _serialize_risk_policy(policy: RiskPolicy) -> dict:
    return {
        "id": policy.id,
        "name": policy.name,
        "version": policy.version,
        "status": policy.status,
        "is_active": policy.is_active,
        "params": policy.params,
        "notes": policy.notes,
        "created_at": policy.created_at.isoformat() if policy.created_at else None,
        "updated_at": policy.updated_at.isoformat() if policy.updated_at else None,
    }


def _serialize_position_policy(policy: PositionPolicyConfig) -> dict:
    return {
        "id": str(policy.id),
        "name": policy.name,
        "version": policy.version,
        "status": policy.status,
        "params": policy.params,
        "notes": policy.notes,
        "created_at": policy.created_at.isoformat() if policy.created_at else None,
        "updated_at": policy.updated_at.isoformat() if policy.updated_at else None,
    }


def _policy_info(policy: RiskPolicy | PositionPolicyConfig | None) -> dict | None:
    if policy is None:
        return None
    return {
        "id": str(policy.id),
        "name": getattr(policy, "name", None),
        "version": getattr(policy, "version", None),
        "status": getattr(policy, "status", None),
    }


def _binding_payload(
    *,
    strategy_config_id: int,
    risk_policy_id: int | None,
    position_policy_id: UUID | None,
    notes: str | None,
    updated_at: Any,
) -> dict:
    return {
        "strategy_config_id": strategy_config_id,
        "risk_policy_id": risk_policy_id,
        "position_policy_id": str(position_policy_id) if position_policy_id else None,
        "notes": notes,
        "updated_at": updated_at.isoformat() if updated_at else None,
    }


def _serialize_strategy_config(strategy: StrategyConfig) -> dict[str, Any]:
    return {
        "id": strategy.id,
        "symbols": strategy.symbols,
        "timeframe": strategy.timeframe,
        "thresholds": strategy.thresholds,
        "order_type": strategy.order_type,
        "allow_short": strategy.allow_short,
        "min_notional_usd": str(strategy.min_notional_usd) if strategy.min_notional_usd is not None else None,
        "risk_policy_id": strategy.risk_policy_id,
        "is_active": strategy.is_active,
        "created_at": strategy.created_at.isoformat() if strategy.created_at else None,
        "updated_at": strategy.updated_at.isoformat() if strategy.updated_at else None,
    }


def _normalize_symbol_or_400(symbol: str | None) -> str:
    symbol_norm = normalize_symbol((symbol or "").strip())
    if not symbol_norm:
        raise HTTPException(status_code=400, detail="symbol is required")
    return symbol_norm


def _merge_symbol_limits(params: dict[str, Any] | None, *, symbol: str, limits: dict[str, Any]) -> dict[str, Any]:
    payload = dict(params) if isinstance(params, dict) else {}
    per_symbol_raw = payload.get("per_symbol")
    per_symbol: dict[str, Any] = dict(per_symbol_raw) if isinstance(per_symbol_raw, dict) else {}
    existing = per_symbol.get(symbol)
    merged = dict(existing) if isinstance(existing, dict) else {}
    merged.update(limits)
    per_symbol[symbol] = merged
    payload["per_symbol"] = per_symbol
    return payload


async def _audit_action(
    session: AsyncSession,
    *,
    action: str,
    message: str,
    meta: dict[str, Any],
) -> None:
    session.add(
        AdminAction(
            action=action,
            status="ok",
            message=message,
            meta=meta,
        )
    )


@router.post("/risk-policies")
async def create_risk_policy(payload: PolicyCreate, session: AsyncSession = Depends(get_db_session)) -> dict:
    policy = await create_risk_policy_version(
        session,
        name=payload.name,
        params=payload.params,
        notes=payload.notes,
        status=payload.status,
        activate=payload.activate,
    )
    await _audit_action(
        session,
        action="POLICY_CREATED",
        message="Risk policy version created",
        meta={
            "strategy_config_id": None,
            "policy_type": "risk",
            "actor": "admin",
            "name": policy.name,
            "from_id": None,
            "from_version": None,
            "to_id": policy.id,
            "to_version": policy.version,
            "status": policy.status,
            "is_active": policy.is_active,
            "notes": payload.notes,
        },
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_risk_policy(policy)}


@router.get("/risk-policies")
async def list_risk_policies(
    name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: str = Query("updated_at_desc"),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    limit, offset = clamp_limit_offset(limit=limit, offset=offset)
    conditions = []
    if name:
        conditions.append(RiskPolicy.name == name)
    if status:
        conditions.append(RiskPolicy.status == status)
    if is_active is not None:
        conditions.append(RiskPolicy.is_active.is_(is_active))

    stmt = select(RiskPolicy)
    count_stmt = select(func.count()).select_from(RiskPolicy)
    if conditions:
        stmt = stmt.where(and_(*conditions))
        count_stmt = count_stmt.where(and_(*conditions))

    if order == "updated_at_desc":
        stmt = stmt.order_by(desc(RiskPolicy.updated_at), desc(RiskPolicy.version), desc(RiskPolicy.id))
    elif order == "updated_at_asc":
        stmt = stmt.order_by(RiskPolicy.updated_at.asc(), RiskPolicy.version.asc(), RiskPolicy.id.asc())
    elif order == "created_at_desc":
        stmt = stmt.order_by(desc(RiskPolicy.created_at), desc(RiskPolicy.version), desc(RiskPolicy.id))
    elif order == "created_at_asc":
        stmt = stmt.order_by(RiskPolicy.created_at.asc(), RiskPolicy.version.asc(), RiskPolicy.id.asc())
    elif order == "version_desc":
        stmt = stmt.order_by(desc(RiskPolicy.version), desc(RiskPolicy.id))
    else:
        raise HTTPException(status_code=400, detail="Unsupported order")

    total, items = await fetch_page(
        session,
        count_stmt=count_stmt,
        data_stmt=stmt,
        limit=limit,
        offset=offset,
    )
    return {
        "ok": True,
        "items": [_serialize_risk_policy(p) for p in items],
        "limit": limit,
        "offset": offset,
        "total": total,
    }


@router.post("/risk-policies/{policy_id}/activate")
async def activate_risk_policy(
    policy_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    try:
        policy, previous_active = await activate_risk_policy_version(session, policy_id=policy_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    await _audit_action(
        session,
        action="POLICY_ACTIVATED",
        message="Risk policy activated",
        meta={
            "strategy_config_id": None,
            "policy_type": "risk",
            "actor": "admin",
            "name": policy.name,
            "from_id": previous_active.id if previous_active else None,
            "from_version": previous_active.version if previous_active else None,
            "to_id": policy.id,
            "to_version": policy.version,
            "notes": None,
        },
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_risk_policy(policy)}


@router.post("/risk-policies/{policy_id}/upsert-symbol-limits")
async def upsert_risk_policy_symbol_limits(
    payload: PolicySymbolLimitsUpsert,
    policy_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    policy = await session.get(RiskPolicy, policy_id)
    if policy is None:
        raise HTTPException(status_code=404, detail="Risk policy not found")
    if not isinstance(payload.limits, dict):
        raise HTTPException(status_code=400, detail="limits must be an object")
    symbol = _normalize_symbol_or_400(payload.symbol)

    policy.params = _merge_symbol_limits(
        policy.params if isinstance(policy.params, dict) else {},
        symbol=symbol,
        limits=payload.limits,
    )
    await _audit_action(
        session,
        action="POLICY_SYMBOL_LIMITS_UPSERTED",
        message="Risk policy symbol limits upserted",
        meta={
            "actor": "admin",
            "policy_type": "risk",
            "policy_id": policy.id,
            "policy_name": policy.name,
            "policy_version": policy.version,
            "symbol": symbol,
            "limits": payload.limits,
        },
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_risk_policy(policy)}


@router.delete("/risk-policies/{policy_id}/delete-symbol-limits")
async def delete_risk_policy_symbol_limits(
    policy_id: int = Path(..., ge=1),
    symbol: str = Query(..., min_length=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    policy = await session.get(RiskPolicy, policy_id)
    if policy is None:
        raise HTTPException(status_code=404, detail="Risk policy not found")
    symbol_norm = _normalize_symbol_or_400(symbol)

    params = dict(policy.params) if isinstance(policy.params, dict) else {}
    per_symbol_raw = params.get("per_symbol")
    per_symbol = dict(per_symbol_raw) if isinstance(per_symbol_raw, dict) else {}
    removed = per_symbol.pop(symbol_norm, None)
    params["per_symbol"] = per_symbol
    policy.params = params

    await _audit_action(
        session,
        action="POLICY_SYMBOL_LIMITS_DELETED",
        message="Risk policy symbol limits deleted",
        meta={
            "actor": "admin",
            "policy_type": "risk",
            "policy_id": policy.id,
            "policy_name": policy.name,
            "policy_version": policy.version,
            "symbol": symbol_norm,
            "deleted": True,
            "previous_limits": removed,
        },
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_risk_policy(policy)}


@router.post("/position-policies")
async def create_position_policy(payload: PolicyCreate, session: AsyncSession = Depends(get_db_session)) -> dict:
    policy = await create_position_policy_version(
        session,
        name=payload.name,
        params=payload.params,
        notes=payload.notes,
        status=payload.status,
    )
    await _audit_action(
        session,
        action="POLICY_CREATED",
        message="Position policy version created",
        meta={
            "strategy_config_id": None,
            "policy_type": "position",
            "actor": "admin",
            "name": policy.name,
            "from_id": None,
            "from_version": None,
            "to_id": str(policy.id),
            "to_version": policy.version,
            "status": policy.status,
            "notes": payload.notes,
        },
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_position_policy(policy)}


@router.get("/position-policies")
async def list_position_policies(
    name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: str = Query("updated_at_desc"),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    limit, offset = clamp_limit_offset(limit=limit, offset=offset)
    conditions = []
    if name:
        conditions.append(PositionPolicyConfig.name == name)
    if status:
        conditions.append(PositionPolicyConfig.status == status)

    stmt = select(PositionPolicyConfig)
    count_stmt = select(func.count()).select_from(PositionPolicyConfig)
    if conditions:
        stmt = stmt.where(and_(*conditions))
        count_stmt = count_stmt.where(and_(*conditions))

    if order == "updated_at_desc":
        stmt = stmt.order_by(
            desc(PositionPolicyConfig.updated_at),
            desc(PositionPolicyConfig.version),
            desc(PositionPolicyConfig.id),
        )
    elif order == "updated_at_asc":
        stmt = stmt.order_by(
            PositionPolicyConfig.updated_at.asc(),
            PositionPolicyConfig.version.asc(),
            PositionPolicyConfig.id.asc(),
        )
    elif order == "created_at_desc":
        stmt = stmt.order_by(
            desc(PositionPolicyConfig.created_at),
            desc(PositionPolicyConfig.version),
            desc(PositionPolicyConfig.id),
        )
    elif order == "created_at_asc":
        stmt = stmt.order_by(
            PositionPolicyConfig.created_at.asc(),
            PositionPolicyConfig.version.asc(),
            PositionPolicyConfig.id.asc(),
        )
    elif order == "version_desc":
        stmt = stmt.order_by(desc(PositionPolicyConfig.version), desc(PositionPolicyConfig.id))
    else:
        raise HTTPException(status_code=400, detail="Unsupported order")

    total, items = await fetch_page(
        session,
        count_stmt=count_stmt,
        data_stmt=stmt,
        limit=limit,
        offset=offset,
    )
    return {
        "ok": True,
        "items": [_serialize_position_policy(p) for p in items],
        "limit": limit,
        "offset": offset,
        "total": total,
    }


@router.post("/position-policies/{policy_id}/activate")
async def activate_position_policy(
    policy_id: UUID,
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    try:
        policy, previous_active = await activate_position_policy_version(session, policy_id=policy_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    await _audit_action(
        session,
        action="POLICY_ACTIVATED",
        message="Position policy activated",
        meta={
            "strategy_config_id": None,
            "policy_type": "position",
            "actor": "admin",
            "name": policy.name,
            "from_id": str(previous_active.id) if previous_active else None,
            "from_version": previous_active.version if previous_active else None,
            "to_id": str(policy.id),
            "to_version": policy.version,
            "notes": None,
        },
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_position_policy(policy)}


@router.post("/position-policies/{policy_id}/upsert-symbol-limits")
async def upsert_position_policy_symbol_limits(
    payload: PolicySymbolLimitsUpsert,
    policy_id: UUID,
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    policy = await session.get(PositionPolicyConfig, policy_id)
    if policy is None:
        raise HTTPException(status_code=404, detail="Position policy not found")
    if not isinstance(payload.limits, dict):
        raise HTTPException(status_code=400, detail="limits must be an object")
    symbol = _normalize_symbol_or_400(payload.symbol)

    policy.params = _merge_symbol_limits(
        policy.params if isinstance(policy.params, dict) else {},
        symbol=symbol,
        limits=payload.limits,
    )
    await _audit_action(
        session,
        action="POLICY_SYMBOL_LIMITS_UPSERTED",
        message="Position policy symbol limits upserted",
        meta={
            "actor": "admin",
            "policy_type": "position",
            "policy_id": str(policy.id),
            "policy_name": policy.name,
            "policy_version": policy.version,
            "symbol": symbol,
            "limits": payload.limits,
        },
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_position_policy(policy)}


@router.delete("/position-policies/{policy_id}/delete-symbol-limits")
async def delete_position_policy_symbol_limits(
    policy_id: UUID,
    symbol: str = Query(..., min_length=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    policy = await session.get(PositionPolicyConfig, policy_id)
    if policy is None:
        raise HTTPException(status_code=404, detail="Position policy not found")
    symbol_norm = _normalize_symbol_or_400(symbol)

    params = dict(policy.params) if isinstance(policy.params, dict) else {}
    per_symbol_raw = params.get("per_symbol")
    per_symbol = dict(per_symbol_raw) if isinstance(per_symbol_raw, dict) else {}
    removed = per_symbol.pop(symbol_norm, None)
    params["per_symbol"] = per_symbol
    policy.params = params

    await _audit_action(
        session,
        action="POLICY_SYMBOL_LIMITS_DELETED",
        message="Position policy symbol limits deleted",
        meta={
            "actor": "admin",
            "policy_type": "position",
            "policy_id": str(policy.id),
            "policy_name": policy.name,
            "policy_version": policy.version,
            "symbol": symbol_norm,
            "deleted": True,
            "previous_limits": removed,
        },
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_position_policy(policy)}


@router.get("/strategy-configs")
async def list_strategy_configs(
    is_active: Optional[bool] = Query(None),
    symbol: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: str = Query("updated_at_desc"),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    limit, offset = clamp_limit_offset(limit=limit, offset=offset)
    conditions = []
    if is_active is not None:
        conditions.append(StrategyConfig.is_active.is_(is_active))
    symbol_norm = None
    if symbol:
        symbol_norm = _normalize_symbol_or_400(symbol)
        conditions.append(cast(StrategyConfig.symbols, Text).ilike(f'%"{symbol_norm}"%'))

    count_stmt = select(func.count()).select_from(StrategyConfig)
    if conditions:
        count_stmt = count_stmt.where(and_(*conditions))
    total = int((await session.execute(count_stmt)).scalar_one() or 0)

    stmt = (
        select(StrategyConfig, StrategyPolicyBinding)
        .outerjoin(StrategyPolicyBinding, StrategyPolicyBinding.strategy_config_id == StrategyConfig.id)
    )
    if conditions:
        stmt = stmt.where(and_(*conditions))
    if order == "updated_at_desc":
        stmt = stmt.order_by(desc(StrategyConfig.updated_at), desc(StrategyConfig.id))
    elif order == "updated_at_asc":
        stmt = stmt.order_by(StrategyConfig.updated_at.asc(), StrategyConfig.id.asc())
    elif order == "created_at_desc":
        stmt = stmt.order_by(desc(StrategyConfig.created_at), desc(StrategyConfig.id))
    elif order == "created_at_asc":
        stmt = stmt.order_by(StrategyConfig.created_at.asc(), StrategyConfig.id.asc())
    else:
        raise HTTPException(status_code=400, detail="Unsupported order")

    rows = await session.execute(stmt.limit(limit).offset(offset))
    items = []
    for strategy, binding in rows.all():
        items.append(
            {
                "strategy_config": _serialize_strategy_config(strategy),
                "binding": _binding_payload(
                    strategy_config_id=strategy.id,
                    risk_policy_id=binding.risk_policy_id if binding else None,
                    position_policy_id=binding.position_policy_id if binding else None,
                    notes=binding.notes if binding else None,
                    updated_at=binding.updated_at if binding else None,
                )
                if binding
                else None,
            }
        )
    return {
        "ok": True,
        "items": items,
        "limit": limit,
        "offset": offset,
        "total": total,
        "symbol_filter": symbol_norm,
    }


@router.get("/strategy-policy-bindings")
async def list_strategy_policy_bindings(
    strategy_config_id: Optional[int] = Query(None, ge=1),
    risk_policy_id: Optional[int] = Query(None, ge=1),
    position_policy_id: Optional[UUID] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    order: str = Query("updated_at_desc"),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    limit, offset = clamp_limit_offset(limit=limit, offset=offset)
    conditions = []
    if strategy_config_id is not None:
        conditions.append(StrategyPolicyBinding.strategy_config_id == strategy_config_id)
    if risk_policy_id is not None:
        conditions.append(StrategyPolicyBinding.risk_policy_id == risk_policy_id)
    if position_policy_id is not None:
        conditions.append(StrategyPolicyBinding.position_policy_id == position_policy_id)

    count_stmt = select(func.count()).select_from(StrategyPolicyBinding)
    if conditions:
        count_stmt = count_stmt.where(and_(*conditions))
    total = int((await session.execute(count_stmt)).scalar_one() or 0)

    stmt = select(StrategyPolicyBinding)
    if conditions:
        stmt = stmt.where(and_(*conditions))
    if order == "updated_at_desc":
        stmt = stmt.order_by(desc(StrategyPolicyBinding.updated_at), desc(StrategyPolicyBinding.strategy_config_id))
    elif order == "updated_at_asc":
        stmt = stmt.order_by(StrategyPolicyBinding.updated_at.asc(), StrategyPolicyBinding.strategy_config_id.asc())
    elif order == "created_at_desc":
        stmt = stmt.order_by(desc(StrategyPolicyBinding.updated_at), desc(StrategyPolicyBinding.strategy_config_id))
    elif order == "created_at_asc":
        stmt = stmt.order_by(StrategyPolicyBinding.updated_at.asc(), StrategyPolicyBinding.strategy_config_id.asc())
    else:
        raise HTTPException(status_code=400, detail="Unsupported order")

    rows = await session.execute(stmt.limit(limit).offset(offset))
    items = [
        _binding_payload(
            strategy_config_id=row.strategy_config_id,
            risk_policy_id=row.risk_policy_id,
            position_policy_id=row.position_policy_id,
            notes=row.notes,
            updated_at=row.updated_at,
        )
        for row in rows.scalars().all()
    ]
    return {"ok": True, "items": items, "limit": limit, "offset": offset, "total": total}


@router.get("/strategy-configs/{strategy_config_id}/effective-policies")
async def get_effective_policies_for_strategy(
    strategy_config_id: int = Path(..., ge=1),
    symbol: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    strategy = await session.get(StrategyConfig, strategy_config_id)
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy config not found")

    symbol_norm = _normalize_symbol_or_400(symbol) if symbol is not None else None
    snapshot = None
    if symbol_norm is not None:
        snapshot = await resolve_effective_policy_snapshot(
            session,
            strategy_config_id=strategy_config_id,
            symbol=symbol_norm,
        )
        if snapshot is None:
            raise HTTPException(status_code=404, detail="Strategy config not found")

    binding = await get_strategy_policy_binding(session, strategy_config_id=strategy_config_id)
    if binding and (binding.risk_policy_id is not None or binding.position_policy_id is not None):
        policy_source = "binding"
    elif strategy.risk_policy_id is not None:
        policy_source = "legacy"
    else:
        policy_source = "default"

    policy_binding = {
        "strategy_config_id": strategy_config_id,
        "legacy_risk_policy_id": strategy.risk_policy_id,
        "bound_risk_policy_id": binding.risk_policy_id if binding else None,
        "effective_risk_policy_id": (binding.risk_policy_id if binding and binding.risk_policy_id is not None else strategy.risk_policy_id),
        "bound_position_policy_id": str(binding.position_policy_id) if binding and binding.position_policy_id else None,
        "effective_position_policy_id": str(binding.position_policy_id) if binding and binding.position_policy_id else None,
    }
    if snapshot is not None:
        policy_binding = snapshot.policy_binding
        if snapshot.policy_source == "binding":
            policy_source = "binding"
        elif snapshot.policy_source == "strategy_legacy":
            policy_source = "legacy"
        elif snapshot.policy_source == "account_default":
            policy_source = "default"

    computed_limits = snapshot.computed_limits if snapshot is not None else {
        "risk_limits": {
            "max_order_notional_usdt": None,
            "min_order_notional_usdt": None,
            "max_leverage": None,
            "max_drawdown_pct": None,
            "max_drawdown_usdt": None,
            "max_daily_loss_usdt": None,
            "equity_lookback_hours": None,
            "lookback_minutes": None,
        },
        "risk_limit_source": None,
        "risk_policy_overrides": None,
        "symbol_limits": {
            "max_order_qty": None,
            "max_position_qty": None,
            "max_position_notional_usdt": None,
            "max_position_pct_equity": None,
        },
        "symbol_limit_source": None,
        "position_policy_overrides": None,
    }

    return {
        "ok": True,
        "policy_source": policy_source,
        "strategy_config": _serialize_strategy_config(strategy),
        "policy_binding": policy_binding,
        "symbol": symbol_norm,
        "computed_limits": computed_limits,
    }


async def _bind_policies_impl(
    *,
    payload: PolicyBindingUpdate,
    strategy_config_id: int,
    session: AsyncSession,
) -> dict:
    strategy = await session.get(StrategyConfig, strategy_config_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy config not found")

    existing = await get_strategy_policy_binding(session, strategy_config_id=strategy_config_id)
    fields_set = set(
        getattr(payload, "model_fields_set", None)
        or getattr(payload, "__fields_set__", set())
    )

    risk_policy_id = (
        payload.risk_policy_id
        if "risk_policy_id" in fields_set
        else (existing.risk_policy_id if existing else None)
    )
    position_policy_id = (
        payload.position_policy_id
        if "position_policy_id" in fields_set
        else (existing.position_policy_id if existing else None)
    )
    notes = payload.notes if "notes" in fields_set else (existing.notes if existing else None)

    risk_policy = None
    position_policy = None
    if risk_policy_id is not None:
        risk_policy = await session.get(RiskPolicy, risk_policy_id)
        if risk_policy is None:
            raise HTTPException(status_code=404, detail="Risk policy not found")
    if position_policy_id is not None:
        position_policy = await session.get(PositionPolicyConfig, position_policy_id)
        if position_policy is None:
            raise HTTPException(status_code=404, detail="Position policy not found")

    old_risk = await session.get(RiskPolicy, existing.risk_policy_id) if existing and existing.risk_policy_id else None
    old_position = (
        await session.get(PositionPolicyConfig, existing.position_policy_id)
        if existing and existing.position_policy_id
        else None
    )

    binding, _ = await bind_strategy_policy(
        session,
        strategy_config_id=strategy_config_id,
        risk_policy_id=risk_policy_id,
        position_policy_id=position_policy_id,
        notes=notes,
    )

    await _audit_action(
        session,
        action="STRATEGY_POLICY_BOUND",
        message="Strategy policy binding updated",
        meta={
            "strategy_config_id": strategy_config_id,
            "policy_type": "risk+position",
            "actor": "admin",
            "name": {
                "risk": risk_policy.name if risk_policy else (old_risk.name if old_risk else None),
                "position": position_policy.name if position_policy else (old_position.name if old_position else None),
            },
            "notes": notes,
            "from_id": {
                "risk": old_risk.id if old_risk else None,
                "position": str(old_position.id) if old_position else None,
            },
            "from_version": {
                "risk": old_risk.version if old_risk else None,
                "position": old_position.version if old_position else None,
            },
            "to_id": {
                "risk": risk_policy.id if risk_policy else None,
                "position": str(position_policy.id) if position_policy else None,
            },
            "to_version": {
                "risk": risk_policy.version if risk_policy else None,
                "position": position_policy.version if position_policy else None,
            },
            "risk": {
                "from_id": old_risk.id if old_risk else None,
                "from_version": old_risk.version if old_risk else None,
                "to_id": risk_policy.id if risk_policy else None,
                "to_version": risk_policy.version if risk_policy else None,
            },
            "position": {
                "from_id": str(old_position.id) if old_position else None,
                "from_version": old_position.version if old_position else None,
                "to_id": str(position_policy.id) if position_policy else None,
                "to_version": position_policy.version if position_policy else None,
            },
        },
    )

    await session.commit()
    await session.refresh(binding)
    return {
        "ok": True,
        "binding": _binding_payload(
            strategy_config_id=binding.strategy_config_id,
            risk_policy_id=binding.risk_policy_id,
            position_policy_id=binding.position_policy_id,
            notes=binding.notes,
            updated_at=binding.updated_at,
        ),
        "policies": {
            "risk_policy": _serialize_risk_policy(risk_policy) if risk_policy else None,
            "position_policy": _serialize_position_policy(position_policy) if position_policy else None,
        },
    }


@router.post("/strategy-configs/{strategy_config_id}/bind-policies")
async def bind_policies(
    payload: PolicyBindingUpdate,
    strategy_config_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    return await _bind_policies_impl(
        payload=payload,
        strategy_config_id=strategy_config_id,
        session=session,
    )


@router.put("/strategies/{strategy_config_id}/policy-binding")
async def update_strategy_policy_binding(
    payload: PolicyBindingUpdate,
    strategy_config_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    # Backward-compatible alias for existing clients.
    return await _bind_policies_impl(
        payload=payload,
        strategy_config_id=strategy_config_id,
        session=session,
    )


@router.post("/strategy-configs/{strategy_config_id}/rollback")
async def rollback_strategy_policy(
    payload: PolicyRollbackRequest,
    strategy_config_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    strategy, binding = await get_effective_binding_context(
        session,
        strategy_config_id=strategy_config_id,
    )
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy config not found")

    policy_type = payload.policy_type
    notes = payload.notes

    if policy_type == "risk":
        current_risk_id = binding.risk_policy_id if binding and binding.risk_policy_id is not None else strategy.risk_policy_id
        if current_risk_id is None:
            raise HTTPException(status_code=400, detail="Cannot resolve current risk policy name")
        from_policy = await session.get(RiskPolicy, current_risk_id)
        if from_policy is None:
            raise HTTPException(status_code=400, detail="Cannot resolve current risk policy name")
        to_policy = await get_risk_policy_by_name_version(
            session,
            name=from_policy.name,
            version=payload.to_version,
        )
        if to_policy is None:
            raise HTTPException(status_code=400, detail="Target risk policy version not found")

        binding_row, _ = await bind_strategy_policy(
            session,
            strategy_config_id=strategy_config_id,
            risk_policy_id=to_policy.id,
            position_policy_id=binding.position_policy_id if binding else None,
            notes=notes,
        )
        await _audit_action(
            session,
            action="STRATEGY_POLICY_ROLLBACK",
            message="Strategy risk policy rolled back",
            meta={
                "strategy_config_id": strategy_config_id,
                "policy_type": "risk",
                "actor": "admin",
                "name": from_policy.name,
                "notes": notes,
                "from_id": from_policy.id,
                "from_version": from_policy.version,
                "to_id": to_policy.id,
                "to_version": to_policy.version,
            },
        )
        await session.commit()
        await session.refresh(binding_row)
        return {
            "ok": True,
            "binding": _binding_payload(
                strategy_config_id=binding_row.strategy_config_id,
                risk_policy_id=binding_row.risk_policy_id,
                position_policy_id=binding_row.position_policy_id,
                notes=binding_row.notes,
                updated_at=binding_row.updated_at,
            ),
            "from_policy": _policy_info(from_policy),
            "to_policy": _policy_info(to_policy),
        }

    if binding is None or binding.position_policy_id is None:
        raise HTTPException(status_code=400, detail="Cannot resolve current position policy name")
    from_policy = await session.get(PositionPolicyConfig, binding.position_policy_id)
    if from_policy is None:
        raise HTTPException(status_code=400, detail="Cannot resolve current position policy name")
    to_policy = await get_position_policy_by_name_version(
        session,
        name=from_policy.name,
        version=payload.to_version,
    )
    if to_policy is None:
        raise HTTPException(status_code=400, detail="Target position policy version not found")

    binding_row, _ = await bind_strategy_policy(
        session,
        strategy_config_id=strategy_config_id,
        risk_policy_id=binding.risk_policy_id if binding else None,
        position_policy_id=to_policy.id,
        notes=notes,
    )
    await _audit_action(
        session,
        action="STRATEGY_POLICY_ROLLBACK",
        message="Strategy position policy rolled back",
        meta={
            "strategy_config_id": strategy_config_id,
            "policy_type": "position",
            "actor": "admin",
            "name": from_policy.name,
            "notes": notes,
            "from_id": str(from_policy.id),
            "from_version": from_policy.version,
            "to_id": str(to_policy.id),
            "to_version": to_policy.version,
        },
    )
    await session.commit()
    await session.refresh(binding_row)
    return {
        "ok": True,
        "binding": _binding_payload(
            strategy_config_id=binding_row.strategy_config_id,
            risk_policy_id=binding_row.risk_policy_id,
            position_policy_id=binding_row.position_policy_id,
            notes=binding_row.notes,
            updated_at=binding_row.updated_at,
        ),
        "from_policy": _policy_info(from_policy),
        "to_policy": _policy_info(to_policy),
    }
