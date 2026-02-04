from __future__ import annotations

from typing import Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import AdminAction, PositionPolicyConfig, RiskPolicy, StrategyConfig
from ai_crypto_trader.services.paper_trader.policy_bindings import (
    bind_strategy_policy,
    create_position_policy_version,
    create_risk_policy_version,
    list_position_policy_versions,
    list_risk_policy_versions,
)

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin_token)])


class PolicyCreate(BaseModel):
    name: str = Field(..., min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    status: str = Field("active", min_length=1)
    notes: Optional[str] = None


class PolicyBindingUpdate(BaseModel):
    risk_policy_id: Optional[int] = None
    position_policy_id: Optional[UUID] = None
    notes: Optional[str] = None


def _serialize_risk_policy(policy: RiskPolicy) -> dict:
    return {
        "id": policy.id,
        "name": policy.name,
        "version": policy.version,
        "status": policy.status,
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


@router.post("/risk-policies")
async def create_risk_policy(payload: PolicyCreate, session: AsyncSession = Depends(get_db_session)) -> dict:
    policy = await create_risk_policy_version(
        session,
        name=payload.name,
        params=payload.params,
        notes=payload.notes,
        status=payload.status,
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_risk_policy(policy)}


@router.get("/risk-policies")
async def list_risk_policies(
    name: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    policies = await list_risk_policy_versions(session, name=name)
    return {"ok": True, "items": [_serialize_risk_policy(p) for p in policies]}


@router.post("/position-policies")
async def create_position_policy(payload: PolicyCreate, session: AsyncSession = Depends(get_db_session)) -> dict:
    policy = await create_position_policy_version(
        session,
        name=payload.name,
        params=payload.params,
        notes=payload.notes,
        status=payload.status,
    )
    await session.commit()
    await session.refresh(policy)
    return {"ok": True, "policy": _serialize_position_policy(policy)}


@router.get("/position-policies")
async def list_position_policies(
    name: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    policies = await list_position_policy_versions(session, name=name)
    return {"ok": True, "items": [_serialize_position_policy(p) for p in policies]}


@router.put("/strategies/{strategy_config_id}/policy-binding")
async def update_strategy_policy_binding(
    payload: PolicyBindingUpdate,
    strategy_config_id: int = Path(..., ge=1),
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    strategy = await session.get(StrategyConfig, strategy_config_id)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy config not found")

    risk_policy = None
    position_policy = None
    if payload.risk_policy_id is not None:
        risk_policy = await session.get(RiskPolicy, payload.risk_policy_id)
        if risk_policy is None:
            raise HTTPException(status_code=404, detail="Risk policy not found")
    if payload.position_policy_id is not None:
        position_policy = await session.get(PositionPolicyConfig, payload.position_policy_id)
        if position_policy is None:
            raise HTTPException(status_code=404, detail="Position policy not found")

    binding, changes = await bind_strategy_policy(
        session,
        strategy_config_id=strategy_config_id,
        risk_policy_id=payload.risk_policy_id,
        position_policy_id=payload.position_policy_id,
        notes=payload.notes,
    )

    old_meta = changes.get("old", {})
    new_meta = changes.get("new", {})
    old_risk = await session.get(RiskPolicy, old_meta.get("risk_policy_id")) if old_meta.get("risk_policy_id") else None
    old_position = await session.get(PositionPolicyConfig, old_meta.get("position_policy_id")) if old_meta.get("position_policy_id") else None

    action_meta = {
        "strategy_config_id": strategy_config_id,
        "old": {
            "risk_policy": _policy_info(old_risk),
            "position_policy": _policy_info(old_position),
        },
        "new": {
            "risk_policy": _policy_info(risk_policy),
            "position_policy": _policy_info(position_policy),
        },
        "notes": payload.notes,
    }
    session.add(
        AdminAction(
            action="POLICY_BINDING_UPDATED",
            status="ok",
            message="Policy binding updated",
            meta=action_meta,
        )
    )
    await session.commit()
    await session.refresh(binding)

    return {
        "ok": True,
        "binding": {
            "strategy_config_id": binding.strategy_config_id,
            "risk_policy_id": binding.risk_policy_id,
            "position_policy_id": str(binding.position_policy_id) if binding.position_policy_id else None,
            "notes": binding.notes,
            "updated_at": binding.updated_at.isoformat() if binding.updated_at else None,
        },
        "policies": {
            "risk_policy": _serialize_risk_policy(risk_policy) if risk_policy else None,
            "position_policy": _serialize_position_policy(position_policy) if position_policy else None,
        },
    }
