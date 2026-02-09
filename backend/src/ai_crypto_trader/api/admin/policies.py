from __future__ import annotations

from typing import Any, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.api.admin_paper_trader import require_admin_token
from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import AdminAction, PositionPolicyConfig, RiskPolicy, StrategyConfig
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
    list_position_policy_versions,
    list_risk_policy_versions,
)

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin_token)])


class PolicyCreate(BaseModel):
    name: str = Field(..., min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    status: str = Field("draft", min_length=1)
    notes: Optional[str] = None


class PolicyBindingUpdate(BaseModel):
    risk_policy_id: Optional[int] = None
    position_policy_id: Optional[UUID] = None
    notes: Optional[str] = None


class PolicyRollbackRequest(BaseModel):
    policy_type: Literal["risk", "position"]
    to_version: int = Field(..., ge=1)
    notes: Optional[str] = None


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
            "notes": payload.notes,
        },
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
    session: AsyncSession = Depends(get_db_session),
) -> dict:
    policies = await list_position_policy_versions(session, name=name)
    return {"ok": True, "items": [_serialize_position_policy(p) for p in policies]}


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
    fields_set = set(getattr(payload, "__fields_set__", set()))

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
            raise HTTPException(status_code=409, detail="No effective risk policy to roll back")
        from_policy = await session.get(RiskPolicy, current_risk_id)
        if from_policy is None:
            raise HTTPException(status_code=404, detail="Current risk policy not found")
        to_policy = await get_risk_policy_by_name_version(
            session,
            name=from_policy.name,
            version=payload.to_version,
        )
        if to_policy is None:
            raise HTTPException(status_code=404, detail="Target risk policy version not found")

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
        raise HTTPException(status_code=409, detail="No bound position policy to roll back")
    from_policy = await session.get(PositionPolicyConfig, binding.position_policy_id)
    if from_policy is None:
        raise HTTPException(status_code=404, detail="Current position policy not found")
    to_policy = await get_position_policy_by_name_version(
        session,
        name=from_policy.name,
        version=payload.to_version,
    )
    if to_policy is None:
        raise HTTPException(status_code=404, detail="Target position policy version not found")

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
