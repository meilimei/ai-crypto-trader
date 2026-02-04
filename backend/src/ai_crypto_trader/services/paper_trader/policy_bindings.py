from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import PositionPolicyConfig, RiskPolicy, StrategyPolicyBinding


@dataclass(frozen=True)
class BoundPolicies:
    risk_policy: RiskPolicy | None
    position_policy: PositionPolicyConfig | None
    source: str


def _safe_params(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


async def create_risk_policy_version(
    session: AsyncSession,
    *,
    name: str,
    params: dict | None = None,
    notes: str | None = None,
    status: str = "active",
) -> RiskPolicy:
    max_version = await session.scalar(
        select(func.max(RiskPolicy.version)).where(RiskPolicy.name == name)
    )
    version = int(max_version or 0) + 1
    policy = RiskPolicy(
        name=name,
        version=version,
        status=status,
        params=_safe_params(params),
        notes=notes,
        max_loss_per_trade_usd=Decimal("0"),
        max_loss_per_day_usd=Decimal("0"),
        max_position_usd=Decimal("0"),
        max_open_positions=0,
        cooldown_seconds=0,
        fee_bps=Decimal("0"),
        slippage_bps=Decimal("0"),
        is_active=status.lower() == "active",
    )
    session.add(policy)
    await session.flush()
    return policy


async def list_risk_policy_versions(
    session: AsyncSession,
    *,
    name: str | None = None,
) -> list[RiskPolicy]:
    stmt = select(RiskPolicy).order_by(RiskPolicy.name.asc(), RiskPolicy.version.desc())
    if name:
        stmt = stmt.where(RiskPolicy.name == name)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def create_position_policy_version(
    session: AsyncSession,
    *,
    name: str,
    params: dict | None = None,
    notes: str | None = None,
    status: str = "active",
) -> PositionPolicyConfig:
    max_version = await session.scalar(
        select(func.max(PositionPolicyConfig.version)).where(PositionPolicyConfig.name == name)
    )
    version = int(max_version or 0) + 1
    policy = PositionPolicyConfig(
        name=name,
        version=version,
        status=status,
        params=_safe_params(params),
        notes=notes,
    )
    session.add(policy)
    await session.flush()
    return policy


async def list_position_policy_versions(
    session: AsyncSession,
    *,
    name: str | None = None,
) -> list[PositionPolicyConfig]:
    stmt = select(PositionPolicyConfig).order_by(
        PositionPolicyConfig.name.asc(), PositionPolicyConfig.version.desc()
    )
    if name:
        stmt = stmt.where(PositionPolicyConfig.name == name)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_strategy_policy_binding(
    session: AsyncSession,
    *,
    strategy_config_id: int,
) -> StrategyPolicyBinding | None:
    return await session.get(StrategyPolicyBinding, strategy_config_id)


async def bind_strategy_policy(
    session: AsyncSession,
    *,
    strategy_config_id: int,
    risk_policy_id: int | None,
    position_policy_id: object | None,
    notes: str | None = None,
) -> tuple[StrategyPolicyBinding, dict[str, Any]]:
    existing = await session.get(StrategyPolicyBinding, strategy_config_id)
    old_meta = {
        "risk_policy_id": existing.risk_policy_id if existing else None,
        "position_policy_id": existing.position_policy_id if existing else None,
    }

    if existing:
        existing.risk_policy_id = risk_policy_id
        existing.position_policy_id = position_policy_id
        existing.notes = notes
        binding = existing
    else:
        binding = StrategyPolicyBinding(
            strategy_config_id=strategy_config_id,
            risk_policy_id=risk_policy_id,
            position_policy_id=position_policy_id,
            notes=notes,
        )
        session.add(binding)
    await session.flush()
    new_meta = {
        "risk_policy_id": binding.risk_policy_id,
        "position_policy_id": binding.position_policy_id,
    }
    return binding, {"old": old_meta, "new": new_meta}


async def get_bound_policies(
    session: AsyncSession,
    *,
    strategy_config_id: int,
) -> BoundPolicies:
    binding = await get_strategy_policy_binding(session, strategy_config_id=strategy_config_id)
    if not binding:
        return BoundPolicies(risk_policy=None, position_policy=None, source="none")

    risk_policy = None
    position_policy = None
    if binding.risk_policy_id is not None:
        risk_policy = await session.get(RiskPolicy, binding.risk_policy_id)
    if binding.position_policy_id is not None:
        position_policy = await session.get(PositionPolicyConfig, binding.position_policy_id)

    source = "binding"
    if risk_policy is None and position_policy is None:
        source = "none"
    return BoundPolicies(
        risk_policy=risk_policy,
        position_policy=position_policy,
        source=source,
    )
