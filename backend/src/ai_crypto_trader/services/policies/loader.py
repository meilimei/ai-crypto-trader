from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import PositionPolicyConfig, RiskPolicy, StrategyConfig, StrategyPolicyBinding

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EffectivePolicyIds:
    strategy_config_id: int
    legacy_risk_policy_id: Optional[int]
    bound_risk_policy_id: Optional[int]
    effective_risk_policy_id: Optional[int]
    position_policy_id: Optional[UUID]
    strategy_symbols: list[str]


async def get_effective_policy_ids(
    session: AsyncSession,
    *,
    strategy_config_id: int,
) -> EffectivePolicyIds | None:
    row = await session.execute(
        select(
            StrategyConfig.id,
            StrategyConfig.risk_policy_id,
            StrategyPolicyBinding.risk_policy_id,
            StrategyPolicyBinding.position_policy_id,
            StrategyConfig.symbols,
        )
        .outerjoin(
            StrategyPolicyBinding,
            StrategyPolicyBinding.strategy_config_id == StrategyConfig.id,
        )
        .where(StrategyConfig.id == strategy_config_id)
        .limit(1)
    )
    record = row.first()
    if not record:
        logger.warning(
            "POLICY_RESOLVED",
            extra={
                "strategy_config_id": strategy_config_id,
                "legacy_risk_policy_id": None,
                "bound_risk_policy_id": None,
                "effective_risk_policy_id": None,
                "position_policy_id": None,
                "error": "strategy_config_not_found",
            },
        )
        return None

    legacy_risk_policy_id = record[1]
    bound_risk_policy_id = record[2]
    position_policy_id = record[3]
    symbols_raw = record[4] or []
    strategy_symbols = symbols_raw if isinstance(symbols_raw, list) else []
    effective_risk_policy_id = bound_risk_policy_id or legacy_risk_policy_id

    logger.info(
        "POLICY_RESOLVED",
        extra={
            "strategy_config_id": strategy_config_id,
            "legacy_risk_policy_id": legacy_risk_policy_id,
            "bound_risk_policy_id": bound_risk_policy_id,
            "effective_risk_policy_id": effective_risk_policy_id,
            "position_policy_id": str(position_policy_id) if position_policy_id else None,
        },
    )

    return EffectivePolicyIds(
        strategy_config_id=strategy_config_id,
        legacy_risk_policy_id=legacy_risk_policy_id,
        bound_risk_policy_id=bound_risk_policy_id,
        effective_risk_policy_id=effective_risk_policy_id,
        position_policy_id=position_policy_id,
        strategy_symbols=strategy_symbols,
    )


async def load_risk_policy(
    session: AsyncSession,
    *,
    policy_id: int | None,
) -> RiskPolicy | None:
    if policy_id is None:
        return None
    policy = await session.get(RiskPolicy, policy_id)
    if policy is None:
        logger.warning("Risk policy missing", extra={"risk_policy_id": policy_id})
    return policy


async def load_position_policy(
    session: AsyncSession,
    *,
    policy_id: UUID | None,
) -> PositionPolicyConfig | None:
    if policy_id is None:
        return None
    policy = await session.get(PositionPolicyConfig, policy_id)
    if policy is None:
        logger.warning("Position policy missing", extra={"position_policy_id": str(policy_id)})
    return policy
