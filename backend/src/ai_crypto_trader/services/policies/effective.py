from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.paper_trader.order_entry import (
    get_effective_position_limits,
    get_effective_risk_limits,
)
from ai_crypto_trader.services.policies.loader import (
    EffectivePolicyIds,
    get_effective_policy_ids,
    load_position_policy,
    load_risk_policy,
)


def _serialize_limit_values(limits: dict[str, Any] | None) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if not isinstance(limits, dict):
        return payload
    for key, value in limits.items():
        if value is None:
            payload[key] = None
        elif isinstance(value, Decimal):
            payload[key] = str(value)
        else:
            payload[key] = value
    return payload


@dataclass(frozen=True)
class EffectivePolicySnapshot:
    symbol: str
    policy_source: str
    policy_binding: dict[str, Any]
    computed_limits: dict[str, Any]
    policy_ids: EffectivePolicyIds


async def resolve_effective_policy_snapshot(
    session: AsyncSession,
    *,
    strategy_config_id: int,
    symbol: str,
) -> EffectivePolicySnapshot | None:
    policy_ids = await get_effective_policy_ids(session, strategy_config_id=strategy_config_id)
    if policy_ids is None:
        return None

    symbol_norm = normalize_symbol(symbol)
    if policy_ids.bound_risk_policy_id is not None or policy_ids.position_policy_id is not None:
        policy_source = "binding"
    elif policy_ids.effective_risk_policy_id is not None:
        policy_source = "strategy_legacy"
    else:
        policy_source = "account_default"

    risk_limits = {
        "max_order_notional_usdt": None,
        "min_order_notional_usdt": None,
        "max_leverage": None,
        "max_drawdown_pct": None,
        "max_drawdown_usdt": None,
        "max_daily_loss_usdt": None,
        "equity_lookback_hours": None,
        "lookback_minutes": None,
    }
    risk_limit_source = None
    risk_policy_overrides = None
    position_limits = {
        "max_position_qty": None,
        "max_position_notional_usdt": None,
        "max_position_pct_equity": None,
    }
    position_limit_source = None
    position_policy_overrides = None

    if policy_ids.effective_risk_policy_id is not None:
        risk_policy_row = await load_risk_policy(session, policy_id=policy_ids.effective_risk_policy_id)
        if risk_policy_row is not None:
            (
                risk_limits,
                risk_limit_source,
                risk_policy_overrides,
            ) = get_effective_risk_limits(risk_policy_row.params or {}, symbol_norm)

    if policy_ids.position_policy_id is not None:
        position_policy_row = await load_position_policy(session, policy_id=policy_ids.position_policy_id)
        if position_policy_row is not None:
            (
                position_limits,
                position_limit_source,
                position_policy_overrides,
            ) = get_effective_position_limits(position_policy_row.params or {}, symbol_norm)

    policy_binding = {
        "strategy_config_id": strategy_config_id,
        "legacy_risk_policy_id": policy_ids.legacy_risk_policy_id,
        "bound_risk_policy_id": policy_ids.bound_risk_policy_id,
        "effective_risk_policy_id": policy_ids.effective_risk_policy_id,
        "bound_position_policy_id": str(policy_ids.position_policy_id) if policy_ids.position_policy_id else None,
        "effective_position_policy_id": str(policy_ids.position_policy_id) if policy_ids.position_policy_id else None,
    }
    computed_limits = {
        "risk_limits": _serialize_limit_values(risk_limits),
        "risk_limit_source": risk_limit_source,
        "risk_policy_overrides": risk_policy_overrides,
        "symbol_limits": _serialize_limit_values(position_limits),
        "symbol_limit_source": position_limit_source,
        "position_policy_overrides": position_policy_overrides,
    }

    return EffectivePolicySnapshot(
        symbol=symbol_norm,
        policy_source=policy_source,
        policy_binding=policy_binding,
        computed_limits=computed_limits,
        policy_ids=policy_ids,
    )

