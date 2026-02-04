import asyncio
import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import PositionPolicyConfig, RiskPolicy, StrategyConfig, StrategyPolicyBinding
from ai_crypto_trader.services.policies.loader import load_position_policy, load_risk_policy


logger = logging.getLogger(__name__)


def _split_symbols(raw: str) -> List[str]:
    return [s.strip() for s in raw.split(",") if s.strip()]


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    logger.warning("Unknown boolean value for %s=%r, falling back to %s", name, raw, default)
    return default


@dataclass
class PaperTraderConfig:
    engine_enabled: bool
    exchange_name: str
    symbols: List[str]
    timeframe: str
    poll_seconds: int
    base_ccy: str
    initial_balance: Decimal
    fee_bps: Decimal
    slippage_bps: Decimal
    risk_max_leverage: Decimal
    risk_max_position_pct: Decimal
    risk_max_drawdown_pct: Decimal
    risk_min_confidence: Decimal
    risk_min_size_score: Decimal

    @classmethod
    def from_env(cls) -> "PaperTraderConfig":
        return cls(
            engine_enabled=_env_flag("ENGINE_ENABLED", default=False),
            exchange_name=os.getenv("ENGINE_EXCHANGE_NAME", "binance").lower(),
            symbols=_split_symbols(os.getenv("ENGINE_SYMBOLS", "BTC/USDT,ETH/USDT")),
            timeframe=os.getenv("ENGINE_TIMEFRAME", "1m"),
            poll_seconds=int(os.getenv("ENGINE_POLL_SECONDS", "30")),
            base_ccy=os.getenv("PAPER_BASE_CCY", "USDT"),
            initial_balance=Decimal(os.getenv("PAPER_INITIAL_BALANCE", "10000")),
            fee_bps=Decimal(os.getenv("PAPER_FEE_BPS", "5")),  # 5 bps = 0.05%
            slippage_bps=Decimal(os.getenv("PAPER_SLIPPAGE_BPS", "5")),
            risk_max_leverage=Decimal(os.getenv("RISK_MAX_LEVERAGE", "2")),
            risk_max_position_pct=Decimal(os.getenv("RISK_MAX_POSITION_PCT", "50")),
            risk_max_drawdown_pct=Decimal(os.getenv("RISK_MAX_DRAWDOWN_PCT", "50")),
            risk_min_confidence=Decimal(os.getenv("RISK_MIN_CONFIDENCE", "0.2")),
            risk_min_size_score=Decimal(os.getenv("RISK_MIN_SIZE_SCORE", "0.1")),
        )


class ActiveConfigMissingError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


_CACHE_TTL_SECONDS = 5
_cache_lock = asyncio.Lock()
_cache_bundle: Optional[Dict[str, Any]] = None
_cache_expiry: float = 0.0


def _serialize_risk(policy: RiskPolicy) -> Dict[str, Any]:
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


def _serialize_position(policy: PositionPolicyConfig) -> Dict[str, Any]:
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


def _serialize_strategy(strategy: StrategyConfig) -> Dict[str, Any]:
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


async def _load_active_bundle(session: AsyncSession) -> Dict[str, Any]:
    row = await session.execute(
        select(StrategyConfig, StrategyPolicyBinding)
        .outerjoin(
            StrategyPolicyBinding,
            StrategyPolicyBinding.strategy_config_id == StrategyConfig.id,
        )
        .where(StrategyConfig.is_active.is_(True))
        .order_by(StrategyConfig.updated_at.desc(), StrategyConfig.id.desc())
        .limit(1)
    )
    record = row.first()
    if not record:
        raise ActiveConfigMissingError("NO_ACTIVE_STRATEGY")
    strategy, binding = record

    if not strategy.symbols:
        raise ActiveConfigMissingError("INVALID_STRATEGY")
    if not strategy.timeframe:
        raise ActiveConfigMissingError("INVALID_STRATEGY")
    if strategy.min_notional_usd is None or Decimal(str(strategy.min_notional_usd)) <= 0:
        raise ActiveConfigMissingError("INVALID_STRATEGY")
    risk = None
    position_policy = None
    policy_source = "legacy"
    bound_risk_policy_id = binding.risk_policy_id if binding else None
    bound_position_policy_id = binding.position_policy_id if binding else None
    effective_risk_policy_id = bound_risk_policy_id or strategy.risk_policy_id
    if effective_risk_policy_id is not None:
        risk = await load_risk_policy(session, policy_id=effective_risk_policy_id)
    if bound_position_policy_id is not None:
        position_policy = await load_position_policy(session, policy_id=bound_position_policy_id)
    if bound_risk_policy_id is not None or bound_position_policy_id is not None:
        policy_source = "binding"
        if bound_risk_policy_id is None and bound_position_policy_id is not None:
            policy_source = "mixed"

    logger.info(
        "POLICY_RESOLVED",
        extra={
            "strategy_config_id": strategy.id,
            "legacy_risk_policy_id": strategy.risk_policy_id,
            "bound_risk_policy_id": bound_risk_policy_id,
            "effective_risk_policy_id": effective_risk_policy_id,
            "position_policy_id": str(bound_position_policy_id) if bound_position_policy_id else None,
        },
    )

    if risk is None:
        raise ActiveConfigMissingError("NO_RISK_POLICY_ID")
    if getattr(risk, "status", "active") != "active" and not risk.is_active:
        raise ActiveConfigMissingError("RISK_POLICY_INACTIVE")

    return {
        "strategy_config": _serialize_strategy(strategy),
        "risk_policy": _serialize_risk(risk),
        "position_policy": _serialize_position(position_policy) if position_policy else None,
        "policy_source": policy_source,
    }


async def get_active_bundle(session: AsyncSession) -> Dict[str, Any]:
    """
    Returns the active strategy + risk policy bundle with a 5s in-process cache.
    """
    global _cache_bundle, _cache_expiry
    now = time.monotonic()
    if _cache_bundle is not None and _cache_expiry > now:
        return {
            **_cache_bundle,
            "cache": {"ttl_seconds": _CACHE_TTL_SECONDS, "hit": True},
        }

    async with _cache_lock:
        now = time.monotonic()
        if _cache_bundle is not None and _cache_expiry > now:
            return {
                **_cache_bundle,
                "cache": {"ttl_seconds": _CACHE_TTL_SECONDS, "hit": True},
            }
        bundle = await _load_active_bundle(session)
        _cache_bundle = bundle
        _cache_expiry = now + _CACHE_TTL_SECONDS
        return {
            **bundle,
            "cache": {"ttl_seconds": _CACHE_TTL_SECONDS, "hit": False},
        }
