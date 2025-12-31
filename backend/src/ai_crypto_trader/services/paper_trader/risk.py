from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional

from sqlalchemy import func, select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.services.llm_agent.schemas import AdviceResponse
from ai_crypto_trader.common.models import (
    EquitySnapshot,
    PaperOrder,
    PaperPosition,
    PaperTrade,
)


def enforce_confidence(advice: AdviceResponse, min_confidence: Decimal, min_size_score: Decimal) -> bool:
    return Decimal(str(advice.confidence)) >= min_confidence and Decimal(str(advice.size_score)) >= min_size_score


def clamp_notional(
    desired_notional: Decimal,
    balance: Decimal,
    equity: Decimal,
    max_leverage: Decimal,
    max_position_pct: Decimal,
    max_drawdown_pct: Decimal,
    peak_equity: Optional[Decimal],
) -> Decimal:
    """
    Ensure target notional respects leverage, position sizing, and drawdown limits.
    """
    max_notional_leverage = balance * max_leverage
    max_notional_pct = equity * (max_position_pct / Decimal("100"))
    max_allowed = min(max_notional_leverage, max_notional_pct)

    if peak_equity and peak_equity > 0:
        drawdown = (peak_equity - equity) / peak_equity * Decimal("100")
        if drawdown > max_drawdown_pct:
            return Decimal("0")

    signed_limit = max_allowed if desired_notional >= 0 else -max_allowed
    if abs(desired_notional) > abs(signed_limit):
        return signed_limit
    return desired_notional


REASONS = {
    "COOLDOWN_ACTIVE": "COOLDOWN_ACTIVE",
    "MAX_OPEN_POSITIONS": "MAX_OPEN_POSITIONS",
    "MAX_POSITION_USD": "MAX_POSITION_USD",
    "MIN_NOTIONAL": "MIN_NOTIONAL",
    "NO_STOP_LOSS_CONFIG": "NO_STOP_LOSS_CONFIG",
    "MAX_LOSS_PER_TRADE": "MAX_LOSS_PER_TRADE",
    "MAX_DAILY_LOSS": "MAX_DAILY_LOSS",
    "SHORT_NOT_ALLOWED": "SHORT_NOT_ALLOWED",
}


def _quantize(value: Decimal, exp: str) -> Decimal:
    return Decimal(value).quantize(Decimal(exp), rounding=ROUND_DOWN)


async def evaluate_and_size(
    session: AsyncSession,
    account_id: int,
    symbol: str,
    side: str,
    desired_notional_usd: Decimal,
    price: Decimal,
    bundle: Dict[str, Dict[str, object]],
) -> Dict[str, object]:
    strategy = bundle.get("strategy_config") or {}
    risk_policy = bundle.get("risk_policy") or {}
    side_norm = (side or "").lower().strip()

    stop_loss_bps = strategy.get("thresholds", {}).get("stop_loss_bps") if strategy else None
    if stop_loss_bps is None or Decimal(str(stop_loss_bps)) <= 0:
        return {"allowed": False, "sized_notional_usd": "0", "qty": "0", "reasons": [REASONS["NO_STOP_LOSS_CONFIG"]]}
    stop_loss_bps_dec = Decimal(str(stop_loss_bps))

    max_position_usd = Decimal(str(risk_policy.get("max_position_usd", "0")))
    max_loss_per_trade_usd = Decimal(str(risk_policy.get("max_loss_per_trade_usd", "0")))
    max_loss_per_day_usd = Decimal(str(risk_policy.get("max_loss_per_day_usd", "0")))
    max_open_positions = int(risk_policy.get("max_open_positions", 0) or 0)
    cooldown_seconds = int(risk_policy.get("cooldown_seconds", 0) or 0)
    fee_bps = Decimal(str(risk_policy.get("fee_bps", "0")))
    slippage_bps = Decimal(str(risk_policy.get("slippage_bps", "0")))
    min_notional_usd = Decimal(str(strategy.get("min_notional_usd", "0")))
    allow_short = bool(strategy.get("allow_short", False))

    now = datetime.now(timezone.utc)
    reasons: List[str] = []

    position = await session.scalar(
        select(PaperPosition).where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol)
    )
    current_qty = Decimal("0")
    if position and position.qty is not None:
        current_qty = Decimal(str(position.qty))

    exposure = abs(current_qty) * Decimal(str(price))

    increasing_exposure = True
    if side_norm == "buy":
        increasing_exposure = current_qty >= 0
    elif side_norm == "sell":
        increasing_exposure = current_qty <= 0

    if not increasing_exposure:
        cap_by_position = max_position_usd
    else:
        cap_by_position = max(Decimal("0"), max_position_usd - exposure)

    if cap_by_position <= 0:
        reasons.append(REASONS["MAX_POSITION_USD"])

    cap_by_trade_loss = Decimal("0")
    if stop_loss_bps_dec > 0:
        cap_by_trade_loss = max_loss_per_trade_usd / (stop_loss_bps_dec / Decimal("10000"))
    else:
        reasons.append(REASONS["NO_STOP_LOSS_CONFIG"])

    sized_notional = min(Decimal(str(desired_notional_usd)), cap_by_position or Decimal("0"), cap_by_trade_loss or Decimal("0"))
    sized_notional = _quantize(sized_notional, "0.01")

    if side_norm == "sell" and current_qty > 0 and not allow_short:
        cap_by_close = current_qty * Decimal(str(price))
        sized_notional = min(sized_notional, cap_by_close)
        sized_notional = _quantize(sized_notional, "0.01")
        if sized_notional <= 0:
            reasons.append(REASONS["MIN_NOTIONAL"])

    if sized_notional <= 0:
        reasons.append(REASONS["MAX_LOSS_PER_TRADE"])

    if sized_notional < min_notional_usd:
        reasons.append(REASONS["MIN_NOTIONAL"])

    if side.lower() == "sell" and current_qty <= 0 and not allow_short:
        reasons.append(REASONS["SHORT_NOT_ALLOWED"])

    if current_qty == 0 and sized_notional > 0 and max_open_positions > 0:
        open_positions = await session.scalar(
            select(func.count()).select_from(PaperPosition).where(PaperPosition.account_id == account_id, PaperPosition.qty != 0)
        )
        if (open_positions or 0) >= max_open_positions:
            reasons.append(REASONS["MAX_OPEN_POSITIONS"])

    if cooldown_seconds > 0:
        last_trade = await session.scalar(
            select(PaperTrade)
            .where(PaperTrade.account_id == account_id, PaperTrade.symbol == symbol)
            .order_by(PaperTrade.created_at.desc())
            .limit(1)
        )
        last_time = last_trade.created_at if last_trade else None
        if not last_time:
            last_order = await session.scalar(
                select(PaperOrder)
                .where(PaperOrder.account_id == account_id, PaperOrder.symbol == symbol, PaperOrder.status == "filled")
                .order_by(PaperOrder.created_at.desc())
                .limit(1)
            )
            last_time = last_order.created_at if last_order else None
        if last_time:
            if last_time.tzinfo is None:
                last_time = last_time.replace(tzinfo=timezone.utc)
            else:
                last_time = last_time.astimezone(timezone.utc)
            delta_sec = (now - last_time).total_seconds()
            if delta_sec < cooldown_seconds:
                reasons.append(REASONS["COOLDOWN_ACTIVE"])

    # Daily loss check
    if max_loss_per_day_usd > 0:
        start_of_day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        first_snapshot = await session.scalar(
            select(EquitySnapshot)
            .where(EquitySnapshot.account_id == account_id, EquitySnapshot.created_at >= start_of_day)
            .order_by(EquitySnapshot.created_at.asc())
            .limit(1)
        )
        latest_snapshot = await session.scalar(
            select(EquitySnapshot)
            .where(EquitySnapshot.account_id == account_id)
            .order_by(EquitySnapshot.created_at.desc())
            .limit(1)
        )
        if first_snapshot and latest_snapshot:
            start_equity = Decimal(str(first_snapshot.equity))
            latest_equity = Decimal(str(latest_snapshot.equity))
            if latest_equity - start_equity <= -max_loss_per_day_usd:
                reasons.append(REASONS["MAX_DAILY_LOSS"])

    qty = Decimal("0")
    if sized_notional > 0 and price > 0:
        qty = _quantize(sized_notional / Decimal(str(price)), "0.00000001")

    est_loss = _quantize(sized_notional * (stop_loss_bps_dec / Decimal("10000")), "0.01") if stop_loss_bps_dec > 0 else Decimal("0")
    est_fee = _quantize(sized_notional * (fee_bps / Decimal("10000")), "0.01") if fee_bps > 0 else Decimal("0")
    est_slippage = _quantize(sized_notional * (slippage_bps / Decimal("10000")), "0.01") if slippage_bps > 0 else Decimal("0")

    allowed = len(reasons) == 0
    return {
        "allowed": allowed,
        "sized_notional_usd": str(sized_notional),
        "qty": str(qty),
        "reasons": reasons,
        "est": {
            "stop_loss_bps": int(stop_loss_bps_dec),
            "max_loss_trade_usd": str(max_loss_per_trade_usd),
            "est_loss_usd": str(est_loss),
            "fee_bps": str(fee_bps),
            "slippage_bps": str(slippage_bps),
            "est_fee_usd": str(est_fee),
            "est_slippage_usd": str(est_slippage),
        },
    }
