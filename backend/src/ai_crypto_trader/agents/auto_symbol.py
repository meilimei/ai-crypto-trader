from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from statistics import pstdev
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, Candle
from ai_crypto_trader.services.llm_agent.config import LLMConfig
from ai_crypto_trader.services.llm_agent.service import LLMService
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)


def _symbol_variants(symbol_normalized: str) -> list[str]:
    variants = [symbol_normalized]
    if symbol_normalized.endswith("USDT") and len(symbol_normalized) > 4:
        variants.append(f"{symbol_normalized[:-4]}/USDT")
    return variants


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _parse_json_object(content: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(content)
        return payload if isinstance(payload, dict) else None
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        return None
    try:
        payload = json.loads(match.group(0))
        return payload if isinstance(payload, dict) else None
    except json.JSONDecodeError:
        return None


def _coerce_rationale(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    items: list[str] = []
    for value in raw:
        text = str(value).strip()
        if not text:
            continue
        items.append(text[:200])
        if len(items) >= 5:
            break
    return items


def _coerce_signals(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    signals: dict[str, Any] = {}
    for key, value in list(raw.items())[:20]:
        signals[str(key)] = value
    return signals


def validate_llm_symbol_payload(
    payload: dict[str, Any] | None,
    allowed_symbols: list[str],
) -> dict[str, Any]:
    allowed = [normalize_symbol(symbol) for symbol in allowed_symbols if normalize_symbol(symbol)]
    fallback_symbol = allowed[0] if allowed else None
    reason: str | None = None

    candidate_symbol = normalize_symbol(str((payload or {}).get("symbol") or ""))
    if not candidate_symbol:
        candidate_symbol = fallback_symbol or ""
        reason = "missing_llm_symbol"
    elif candidate_symbol not in allowed:
        candidate_symbol = fallback_symbol or candidate_symbol
        reason = "invalid_llm_symbol"

    confidence = _safe_float((payload or {}).get("confidence"), 0.0)
    confidence = max(0.0, min(confidence, 1.0))
    rationale = _coerce_rationale((payload or {}).get("rationale"))
    signals = _coerce_signals((payload or {}).get("signals"))

    return {
        "symbol": candidate_symbol,
        "selected_symbol": candidate_symbol,
        "confidence": confidence,
        "rationale": rationale,
        "signals": signals,
        "reason": reason,
    }


async def _load_symbol_features(
    session: AsyncSession,
    *,
    symbol: str,
    timeframe: str,
    lookback: int,
) -> dict[str, Any] | None:
    variants = _symbol_variants(symbol)
    rows = (
        await session.execute(
            select(Candle.open_time, Candle.close, Candle.volume, Candle.symbol)
            .where(Candle.timeframe == timeframe, Candle.symbol.in_(variants))
            .order_by(desc(Candle.open_time))
            .limit(max(lookback, 10))
        )
    ).all()
    if not rows:
        return None

    points = sorted(rows, key=lambda item: item.open_time)
    closes = [_safe_decimal(item.close) for item in points]
    if not closes:
        return None

    returns: list[float] = []
    for idx in range(1, len(closes)):
        prev = closes[idx - 1]
        if prev == 0:
            continue
        returns.append(float((closes[idx] - prev) / prev))

    last_close = closes[-1]
    return_15 = 0.0
    return_1h = 0.0
    if len(closes) >= 2:
        prev_idx_15 = max(0, len(closes) - 16)
        prev_idx_60 = max(0, len(closes) - 61)
        if closes[prev_idx_15] != 0:
            return_15 = float((last_close - closes[prev_idx_15]) / closes[prev_idx_15])
        if closes[prev_idx_60] != 0:
            return_1h = float((last_close - closes[prev_idx_60]) / closes[prev_idx_60])

    last_open_time = points[-1].open_time
    if last_open_time.tzinfo is None:
        last_open_time = last_open_time.replace(tzinfo=timezone.utc)
    else:
        last_open_time = last_open_time.astimezone(timezone.utc)

    return {
        "symbol": symbol,
        "last_close": str(last_close),
        "return_15m": return_15,
        "return_1h": return_1h,
        "volatility": float(pstdev(returns)) if len(returns) >= 2 else 0.0,
        "candles_count": len(points),
        "last_open_time": last_open_time.isoformat(),
        "source_symbol": str(points[-1].symbol or symbol),
        "last_volume": str(points[-1].volume) if points[-1].volume is not None else None,
    }


async def select_symbol(
    session: AsyncSession,
    *,
    account_id: int,
    strategy_config_id: int,
    timeframe: str,
    symbols: list[str],
    now_utc: datetime,
) -> dict[str, Any]:
    allowed_symbols = [normalize_symbol(sym) for sym in symbols if normalize_symbol(sym) and normalize_symbol(sym) != "AUTO"]
    if not allowed_symbols:
        raise ValueError("AUTO symbol selection requires strategy_config symbols")
    fallback_symbol = allowed_symbols[0]

    lookback = max(int(os.getenv("AUTO_SYMBOL_CANDLE_LOOKBACK", "60")), 10)
    timeout_seconds = float(os.getenv("AI_AGENT_TIMEOUT_SECONDS", "15"))

    features_summary: list[dict[str, Any]] = []
    for symbol in allowed_symbols:
        features = await _load_symbol_features(
            session,
            symbol=symbol,
            timeframe=timeframe,
            lookback=lookback,
        )
        if features:
            features_summary.append(features)

    llm_raw: str | None = None
    selected = {
        "symbol": fallback_symbol,
        "selected_symbol": fallback_symbol,
        "confidence": 0.0,
        "rationale": ["No candles available, fallback to first strategy symbol"],
        "signals": {},
        "reason": "no_candles_fallback",
    }

    if features_summary:
        llm_service = LLMService(LLMConfig.from_env())
        provider = llm_service._provider_default()
        client = llm_service.clients.get(provider)
        if client is None:
            selected["reason"] = "llm_client_unavailable"
            selected["rationale"] = ["LLM client unavailable, fallback to first strategy symbol"]
        else:
            prompt_payload = {
                "task": "select_best_single_symbol",
                "account_id": account_id,
                "strategy_config_id": strategy_config_id,
                "timeframe": timeframe,
                "allowed_symbols": allowed_symbols,
                "features": features_summary,
            }
            messages = [
                {
                    "role": "system",
                    "content": (
                        "Return STRICT JSON only with keys: symbol, confidence, rationale, signals. "
                        "symbol must be exactly one of allowed_symbols. confidence in [0,1]. "
                        "rationale is a list of short strings."
                    ),
                },
                {"role": "user", "content": json.dumps(prompt_payload, default=str)},
            ]
            try:
                llm_raw = await client.chat_completion(
                    messages,
                    temperature=0.0,
                    timeout_seconds=timeout_seconds,
                )
                parsed = _parse_json_object(llm_raw)
                selected = validate_llm_symbol_payload(parsed, allowed_symbols)
                if selected.get("reason") is None:
                    selected["reason"] = "llm_selected"
                if not selected.get("rationale"):
                    selected["rationale"] = ["Selected by LLM ranking from candle features"]
            except Exception as exc:
                logger.warning(
                    "AUTO_SYMBOL_LLM_FAILED",
                    extra={
                        "account_id": account_id,
                        "strategy_config_id": strategy_config_id,
                        "timeframe": timeframe,
                        "error": str(exc),
                    },
                )
                selected = {
                    "symbol": fallback_symbol,
                    "selected_symbol": fallback_symbol,
                    "confidence": 0.0,
                    "rationale": [f"LLM failure ({exc.__class__.__name__}), fallback to first strategy symbol"],
                    "signals": {},
                    "reason": "llm_error_fallback",
                }
    else:
        logger.warning(
            "AUTO_SYMBOL_NO_CANDLES",
            extra={
                "account_id": account_id,
                "strategy_config_id": strategy_config_id,
                "timeframe": timeframe,
                "allowed_symbols": allowed_symbols,
            },
        )

    action_meta = json_safe(
        {
            "account_id": str(account_id),
            "strategy_config_id": strategy_config_id,
            "timeframe": timeframe,
            "allowed_symbols": allowed_symbols,
            "features_summary": features_summary[:20],
            "llm_raw": llm_raw,
            "selected_symbol": selected.get("symbol"),
            "confidence": selected.get("confidence"),
            "rationale": selected.get("rationale"),
            "signals": selected.get("signals"),
            "reason": selected.get("reason"),
            "created_now_utc": now_utc.isoformat() if now_utc.tzinfo else now_utc.replace(tzinfo=timezone.utc).isoformat(),
        }
    )
    session.add(
        AdminAction(
            action="AUTO_SYMBOL_PLAN",
            status="ok",
            message="AUTO symbol selection plan",
            meta=action_meta,
        )
    )
    await session.flush()
    return selected

