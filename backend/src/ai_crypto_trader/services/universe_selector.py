from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from statistics import pstdev
from typing import Any

from sqlalchemy import and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import AdminAction, Candle, StrategyConfig
from ai_crypto_trader.services.llm_agent.config import LLMConfig
from ai_crypto_trader.services.llm_agent.service import LLMService
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig
from ai_crypto_trader.utils.json_safe import json_safe

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if minimum is not None and value < minimum:
        return minimum
    return value


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _safe_return_pct(first_close: Decimal, last_close: Decimal) -> Decimal:
    if first_close == 0:
        return Decimal("0")
    return (last_close - first_close) / first_close


def _pct_returns(closes: list[Decimal]) -> list[float]:
    values: list[float] = []
    for idx in range(1, len(closes)):
        prev = closes[idx - 1]
        curr = closes[idx]
        if prev == 0:
            continue
        try:
            values.append(float((curr - prev) / prev))
        except (InvalidOperation, ValueError, TypeError):
            continue
    return values


def _symbol_variants(symbol_norm: str) -> list[str]:
    variants = [symbol_norm]
    if symbol_norm.endswith("USDT") and len(symbol_norm) > 4:
        variants.append(f"{symbol_norm[:-4]}/USDT")
    return variants


def _meta_text(col, key):  # type: ignore[no-untyped-def]
    return col.op("->>")(key)


def _meta_symbol_upper(col):  # type: ignore[no-untyped-def]
    return func.upper(
        func.coalesce(
            _meta_text(col, "symbol"),
            _meta_text(col, "symbol_normalized"),
            _meta_text(col, "symbol_in"),
        )
    )


@dataclass
class UniverseSelectorSettings:
    timeframe: str
    lookback_minutes: int
    top_n: int
    use_llm: bool
    llm_timeout_seconds: float
    llm_temperature: float
    max_candidates_default: int
    weight_win_rate: float
    weight_return_pct: float
    weight_volatility: float

    @classmethod
    def from_env(cls) -> "UniverseSelectorSettings":
        return cls(
            timeframe=(os.getenv("UNIVERSE_SELECTOR_TIMEFRAME", "1m").strip() or "1m"),
            lookback_minutes=_env_int("UNIVERSE_SELECTOR_LOOKBACK_MINUTES", 1440, minimum=1),
            top_n=_env_int("UNIVERSE_SELECTOR_TOP_N", 5, minimum=1),
            use_llm=_env_bool("UNIVERSE_SELECTOR_USE_LLM", False),
            llm_timeout_seconds=float(
                _env_float("UNIVERSE_SELECTOR_LLM_TIMEOUT_SECONDS", _env_float("AI_AGENT_TIMEOUT_SECONDS", 15.0))
            ),
            llm_temperature=_env_float("UNIVERSE_SELECTOR_LLM_TEMPERATURE", 0.0),
            max_candidates_default=_env_int("AI_AGENT_MAX_CANDIDATES", 20, minimum=1),
            weight_win_rate=_env_float("UNIVERSE_SELECTOR_WEIGHT_WIN_RATE", 0.6),
            weight_return_pct=_env_float("UNIVERSE_SELECTOR_WEIGHT_RETURN_PCT", 0.3),
            weight_volatility=_env_float("UNIVERSE_SELECTOR_WEIGHT_VOLATILITY", 0.1),
        )


def _parse_json_object(content: str) -> dict[str, Any] | None:
    try:
        data = json.loads(content)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


def _normalize_symbols(raw_symbols: list[Any]) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for raw in raw_symbols:
        symbol = normalize_symbol(str(raw or ""))
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def _normalize_symbol_for_query(symbol: str | None) -> str | None:
    if symbol is None:
        return None
    normalized = normalize_symbol(symbol)
    return normalized if normalized else None


def _score_symbol(
    *,
    win_rate: float,
    return_pct: float,
    volatility: float,
    settings: UniverseSelectorSettings,
) -> float:
    return (
        (settings.weight_win_rate * win_rate)
        + (settings.weight_return_pct * return_pct)
        - (settings.weight_volatility * volatility)
    )


async def _load_outcome_stats(
    session: AsyncSession,
    *,
    strategy_config_id: int,
    symbols: list[str],
    window_start: datetime,
) -> dict[str, dict[str, float]]:
    if not symbols:
        return {}

    stmt = (
        select(
            _meta_symbol_upper(AdminAction.meta).label("symbol"),
            _meta_text(AdminAction.meta, "win").label("win"),
            _meta_text(AdminAction.meta, "return_pct_signed").label("return_pct_signed"),
        )
        .where(
            and_(
                AdminAction.action == "TRADE_OUTCOME",
                AdminAction.created_at >= window_start,
                _meta_text(AdminAction.meta, "strategy_config_id") == str(strategy_config_id),
                _meta_symbol_upper(AdminAction.meta).in_(symbols),
            )
        )
        .order_by(AdminAction.created_at.desc(), AdminAction.id.desc())
    )
    rows = (await session.execute(stmt)).all()
    state: dict[str, dict[str, float]] = {}
    for row in rows:
        symbol = _normalize_symbol_for_query(getattr(row, "symbol", None))
        if not symbol:
            continue
        bucket = state.setdefault(symbol, {"wins": 0.0, "count": 0.0, "ret_sum": 0.0, "ret_count": 0.0})
        win_raw = str(getattr(row, "win", "") or "").strip().lower()
        if win_raw in {"true", "1", "yes", "on"}:
            bucket["wins"] += 1.0
            bucket["count"] += 1.0
        elif win_raw in {"false", "0", "no", "off"}:
            bucket["count"] += 1.0

        ret_raw = getattr(row, "return_pct_signed", None)
        if ret_raw not in (None, ""):
            try:
                bucket["ret_sum"] += float(Decimal(str(ret_raw)))
                bucket["ret_count"] += 1.0
            except Exception:
                pass

    result: dict[str, dict[str, float]] = {}
    for symbol, bucket in state.items():
        win_rate = (bucket["wins"] / bucket["count"]) if bucket["count"] > 0 else 0.0
        avg_return = (bucket["ret_sum"] / bucket["ret_count"]) if bucket["ret_count"] > 0 else 0.0
        result[symbol] = {"win_rate": win_rate, "avg_return_pct": avg_return}
    return result


async def _load_candle_stats(
    session: AsyncSession,
    *,
    timeframe: str,
    candidates: list[str],
    window_start: datetime,
    now_utc: datetime,
) -> dict[str, dict[str, Any]]:
    if not candidates:
        return {}

    variant_to_symbol: dict[str, str] = {}
    variants: list[str] = []
    for symbol in candidates:
        for variant in _symbol_variants(symbol):
            if variant in variant_to_symbol:
                continue
            variant_to_symbol[variant] = symbol
            variants.append(variant)

    rows = (
        await session.execute(
            select(Candle.symbol, Candle.open_time, Candle.close, Candle.volume)
            .where(
                Candle.timeframe == timeframe,
                Candle.open_time >= window_start,
                Candle.symbol.in_(variants),
            )
            .order_by(Candle.open_time.asc(), Candle.id.asc())
        )
    ).all()

    grouped: dict[str, list[tuple[datetime, Decimal, Decimal]]] = {symbol: [] for symbol in candidates}
    for row in rows:
        raw_symbol = str(row.symbol or "")
        symbol_norm = variant_to_symbol.get(raw_symbol) or normalize_symbol(raw_symbol)
        if symbol_norm not in grouped:
            continue
        grouped[symbol_norm].append(
            (
                row.open_time.astimezone(timezone.utc) if row.open_time.tzinfo else row.open_time.replace(tzinfo=timezone.utc),
                _to_decimal(row.close),
                _to_decimal(row.volume),
            )
        )

    stats: dict[str, dict[str, Any]] = {}
    for symbol in candidates:
        points = grouped.get(symbol) or []
        if not points:
            continue
        closes = [item[1] for item in points]
        if not closes:
            continue
        first_close = closes[0]
        last_close = closes[-1]
        return_pct = _safe_return_pct(first_close, last_close)
        returns = _pct_returns(closes)
        volatility = pstdev(returns) if len(returns) >= 2 else 0.0
        latest_open_time = points[-1][0]
        freshness_minutes = max(0.0, (now_utc - latest_open_time).total_seconds() / 60.0)
        stats[symbol] = {
            "symbol": symbol,
            "candles": len(points),
            "last_close": str(last_close),
            "close_lookback": str(first_close),
            "return_pct": str(return_pct),
            "volatility": float(volatility),
            "last_open_time": latest_open_time.isoformat(),
            "freshness_minutes": round(freshness_minutes, 3),
            "last_volume": str(points[-1][2]),
        }
    return stats


async def _load_distinct_candle_symbols(
    session: AsyncSession,
    *,
    timeframe: str,
    window_start: datetime,
    max_candidates: int,
) -> list[str]:
    rows = (
        await session.execute(
            select(Candle.symbol)
            .where(Candle.timeframe == timeframe, Candle.open_time >= window_start)
            .order_by(desc(Candle.open_time))
            .limit(max(500, max_candidates * 30))
        )
    ).all()
    symbols: list[str] = []
    seen: set[str] = set()
    for row in rows:
        symbol = normalize_symbol(str(row[0] or ""))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= max_candidates:
            break
    return symbols


async def _resolve_candidates(
    session: AsyncSession,
    *,
    strategy: StrategyConfig,
    timeframe: str,
    window_start: datetime,
    settings: UniverseSelectorSettings,
) -> tuple[list[str], str, str | None, dict[str, Any]]:
    thresholds = strategy.thresholds if isinstance(strategy.thresholds, dict) else {}
    symbols_raw = strategy.symbols if isinstance(strategy.symbols, list) else []
    normalized_symbols = _normalize_symbols(symbols_raw)
    has_auto = any(symbol == "AUTO" for symbol in normalized_symbols)
    explicit_symbols = [symbol for symbol in normalized_symbols if symbol != "AUTO"]

    auto_universe = bool(thresholds.get("auto_universe")) or has_auto
    universe_quote = normalize_symbol(str(thresholds.get("universe_quote") or "USDT"))
    universe_exclude_raw = thresholds.get("universe_exclude")
    universe_exclude = (
        {normalize_symbol(item) for item in universe_exclude_raw if normalize_symbol(item)}
        if isinstance(universe_exclude_raw, list)
        else set()
    )
    max_candidates = settings.max_candidates_default
    try:
        if thresholds.get("universe_max_candidates") is not None:
            max_candidates = max(int(thresholds.get("universe_max_candidates")), 1)
    except (TypeError, ValueError):
        max_candidates = settings.max_candidates_default
    max_candidates = max(max_candidates, 1)

    source = "strategy_symbols"
    reason: str | None = None
    candidates = explicit_symbols

    if not candidates and auto_universe:
        from_config = _normalize_symbols(PaperTraderConfig.from_env().symbols)
        if from_config:
            candidates = from_config
            source = "config_supported_symbols"
        else:
            from_candles = await _load_distinct_candle_symbols(
                session,
                timeframe=timeframe,
                window_start=window_start,
                max_candidates=max_candidates,
            )
            candidates = from_candles
            source = "candles_distinct_symbols"
            if not candidates:
                reason = "No symbols available from config or candles"
    elif not candidates:
        reason = "No strategy symbols configured"

    filtered: list[str] = []
    for symbol in candidates:
        if not symbol:
            continue
        if universe_quote and not symbol.endswith(universe_quote):
            continue
        if symbol in universe_exclude:
            continue
        filtered.append(symbol)
        if len(filtered) >= max_candidates:
            break

    meta = {
        "auto_universe": auto_universe,
        "universe_quote": universe_quote,
        "universe_exclude": sorted(universe_exclude),
        "max_candidates": max_candidates,
        "source": source,
    }
    return filtered, source, reason, meta


async def _rank_candidates_with_llm(
    *,
    strategy_config_id: int,
    timeframe: str,
    candidates: list[str],
    stats_per_symbol: list[dict[str, Any]],
    top_n: int,
    settings: UniverseSelectorSettings,
) -> tuple[list[str], str | None]:
    if not settings.use_llm:
        return [], None

    llm_service = LLMService(LLMConfig.from_env())
    provider = llm_service._provider_default()
    client = llm_service.clients.get(provider)
    if client is None:
        return [], "LLM client unavailable"

    prompt = {
        "task": "rank_symbols_for_next_trade_cycle",
        "strategy_config_id": strategy_config_id,
        "timeframe": timeframe,
        "candidates": candidates,
        "stats_per_symbol": stats_per_symbol[: min(len(stats_per_symbol), 30)],
        "max_select": top_n,
    }
    messages = [
        {
            "role": "system",
            "content": (
                "Select symbols for trading universe. "
                "Return STRICT JSON only: {\"symbols\": [..], \"reason\": \"...\"}. "
                "symbols must be subset of candidates and max_select length."
            ),
        },
        {"role": "user", "content": json.dumps(prompt, default=str)},
    ]
    try:
        response = await client.chat_completion(
            messages,
            temperature=settings.llm_temperature,
            timeout_seconds=settings.llm_timeout_seconds,
        )
    except Exception as exc:
        logger.warning(
            "universe selector llm call failed",
            extra={"strategy_config_id": strategy_config_id, "error": str(exc)},
        )
        return [], f"LLM call failed: {exc.__class__.__name__}"

    payload = _parse_json_object(response)
    if not isinstance(payload, dict):
        return [], "LLM returned invalid JSON"

    raw_symbols = payload.get("symbols")
    if not isinstance(raw_symbols, list):
        return [], "LLM output missing symbols list"

    picked: list[str] = []
    candidate_set = set(candidates)
    for raw_symbol in raw_symbols:
        symbol = normalize_symbol(str(raw_symbol or ""))
        if not symbol or symbol not in candidate_set:
            continue
        if symbol in picked:
            continue
        picked.append(symbol)
        if len(picked) >= top_n:
            break
    reason = str(payload.get("reason") or "").strip() or None
    return picked, reason


async def select_universe(
    session: AsyncSession,
    strategy_config_id: int,
    now_utc: datetime,
    top_n: int | None = None,
    window_minutes: int | None = None,
) -> dict[str, Any]:
    strategy = await session.get(StrategyConfig, strategy_config_id)
    if strategy is None:
        raise ValueError(f"Strategy config {strategy_config_id} not found")

    now = now_utc.astimezone(timezone.utc) if now_utc.tzinfo else now_utc.replace(tzinfo=timezone.utc)
    settings = UniverseSelectorSettings.from_env()
    timeframe = settings.timeframe
    window_minutes_value = max(int(window_minutes or settings.lookback_minutes), 1)
    top_n_value = max(int(top_n or settings.top_n), 1)
    window_start = now - timedelta(minutes=window_minutes_value)

    previous_symbols = _normalize_symbols(strategy.symbols if isinstance(strategy.symbols, list) else [])
    previous_trade_symbols = [symbol for symbol in previous_symbols if symbol != "AUTO"]
    candidates, source, source_reason, source_meta = await _resolve_candidates(
        session,
        strategy=strategy,
        timeframe=timeframe,
        window_start=window_start,
        settings=settings,
    )

    candle_stats = await _load_candle_stats(
        session,
        timeframe=timeframe,
        candidates=candidates,
        window_start=window_start,
        now_utc=now,
    )
    outcome_stats = await _load_outcome_stats(
        session,
        strategy_config_id=strategy_config_id,
        symbols=candidates,
        window_start=window_start,
    )

    stats_per_symbol: list[dict[str, Any]] = []
    for symbol in candidates:
        candle = candle_stats.get(symbol, {})
        outcomes = outcome_stats.get(symbol, {})
        return_pct = float(_to_decimal(candle.get("return_pct"), Decimal("0")))
        volatility = float(candle.get("volatility") or 0.0)
        win_rate = float(outcomes.get("win_rate") or 0.0)
        avg_return = float(outcomes.get("avg_return_pct") or 0.0)
        score = _score_symbol(
            win_rate=win_rate,
            return_pct=return_pct,
            volatility=volatility,
            settings=settings,
        )
        stats_per_symbol.append(
            {
                "symbol": symbol,
                "score": score,
                "win_rate": win_rate,
                "avg_return_pct": avg_return,
                "return_pct": return_pct,
                "volatility": volatility,
                "candles": candle.get("candles"),
                "last_close": candle.get("last_close"),
                "close_lookback": candle.get("close_lookback"),
                "freshness_minutes": candle.get("freshness_minutes"),
                "last_open_time": candle.get("last_open_time"),
                "last_volume": candle.get("last_volume"),
            }
        )

    stats_per_symbol.sort(key=lambda item: (float(item.get("score") or 0.0), str(item.get("symbol") or "")), reverse=True)
    heuristic_symbols = [str(item["symbol"]) for item in stats_per_symbol[:top_n_value]]

    picked_by = "heuristic"
    llm_reason: str | None = None
    llm_symbols, llm_reason = await _rank_candidates_with_llm(
        strategy_config_id=strategy_config_id,
        timeframe=timeframe,
        candidates=candidates,
        stats_per_symbol=stats_per_symbol,
        top_n=top_n_value,
        settings=settings,
    )
    new_symbols = llm_symbols if llm_symbols else heuristic_symbols
    if llm_symbols:
        picked_by = "llm"

    if not new_symbols:
        fallback = previous_trade_symbols[:top_n_value]
        if not fallback and previous_symbols:
            fallback = previous_symbols[:top_n_value]
        if not fallback:
            fallback = candidates[:top_n_value]
        new_symbols = fallback
    concrete_symbols = [symbol for symbol in new_symbols if symbol != "AUTO"]
    if concrete_symbols:
        new_symbols = concrete_symbols

    strategy.symbols = new_symbols
    strategy.updated_at = now
    await session.flush()

    action_meta = json_safe(
        {
            "strategy_config_id": strategy_config_id,
            "timeframe": timeframe,
            "window_minutes": window_minutes_value,
            "top_n": top_n_value,
            "source": source,
            "source_reason": source_reason,
            "source_meta": source_meta,
            "picked_by": picked_by,
            "picked_reason": llm_reason,
            "previous_symbols": previous_symbols,
            "new_symbols": new_symbols,
            "stats_snapshot": stats_per_symbol[:10],
            "rollback_payload": {
                "strategy_config_id": strategy_config_id,
                "previous_symbols": previous_symbols,
            },
        }
    )
    action = AdminAction(
        action="UNIVERSE_SELECTION",
        status="ok",
        message="Universe selection updated strategy symbols",
        meta=action_meta,
    )
    session.add(action)
    await session.flush()

    logger.info(
        "UNIVERSE_SELECTION",
        extra={
            "strategy_config_id": strategy_config_id,
            "picked_by": picked_by,
            "source": source,
            "previous_symbols": previous_symbols,
            "new_symbols": new_symbols,
            "admin_action_id": action.id,
        },
    )
    return {
        "strategy_config_id": strategy_config_id,
        "admin_action_id": action.id,
        "timeframe": timeframe,
        "window_minutes": window_minutes_value,
        "top_n": top_n_value,
        "previous_symbols": previous_symbols,
        "new_symbols": new_symbols,
        "picked_by": picked_by,
        "picked_reason": llm_reason,
        "source": source,
        "source_reason": source_reason,
        "source_meta": source_meta,
        "stats_per_symbol": stats_per_symbol,
    }


async def rollback_universe(
    session: AsyncSession,
    admin_action_id: int,
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    selection_action = await session.get(AdminAction, admin_action_id)
    if selection_action is None:
        raise ValueError(f"Admin action {admin_action_id} not found")
    if selection_action.action != "UNIVERSE_SELECTION":
        raise ValueError(f"Admin action {admin_action_id} is not UNIVERSE_SELECTION")
    meta = selection_action.meta if isinstance(selection_action.meta, dict) else {}
    rollback_payload = meta.get("rollback_payload") if isinstance(meta.get("rollback_payload"), dict) else {}
    strategy_config_raw = rollback_payload.get("strategy_config_id") or meta.get("strategy_config_id")
    try:
        strategy_config_id = int(strategy_config_raw)
    except (TypeError, ValueError):
        raise ValueError(f"Admin action {admin_action_id} missing strategy_config_id")

    restore_symbols_raw = rollback_payload.get("previous_symbols")
    if not isinstance(restore_symbols_raw, list):
        restore_symbols_raw = meta.get("previous_symbols")
    if not isinstance(restore_symbols_raw, list):
        raise ValueError(f"Admin action {admin_action_id} missing rollback symbols")
    restored_symbols = _normalize_symbols(restore_symbols_raw)

    strategy = await session.get(StrategyConfig, strategy_config_id)
    if strategy is None:
        raise ValueError(f"Strategy config {strategy_config_id} not found")

    previous_symbols = _normalize_symbols(strategy.symbols if isinstance(strategy.symbols, list) else [])
    now = now_utc.astimezone(timezone.utc) if now_utc and now_utc.tzinfo else (now_utc or datetime.now(timezone.utc))
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    strategy.symbols = restored_symbols
    strategy.updated_at = now
    await session.flush()

    action = AdminAction(
        action="UNIVERSE_ROLLBACK",
        status="ok",
        message="Universe symbols restored from selection action",
        meta=json_safe(
            {
                "strategy_config_id": strategy_config_id,
                "selection_admin_action_id": admin_action_id,
                "previous_symbols": previous_symbols,
                "restored_symbols": restored_symbols,
            }
        ),
    )
    session.add(action)
    await session.flush()

    logger.info(
        "UNIVERSE_ROLLBACK",
        extra={
            "strategy_config_id": strategy_config_id,
            "selection_admin_action_id": admin_action_id,
            "previous_symbols": previous_symbols,
            "restored_symbols": restored_symbols,
            "admin_action_id": action.id,
        },
    )
    return {
        "strategy_config_id": strategy_config_id,
        "selection_admin_action_id": admin_action_id,
        "admin_action_id": action.id,
        "previous_symbols": previous_symbols,
        "restored_symbols": restored_symbols,
    }
