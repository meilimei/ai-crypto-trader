from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.models import Candle, StrategyConfig
from ai_crypto_trader.services.llm_agent.config import LLMConfig
from ai_crypto_trader.services.llm_agent.service import LLMService
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig

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


def _env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    if minimum is not None and value < minimum:
        return minimum
    return value


def _symbol_variants(symbol_normalized: str) -> list[str]:
    variants = [symbol_normalized]
    if symbol_normalized.endswith("USDT") and len(symbol_normalized) > 4:
        variants.append(f"{symbol_normalized[:-4]}/USDT")
    return variants


def _to_float(value: Decimal | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compute_volatility(returns: list[Decimal]) -> Decimal:
    if not returns:
        return Decimal("0")
    mean = sum(returns) / Decimal(len(returns))
    variance = sum((item - mean) ** 2 for item in returns) / Decimal(len(returns))
    try:
        return variance.sqrt()
    except InvalidOperation:
        return Decimal("0")


@dataclass(frozen=True)
class AutoTradeSettings:
    enabled: bool
    timeout_seconds: float
    candle_lookback: int
    max_candidates: int
    min_candle_freshness_minutes: int
    temperature: float

    @classmethod
    def from_env(cls) -> "AutoTradeSettings":
        return cls(
            enabled=_env_bool("AI_AGENT_ENABLED", False),
            timeout_seconds=_env_float("AI_AGENT_TIMEOUT_SECONDS", 15.0, minimum=1.0),
            candle_lookback=_env_int("AI_AGENT_CANDLE_LOOKBACK", 60, minimum=10),
            max_candidates=_env_int("AI_AGENT_MAX_CANDIDATES", 20, minimum=1),
            min_candle_freshness_minutes=_env_int(
                "AI_AGENT_MIN_CANDLE_FRESHNESS_MINUTES",
                120,
                minimum=1,
            ),
            temperature=_env_float("AI_AGENT_TEMPERATURE", 0.0, minimum=0.0),
        )


class AutoTradeAgent:
    def __init__(
        self,
        settings: AutoTradeSettings | None = None,
        *,
        llm_service: LLMService | None = None,
    ) -> None:
        self.settings = settings or AutoTradeSettings.from_env()
        self.llm_service = llm_service or LLMService(LLMConfig.from_env())

    async def decide(
        self,
        session: AsyncSession,
        *,
        account_id: int,
        strategy_config_id: int | None,
        timeframe: str,
        candidate_symbols: list[str],
    ) -> dict[str, Any]:
        candidates = self._normalize_candidates(candidate_symbols)
        if not self.settings.enabled:
            return self._hold_decision(
                reason="AI agent disabled",
                candidates=candidates,
                selected_symbol=candidates[0] if candidates else None,
            )
        if not candidates:
            return self._hold_decision(reason="No candidate symbols", candidates=[])

        features = await self._load_features(
            session,
            timeframe=timeframe,
            candidates=candidates,
        )
        if not features:
            return self._hold_decision(
                reason="No fresh candle features for candidates",
                candidates=candidates,
            )

        provider = self.llm_service._provider_default()
        client = self.llm_service.clients.get(provider)
        if client is None:
            return self._hold_decision(
                reason=f"No LLM client configured for provider={provider}",
                candidates=candidates,
                selected_symbol=candidates[0] if candidates else None,
            )

        system_prompt = (
            "You are a crypto execution selector. "
            "Return STRICT JSON only with keys: selected_symbol, side, qty, confidence, rationale_bullets, signals. "
            "Rules: selected_symbol must be one of candidates, side in buy/sell/hold, "
            "qty must be string decimal > 0 for buy/sell and \"0\" for hold, confidence in [0,1], "
            "rationale_bullets max 5 short strings, signals a small object of numeric hints."
        )
        user_prompt = json.dumps(
            {
                "account_id": account_id,
                "strategy_config_id": strategy_config_id,
                "timeframe": timeframe,
                "candidates": features,
            },
            default=str,
        )
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        try:
            content = await client.chat_completion(
                messages,
                temperature=self.settings.temperature,
                timeout_seconds=self.settings.timeout_seconds,
            )
        except Exception as exc:
            logger.warning(
                "auto trade llm request failed",
                extra={
                    "provider": provider,
                    "strategy_config_id": strategy_config_id,
                    "account_id": account_id,
                    "error": str(exc),
                },
            )
            return self._hold_decision(
                reason=f"LLM call failed: {exc.__class__.__name__}",
                candidates=candidates,
                selected_symbol=candidates[0] if candidates else None,
            )

        parsed = self._parse_json(content)
        if not isinstance(parsed, dict):
            return self._hold_decision(
                reason="LLM output invalid JSON",
                candidates=candidates,
                selected_symbol=candidates[0] if candidates else None,
            )

        decision = self._coerce_decision(parsed, candidates)
        if decision["side"] in {"buy", "sell"} and decision["selected_symbol"] and Decimal(decision["qty"]) > 0:
            decision["status"] = "proposed"
            decision["reason"] = None
            return decision
        return self._hold_decision(
            reason=decision.get("reason") or "LLM decision resolved to hold",
            candidates=candidates,
            selected_symbol=decision.get("selected_symbol"),
            confidence=decision.get("confidence"),
            rationale_bullets=decision.get("rationale_bullets"),
            signals=decision.get("signals"),
        )

    async def build_universe_for_strategy(
        self,
        session: AsyncSession,
        *,
        strategy_config_id: int,
        timeframe: str | None = None,
    ) -> dict[str, Any]:
        strategy = await session.get(StrategyConfig, strategy_config_id)
        if strategy is None:
            raise ValueError(f"Strategy config {strategy_config_id} not found")

        thresholds = strategy.thresholds if isinstance(strategy.thresholds, dict) else {}
        strategy_symbols_raw = strategy.symbols if isinstance(strategy.symbols, list) else []
        has_auto_sentinel = any(normalize_symbol(item) == "AUTO" for item in strategy_symbols_raw)
        auto_universe = bool(thresholds.get("auto_universe")) or has_auto_sentinel
        universe_quote = normalize_symbol(str(thresholds.get("universe_quote") or "USDT"))
        universe_exclude_raw = thresholds.get("universe_exclude")
        universe_exclude = (
            {normalize_symbol(item) for item in universe_exclude_raw if normalize_symbol(item)}
            if isinstance(universe_exclude_raw, list)
            else set()
        )
        max_candidates_raw = thresholds.get("universe_max_candidates")
        max_candidates = self.settings.max_candidates
        try:
            if max_candidates_raw is not None:
                max_candidates = max(int(max_candidates_raw), 1)
        except (TypeError, ValueError):
            max_candidates = self.settings.max_candidates
        max_candidates = min(max_candidates, max(self.settings.max_candidates, 1))

        candidates = self._normalize_candidates(strategy_symbols_raw)
        source = "strategy_symbols"
        reason = None
        if not candidates and auto_universe:
            fallback = await self._fallback_candidates(
                session,
                timeframe=timeframe or strategy.timeframe,
            )
            candidates = fallback["candidates"]
            source = fallback["source"]
            reason = fallback.get("reason")
        elif not candidates:
            source = "strategy_symbols"
            reason = "No strategy symbols configured"

        filtered: list[str] = []
        for symbol in candidates:
            if universe_quote and not symbol.endswith(universe_quote):
                continue
            if symbol in universe_exclude:
                continue
            filtered.append(symbol)
            if len(filtered) >= max_candidates:
                break

        tf = timeframe or strategy.timeframe or PaperTraderConfig.from_env().timeframe
        features_all = await self._load_features(
            session,
            timeframe=tf,
            candidates=filtered,
            enforce_freshness=False,
        )
        features_fresh = await self._load_features(
            session,
            timeframe=tf,
            candidates=filtered,
            enforce_freshness=True,
        )
        features_by_symbol = {
            normalize_symbol(str(item.get("symbol") or "")): item
            for item in features_all
        }
        fresh_symbols = {
            normalize_symbol(str(item.get("symbol") or "")): item
            for item in features_fresh
        }
        candidate_rows: list[dict[str, Any]] = []
        for symbol in filtered:
            row = dict(features_by_symbol.get(symbol) or {"symbol": symbol})
            row["is_fresh"] = symbol in fresh_symbols
            candidate_rows.append(row)

        return {
            "strategy_config_id": strategy_config_id,
            "timeframe": tf,
            "auto_universe": auto_universe,
            "source": source,
            "reason": reason,
            "thresholds": {
                "universe_max_candidates": max_candidates,
                "universe_quote": universe_quote,
                "universe_exclude": sorted(universe_exclude),
            },
            "candidates_count": len(filtered),
            "fresh_candidates_count": len(fresh_symbols),
            "candidates": filtered,
            "fresh_candidates": [item["symbol"] for item in candidate_rows if item.get("is_fresh")],
            "candidate_features": candidate_rows,
        }

    async def decide_for_strategy(
        self,
        session: AsyncSession,
        *,
        account_id: int,
        strategy_config_id: int,
        symbol: str | None = None,
        timeframe: str | None = None,
        window_minutes: int | None = None,
    ) -> dict[str, Any]:
        universe = await self.build_universe_for_strategy(
            session,
            strategy_config_id=strategy_config_id,
            timeframe=timeframe,
        )
        explicit_symbol = normalize_symbol(symbol or "")
        if explicit_symbol and explicit_symbol != "AUTO":
            candidates = [explicit_symbol]
        else:
            candidates = list(universe.get("fresh_candidates") or universe.get("candidates") or [])
        if not candidates:
            decision = self._hold_decision(
                reason=str(universe.get("reason") or "No candidates after filters"),
                candidates=[],
            )
        else:
            decision = await self.decide(
                session,
                account_id=account_id,
                strategy_config_id=strategy_config_id,
                timeframe=str(universe.get("timeframe") or timeframe or "1m"),
                candidate_symbols=candidates,
            )
        decision["candidates_count"] = len(candidates)
        decision["candidates_sample"] = candidates[:10]
        if window_minutes is not None:
            decision["window_minutes"] = int(window_minutes)
        return {
            "decision": decision,
            "universe": universe,
        }

    def _normalize_candidates(self, symbols: list[str]) -> list[str]:
        unique: list[str] = []
        seen: set[str] = set()
        for raw in symbols:
            symbol = normalize_symbol(raw)
            if not symbol or symbol == "AUTO":
                continue
            if symbol in seen:
                continue
            seen.add(symbol)
            unique.append(symbol)
            if len(unique) >= self.settings.max_candidates:
                break
        return unique

    async def _fallback_candidates(
        self,
        session: AsyncSession,
        *,
        timeframe: str,
    ) -> dict[str, Any]:
        from_config = self._normalize_candidates(PaperTraderConfig.from_env().symbols)
        if from_config:
            return {
                "source": "config_supported_symbols",
                "candidates": from_config,
                "reason": None,
            }

        rows = (
            await session.execute(
                select(Candle.symbol)
                .where(Candle.timeframe == timeframe)
                .order_by(desc(Candle.open_time))
                .limit(500)
            )
        ).all()
        distinct: list[str] = []
        seen: set[str] = set()
        for row in rows:
            symbol = normalize_symbol(str(row[0] or ""))
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            distinct.append(symbol)
            if len(distinct) >= self.settings.max_candidates:
                break
        if distinct:
            return {
                "source": "candles_distinct_symbols",
                "candidates": distinct,
                "reason": None,
            }
        return {
            "source": "none",
            "candidates": [],
            "reason": "No supported symbols available from config or candles",
        }

    async def _load_features(
        self,
        session: AsyncSession,
        *,
        timeframe: str,
        candidates: list[str],
        enforce_freshness: bool = True,
    ) -> list[dict[str, Any]]:
        now_utc = datetime.now(timezone.utc)
        features: list[dict[str, Any]] = []
        lookback = max(int(self.settings.candle_lookback), 10)

        for symbol in candidates:
            variants = _symbol_variants(symbol)
            rows = (
                await session.execute(
                    select(Candle.open_time, Candle.close, Candle.symbol)
                    .where(
                        Candle.timeframe == timeframe,
                        Candle.symbol.in_(variants),
                    )
                    .order_by(desc(Candle.open_time))
                    .limit(lookback * 2)
                )
            ).all()
            if not rows:
                continue
            # Keep most recent lookback entries in chronological order.
            rows_sorted = sorted(rows, key=lambda item: item[0])[-lookback:]
            closes: list[Decimal] = []
            last_open_time = rows_sorted[-1][0]
            for row in rows_sorted:
                try:
                    closes.append(Decimal(str(row[1])))
                except Exception:
                    closes.append(Decimal("0"))
            if not closes:
                continue

            freshness_minutes = max(
                0.0,
                (now_utc - last_open_time.astimezone(timezone.utc)).total_seconds() / 60.0,
            )
            is_fresh = freshness_minutes <= float(self.settings.min_candle_freshness_minutes)
            if enforce_freshness and not is_fresh:
                continue

            last_close = closes[-1]

            def _ret(period: int) -> Decimal:
                if len(closes) <= period or closes[-period - 1] == 0:
                    return Decimal("0")
                return (closes[-1] - closes[-period - 1]) / closes[-period - 1]

            returns = []
            for idx in range(1, min(len(closes), 21)):
                prev = closes[-idx - 1]
                curr = closes[-idx]
                if prev == 0:
                    continue
                returns.append((curr - prev) / prev)

            feature = {
                "symbol": symbol,
                "last_close": _to_float(last_close),
                "return_1": _to_float(_ret(1)),
                "return_5": _to_float(_ret(5)),
                "return_15": _to_float(_ret(15)),
                "vol_20": _to_float(_compute_volatility(returns)),
                "last_open_time": last_open_time.astimezone(timezone.utc).isoformat(),
                "freshness_minutes": round(freshness_minutes, 3),
                "is_fresh": is_fresh,
            }
            features.append(feature)
        return features

    def _parse_json(self, content: str) -> dict[str, Any] | None:
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

    def _coerce_decision(self, payload: dict[str, Any], candidates: list[str]) -> dict[str, Any]:
        selected_symbol = normalize_symbol(str(payload.get("selected_symbol") or ""))
        side = str(payload.get("side") or "hold").strip().lower()
        if side not in {"buy", "sell", "hold"}:
            side = "hold"
        if selected_symbol and selected_symbol not in candidates:
            selected_symbol = candidates[0] if candidates else None
            side = "hold"

        qty_text = str(payload.get("qty") or "0").strip()
        qty_dec = Decimal("0")
        try:
            qty_dec = Decimal(qty_text)
        except (InvalidOperation, ValueError):
            qty_dec = Decimal("0")

        confidence = payload.get("confidence")
        try:
            confidence_num = float(confidence)
        except (TypeError, ValueError):
            confidence_num = 0.0
        confidence_num = max(0.0, min(confidence_num, 1.0))

        bullets: list[str] = []
        raw_bullets = payload.get("rationale_bullets")
        if isinstance(raw_bullets, list):
            for item in raw_bullets:
                text = str(item).strip()
                if text:
                    bullets.append(text[:200])
                if len(bullets) >= 5:
                    break

        signals = payload.get("signals")
        if not isinstance(signals, dict):
            signals = {}

        reason = None
        if side in {"buy", "sell"}:
            if not selected_symbol:
                side = "hold"
                reason = "Selected symbol missing"
            elif qty_dec <= 0:
                side = "hold"
                reason = "LLM qty invalid or non-positive"

        return {
            "selected_symbol": selected_symbol,
            "side": side,
            "qty": str(qty_dec if qty_dec > 0 else Decimal("0")),
            "confidence": confidence_num,
            "rationale_bullets": bullets,
            "signals": signals,
            "reason": reason,
        }

    def _hold_decision(
        self,
        *,
        reason: str,
        candidates: list[str],
        selected_symbol: str | None = None,
        confidence: float | None = None,
        rationale_bullets: list[str] | None = None,
        signals: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        bullets = list(rationale_bullets or [])
        if reason:
            bullets = [reason] + [item for item in bullets if item != reason]
        return {
            "selected_symbol": selected_symbol,
            "side": "hold",
            "qty": "0",
            "confidence": confidence if confidence is not None else 0.0,
            "rationale_bullets": bullets[:5],
            "signals": signals or {},
            "status": "skipped",
            "reason": reason,
            "candidates": candidates,
        }
