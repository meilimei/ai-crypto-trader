import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import MissingGreenlet
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import AsyncSessionLocal
from ai_crypto_trader.common.models import (
    PaperAccount,
    PaperBalance,
    PaperPosition,
    EquitySnapshot,
    StrategyConfig,
)
from ai_crypto_trader.services.llm_agent.config import LLMConfig
from ai_crypto_trader.services.llm_agent.schemas import AdviceRequest, AdviceResponse, MarketSummary as AdviceMarketSummary, PerformanceSummary
from ai_crypto_trader.services.llm_agent.service import LLMService
from ai_crypto_trader.services.agent.auto_trade import AutoTradeAgent, AutoTradeSettings
from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig
from ai_crypto_trader.services.paper_trader.order_entry import place_order_unified
from ai_crypto_trader.services.explainability.explainability import emit_trade_decision
from ai_crypto_trader.services.paper_trader.rejects import RejectReason, log_reject_throttled, make_reject
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.admin_actions.helpers import add_action_deduped
from ai_crypto_trader.services.paper_trader.market_summary import MarketSummaryBuilder
from ai_crypto_trader.services.paper_trader.risk import clamp_notional, enforce_confidence
from ai_crypto_trader.services.paper_trader.sizing import target_notional_from_advice
from ai_crypto_trader.services.policies.effective import resolve_effective_policy_snapshot

logger = logging.getLogger(__name__)


class PaperTradingEngine:
    def __init__(self, config: PaperTraderConfig, stop_event: Optional[asyncio.Event] = None) -> None:
        self.config = config
        self.llm_service = LLMService(LLMConfig.from_env())
        self.auto_trade_settings = AutoTradeSettings.from_env()
        self.auto_trade_agent = AutoTradeAgent(self.auto_trade_settings, llm_service=self.llm_service)
        self.account_name = f"paper-{self.config.exchange_name}"
        self.peak_equity: Optional[Decimal] = None
        self.stop_event = stop_event or asyncio.Event()
        self.started_at: Optional[datetime] = None
        self.last_cycle_at: Optional[datetime] = None
        self.last_error: Optional[str] = None

    async def run(self) -> None:
        logger.info("Starting paper trading engine")
        self.started_at = datetime.now(timezone.utc)

        while not self.stop_event.is_set():
            self.last_cycle_at = datetime.now(timezone.utc)
            try:
                await self._cycle()
                self.last_error = None
            except MissingGreenlet as exc:
                self.last_error = "MissingGreenlet"
                logger.exception("Paper engine cycle failed with MissingGreenlet")
                async with AsyncSessionLocal() as session:
                    await self._log_engine_error(session, account_id=None, symbol=None, exc=exc)
                await asyncio.sleep(2)
                continue
            except Exception as exc:
                self.last_error = str(exc)
                logger.exception("Paper engine cycle failed")
                await asyncio.sleep(2)
                continue
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.config.poll_seconds)
            except asyncio.TimeoutError:
                continue

        logger.info("Paper trading engine stopped")

    async def _cycle(self) -> None:
        async with AsyncSessionLocal() as session:
            try:
                account = await self._ensure_account(session)
                balance = await self._ensure_balance(session, account.id)
            except MissingGreenlet as exc:
                await self._log_engine_error(session, account_id=None, symbol=None, exc=exc)
                return

            equity = await self._compute_equity(session, balance, account.id)
            if self.peak_equity is None or equity > self.peak_equity:
                self.peak_equity = equity

            strategy_config = await session.scalar(
                select(StrategyConfig)
                .where(StrategyConfig.is_active.is_(True))
                .order_by(StrategyConfig.updated_at.desc(), StrategyConfig.id.desc())
                .limit(1)
            )
            strategy_config_id = strategy_config.id if strategy_config else None
            symbols_for_cycle = (
                strategy_config.symbols if strategy_config and strategy_config.symbols else self.config.symbols
            )
            strategy_thresholds = (
                strategy_config.thresholds
                if strategy_config and isinstance(strategy_config.thresholds, dict)
                else {}
            )
            strategy_timeframe = (
                strategy_config.timeframe
                if strategy_config and strategy_config.timeframe
                else self.config.timeframe
            )

            auto_universe = self._is_auto_universe_mode(
                thresholds=strategy_thresholds,
                symbols=symbols_for_cycle,
            )
            if auto_universe and strategy_config_id is not None:
                try:
                    await self._process_auto_universe(
                        session,
                        account_id=account.id,
                        strategy_config_id=strategy_config_id,
                        timeframe=strategy_timeframe,
                        symbols=symbols_for_cycle,
                    )
                except MissingGreenlet as exc:
                    await self._log_engine_error(session, account_id=account.id, symbol="AUTO", exc=exc)
            else:
                for symbol in symbols_for_cycle:
                    try:
                        await self._process_symbol(
                            session,
                            account.id,
                            balance,
                            equity,
                            symbol,
                            strategy_config_id=strategy_config_id,
                        )
                    except MissingGreenlet as exc:
                        await self._log_engine_error(session, account_id=account.id, symbol=symbol, exc=exc)
                        continue

            try:
                await self._snapshot_equity(session, account.id, balance, equity)
                await session.commit()
            except MissingGreenlet as exc:
                await self._log_engine_error(session, account_id=account.id, symbol=None, exc=exc)
                await session.rollback()
                return

    async def _ensure_account(self, session: AsyncSession) -> PaperAccount:
        account = await session.scalar(select(PaperAccount).where(PaperAccount.name == self.account_name))
        if account:
            return account
        account = PaperAccount(name=self.account_name, base_ccy=self.config.base_ccy)
        session.add(account)
        await session.flush()
        return account

    async def _ensure_balance(self, session: AsyncSession, account_id: int) -> PaperBalance:
        balance = await session.scalar(
            select(PaperBalance).where(PaperBalance.account_id == account_id, PaperBalance.ccy == self.config.base_ccy)
        )
        if balance:
            return balance
        balance = PaperBalance(
            account_id=account_id,
            ccy=self.config.base_ccy,
            available=self.config.initial_balance,
        )
        session.add(balance)
        await session.flush()
        return balance

    def _is_auto_universe_mode(self, *, thresholds: dict, symbols: list[str]) -> bool:
        if not self.auto_trade_settings.enabled:
            return False
        auto_flag = bool(thresholds.get("auto_universe")) if isinstance(thresholds, dict) else False
        has_sentinel = any(normalize_symbol(sym) == "AUTO" for sym in (symbols or []))
        return auto_flag or has_sentinel

    async def _process_auto_universe(
        self,
        session: AsyncSession,
        *,
        account_id: int,
        strategy_config_id: int,
        timeframe: str,
        symbols: list[str],
    ) -> None:
        bundle = await self.auto_trade_agent.decide_for_strategy(
            session,
            account_id=account_id,
            strategy_config_id=strategy_config_id,
            symbol="AUTO",
            timeframe=timeframe,
            window_minutes=60,
        )
        decision = bundle["decision"]
        universe = bundle["universe"]
        candidates = list(decision.get("candidates") or universe.get("candidates") or [])
        selected_symbol = normalize_symbol(str(decision.get("selected_symbol") or ""))
        side = str(decision.get("side") or "hold").lower().strip()
        qty_text = str(decision.get("qty") or "0")
        try:
            qty = Decimal(qty_text)
        except Exception:
            qty = Decimal("0")

        logger.info(
            "AUTO_TRADE_DECISION",
            extra={
                "account_id": account_id,
                "strategy_config_id": strategy_config_id,
                "candidates_count": len(candidates),
                "selected_symbol": selected_symbol or None,
                "side": side,
                "qty": str(qty),
                "confidence": decision.get("confidence"),
                "status": decision.get("status"),
                "reason": decision.get("reason"),
            },
        )

        if side == "hold" or not selected_symbol or qty <= 0:
            snapshot = await resolve_effective_policy_snapshot(
                session,
                strategy_config_id=strategy_config_id,
                symbol=selected_symbol or (candidates[0] if candidates else "AUTO"),
            )
            await emit_trade_decision(
                session,
                {
                    "created_now_utc": datetime.now(timezone.utc),
                    "account_id": str(account_id),
                    "strategy_id": str(strategy_config_id),
                    "strategy_config_id": strategy_config_id,
                    "symbol_in": selected_symbol or "AUTO",
                    "symbol_normalized": selected_symbol or "AUTO",
                    "side": side if side in {"buy", "sell"} else "hold",
                    "qty_requested": str(qty if qty > 0 else Decimal("0")),
                    "status": "skipped",
                    "rationale": decision.get("reason") or "Agent decided hold",
                    "inputs": {
                        "strategy_thresholds": None,
                        "timeframe": timeframe,
                        "symbols": candidates,
                        "price_source": "agent_auto",
                        "market_price_used": None,
                    },
                    "policy_source": snapshot.policy_source if snapshot else "account_default",
                    "policy_binding": snapshot.policy_binding if snapshot else {"strategy_config_id": strategy_config_id},
                    "computed_limits": snapshot.computed_limits if snapshot else {},
                    "agent_decision": decision,
                    "result": {
                        "reject": {
                            "code": "AUTO_HOLD",
                            "reason": decision.get("reason") or "Agent decided hold",
                            "details": {
                                "selected_symbol": selected_symbol or None,
                                "candidates": candidates,
                                "universe_source": universe.get("source"),
                            },
                        }
                    },
                },
            )
            return

        summary_builder = MarketSummaryBuilder(session, self.config.exchange_name, timeframe)
        summary = await summary_builder.build(selected_symbol)
        if summary is None:
            await emit_trade_decision(
                session,
                {
                    "created_now_utc": datetime.now(timezone.utc),
                    "account_id": str(account_id),
                    "strategy_id": str(strategy_config_id),
                    "strategy_config_id": strategy_config_id,
                    "symbol_in": selected_symbol,
                    "symbol_normalized": selected_symbol,
                    "side": side,
                    "qty_requested": str(qty),
                    "status": "skipped",
                    "rationale": "No candles for selected symbol",
                    "inputs": {
                        "strategy_thresholds": None,
                        "timeframe": timeframe,
                        "symbols": candidates,
                        "price_source": "agent_auto",
                        "market_price_used": None,
                    },
                    "policy_source": "binding",
                    "policy_binding": {"strategy_config_id": strategy_config_id},
                    "computed_limits": {},
                    "agent_decision": decision,
                    "result": {
                        "reject": {
                            "code": "NO_CANDLES",
                            "reason": "No candles for selected symbol",
                            "details": {"selected_symbol": selected_symbol},
                        }
                    },
                },
            )
            return

        result = await place_order_unified(
            session,
            account_id=account_id,
            symbol=selected_symbol,
            side=side,
            qty=qty,
            strategy_id=strategy_config_id,
            market_price=summary.last_close,
            market_price_source="agent_auto",
            fee_bps=self.config.fee_bps,
            slippage_bps=self.config.slippage_bps,
            meta={
                "origin": "engine_auto_agent",
                "agent_decision": decision,
            },
            reject_action_type="ORDER_SKIPPED",
            reject_window_seconds=120,
        )
        if isinstance(result, RejectReason):
            logger.info(
                "AUTO_TRADE_DECISION_RESULT",
                extra={
                    "account_id": account_id,
                    "strategy_config_id": strategy_config_id,
                    "selected_symbol": selected_symbol,
                    "side": side,
                    "qty": str(qty),
                    "result": "rejected",
                    "reject_code": str(result.code),
                },
            )
            return
        logger.info(
            "AUTO_TRADE_DECISION_RESULT",
            extra={
                "account_id": account_id,
                "strategy_config_id": strategy_config_id,
                "selected_symbol": selected_symbol,
                "side": side,
                "qty": str(qty),
                "result": "executed",
                "order_id": result.execution.order.id,
                "trade_id": result.execution.trade.id,
            },
        )

    async def _process_symbol(
        self,
        session: AsyncSession,
        account_id: int,
        balance: PaperBalance,
        equity: Decimal,
        symbol: str,
        strategy_config_id: int | None = None,
    ) -> None:
        summary_builder = MarketSummaryBuilder(session, self.config.exchange_name, self.config.timeframe)
        summary = await summary_builder.build(symbol)
        if summary is None:
            logger.warning("No candles for symbol; skipping", extra={"symbol": symbol, "timeframe": self.config.timeframe})
            return
        if self.stop_event.is_set():
            return

        advice = await self._fetch_advice(summary)
        if advice is None:
            return

        if not enforce_confidence(
            advice,
            self.config.risk_min_confidence,
            self.config.risk_min_size_score,
        ):
            logger.info(
                "Advice below thresholds; staying flat",
                extra={
                    "symbol": symbol,
                    "confidence": advice.confidence,
                    "size_score": advice.size_score,
                },
            )
            target_notional = Decimal("0")
        else:
            target_notional = target_notional_from_advice(balance.available, advice, self.config.risk_max_leverage)
            target_notional = clamp_notional(
                target_notional,
                balance.available,
                equity,
                self.config.risk_max_leverage,
                self.config.risk_max_position_pct,
                self.config.risk_max_drawdown_pct,
                self.peak_equity,
            )

        symbol_norm = normalize_symbol(symbol)
        position = await self._get_or_create_position(session, account_id, symbol)
        # Refresh unrealized PnL with current mark even if not trading
        if position.qty != 0:
            position.unrealized_pnl = (summary.last_close - position.avg_entry_price) * position.qty
        else:
            position.unrealized_pnl = Decimal("0")

        target_qty = Decimal("0")
        if summary.last_close != 0:
            target_qty = (target_notional / summary.last_close) if target_notional != 0 else Decimal("0")

        delta_qty = target_qty - position.qty
        if delta_qty == 0:
            return

        side = "buy" if delta_qty > 0 else "sell"
        qty_dec = Decimal("0")

        try:
            qty_dec = Decimal(str(delta_qty)).copy_abs()
            if qty_dec <= 0:
                reject = make_reject("ZERO_OR_NEGATIVE_QTY", "Execution skipped")
                try:
                    await emit_trade_decision(
                        session,
                        {
                            "created_now_utc": datetime.now(timezone.utc),
                            "account_id": str(account_id),
                            "strategy_id": str(strategy_config_id) if strategy_config_id is not None else None,
                            "strategy_config_id": strategy_config_id,
                            "symbol_in": symbol,
                            "symbol_normalized": symbol_norm,
                            "side": side,
                            "qty_requested": str(qty_dec),
                            "status": "skipped",
                            "rationale": "Execution skipped: zero or negative qty",
                            "inputs": {
                                "strategy_thresholds": None,
                                "timeframe": self.config.timeframe,
                                "symbols": [symbol_norm],
                                "price_source": "engine",
                                "market_price_used": str(summary.last_close),
                            },
                            "policy_source": "strategy_legacy" if strategy_config_id is not None else "account_default",
                            "policy_binding": {"strategy_config_id": strategy_config_id},
                            "computed_limits": {},
                            "result": {"reject": reject},
                        },
                    )
                except Exception:
                    logger.exception("Failed to emit trade decision for skipped zero qty")
                await log_reject_throttled(
                    action="ORDER_SKIPPED",
                    account_id=account_id,
                    symbol=symbol_norm,
                    reject=reject,
                    message="Execution skipped",
                    meta={
                        "account_id": account_id,
                        "symbol": symbol_norm,
                        "qty": str(delta_qty),
                        "reason": "ZERO_OR_NEGATIVE_QTY",
                    },
                    window_seconds=120,
                )
                return

            result = await place_order_unified(
                session,
                account_id=account_id,
                symbol=symbol,
                side=side,
                qty=qty_dec,
                strategy_id=strategy_config_id,
                market_price=summary.last_close,
                market_price_source="engine",
                fee_bps=self.config.fee_bps,
                slippage_bps=self.config.slippage_bps,
                meta={"origin": "engine"},
                reject_action_type="ORDER_SKIPPED",
                reject_window_seconds=120,
            )
            if isinstance(result, RejectReason):
                return
            fill = result.execution
        except Exception as exc:  # safety net to keep engine alive
            logger.exception("Execution failed; skipping symbol", extra={"symbol": symbol_norm})
            reject = make_reject(exc.__class__.__name__, "Execution skipped", {"error": str(exc)})
            try:
                await emit_trade_decision(
                    session,
                    {
                        "created_now_utc": datetime.now(timezone.utc),
                        "account_id": str(account_id),
                        "strategy_id": str(strategy_config_id) if strategy_config_id is not None else None,
                        "strategy_config_id": strategy_config_id,
                        "symbol_in": symbol,
                        "symbol_normalized": symbol_norm,
                        "side": side,
                        "qty_requested": str(qty_dec if qty_dec > 0 else Decimal("0")),
                        "status": "skipped",
                        "rationale": f"{exc.__class__.__name__}: {exc}",
                        "inputs": {
                            "strategy_thresholds": None,
                            "timeframe": self.config.timeframe,
                            "symbols": [symbol_norm],
                            "price_source": "engine",
                            "market_price_used": str(summary.last_close),
                        },
                        "policy_source": "strategy_legacy" if strategy_config_id is not None else "account_default",
                        "policy_binding": {"strategy_config_id": strategy_config_id},
                        "computed_limits": {},
                        "result": {"reject": reject},
                    },
                )
            except Exception:
                logger.exception("Failed to emit trade decision for execution exception")
            await log_reject_throttled(
                action="ORDER_SKIPPED",
                account_id=account_id,
                symbol=symbol_norm,
                reject=reject,
                message="Execution skipped",
                meta={
                    "symbol": symbol_norm,
                    "side": side,
                    "qty": str(delta_qty),
                    "error": str(exc),
                    "error_type": exc.__class__.__name__,
                    "account_id": account_id,
                },
                window_seconds=120,
            )
            return

        logger.info(
            "Paper trade",
            extra={
                "symbol": symbol,
                "bias": advice.bias,
                "confidence": advice.confidence,
                "size_score": advice.size_score,
                "leverage_score": advice.leverage_score,
                "target_notional": str(target_notional),
                "order_qty": str(delta_qty),
                "fill_price": fill.costs.get("fill_price"),
                "fee": fill.costs.get("fee_usd"),
                "equity": str(await self._compute_equity(session, balance, account_id)),
            },
        )

    async def _get_or_create_position(self, session: AsyncSession, account_id: int, symbol: str) -> PaperPosition:
        symbol_norm = normalize_symbol(symbol)
        position = await session.scalar(
            select(PaperPosition).where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol_norm)
        )
        if not position and symbol_norm != symbol:
            position = await session.scalar(
                select(PaperPosition).where(PaperPosition.account_id == account_id, PaperPosition.symbol == symbol)
            )
            if position:
                position.symbol = symbol_norm
        if position:
            return position
        position = PaperPosition(
            account_id=account_id,
            symbol=symbol_norm,
            side="long",
            qty=Decimal("0"),
            avg_entry_price=Decimal("0"),
            unrealized_pnl=Decimal("0"),
        )
        session.add(position)
        await session.flush()
        return position

    async def _log_engine_error(
        self,
        session: AsyncSession,
        *,
        account_id: int | None,
        symbol: str | None,
        exc: Exception,
    ) -> None:
        await add_action_deduped(
            session,
            action="ENGINE_ERROR",
            status="alert",
            message="Engine error",
            meta={
                "account_id": account_id,
                "symbol": normalize_symbol(symbol) if symbol else None,
                "error_type": exc.__class__.__name__,
                "error": str(exc),
            },
        )

    async def _fetch_advice(self, summary) -> Optional[AdviceResponse]:
        cfg = self.llm_service.config
        available_providers = {
            "deepseek": bool(cfg.deepseek_api_key and cfg.deepseek_api_key.strip()),
            "openai": bool(cfg.openai_api_key and cfg.openai_api_key.strip()),
        }
        preferred = cfg.provider_default if available_providers.get(cfg.provider_default, False) else None
        provider = preferred or next((p for p, ok in available_providers.items() if ok), None)

        if provider is None:
            logger.warning("No LLM provider configured; skipping advice")
            return None

        try:
            perf = PerformanceSummary(recent_trades=[], metrics={})
            ms = AdviceMarketSummary(
                symbol=summary.symbol,
                timeframe=summary.timeframe,
                recent_stats={
                    "return_24h": str(summary.return_24h),
                    "volatility_24h": str(summary.volatility_24h),
                    "trend": summary.trend,
                    "last_close": str(summary.last_close),
                },
            )
            req = AdviceRequest(
                symbol=summary.symbol,
                timeframe=summary.timeframe,
                market_summary=ms,
                performance_summary=perf,
                provider=provider,  # ensure we choose a configured provider
            )
            return await self.llm_service.get_advice(req)
        except HTTPException as exc:
            logger.warning(
                "LLM advice failed; skipping symbol",
                extra={"symbol": summary.symbol, "provider": provider, "status_code": exc.status_code},
            )
            return None
        except Exception:
            logger.warning(
                "LLM advice failed; skipping symbol",
                extra={"symbol": summary.symbol, "provider": provider},
                exc_info=False,
            )
            return None

    async def _compute_equity(self, session: AsyncSession, balance: PaperBalance, account_id: int) -> Decimal:
        unrealized_total = Decimal("0")
        positions = await session.execute(select(PaperPosition).where(PaperPosition.account_id == account_id))
        for pos in positions.scalars().all():
            unrealized_total += pos.unrealized_pnl or Decimal("0")
        return (balance.available or Decimal("0")) + unrealized_total

    async def _snapshot_equity(self, session: AsyncSession, account_id: int, balance: PaperBalance, equity: Decimal) -> None:
        unrealized_total = Decimal("0")
        positions = await session.execute(select(PaperPosition).where(PaperPosition.account_id == account_id))
        for pos in positions.scalars().all():
            unrealized_total += pos.unrealized_pnl or Decimal("0")

        snapshot = EquitySnapshot(
            account_id=account_id,
            equity=equity,
            balance=balance.available,
            unrealized_pnl=unrealized_total,
        )
        session.add(snapshot)
