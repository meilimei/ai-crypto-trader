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
from ai_crypto_trader.services.paper_trader.config import PaperTraderConfig
from ai_crypto_trader.services.paper_trader.order_entry import place_order_unified
from ai_crypto_trader.services.paper_trader.rejects import RejectReason, log_reject_throttled, make_reject
from ai_crypto_trader.services.paper_trader.accounting import normalize_symbol
from ai_crypto_trader.services.admin_actions.helpers import add_action_deduped
from ai_crypto_trader.services.paper_trader.market_summary import MarketSummaryBuilder
from ai_crypto_trader.services.paper_trader.risk import clamp_notional, enforce_confidence
from ai_crypto_trader.services.paper_trader.sizing import target_notional_from_advice

logger = logging.getLogger(__name__)


class PaperTradingEngine:
    def __init__(self, config: PaperTraderConfig, stop_event: Optional[asyncio.Event] = None) -> None:
        self.config = config
        self.llm_service = LLMService(LLMConfig.from_env())
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
