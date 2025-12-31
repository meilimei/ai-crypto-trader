import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy import delete

from ai_crypto_trader.common.models import Base, PaperAccount, PaperBalance, PaperPosition, PaperTrade
from ai_crypto_trader.services.paper_trader.maintenance import compute_derived_state, reconcile_report


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def session() -> AsyncSession:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with SessionLocal() as session:
        yield session


async def _seed_account(session: AsyncSession) -> PaperAccount:
    account = PaperAccount(name="paper-test", base_ccy="USDT", initial_cash_usd=Decimal("10000"))
    session.add(account)
    await session.flush()
    return account


async def _seed_trades(session: AsyncSession, account_id: int, trades: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    for idx, t in enumerate(trades):
        session.add(
            PaperTrade(
                account_id=account_id,
                symbol=t["symbol"],
                side=t["side"],
                qty=Decimal(str(t["qty"])),
                price=Decimal(str(t["price"])),
                fee=Decimal(str(t.get("fee", "0"))),
                created_at=now + timedelta(seconds=idx),
            )
        )
    await session.flush()


async def _apply_derived_to_db(session: AsyncSession, account_id: int, derived: dict) -> None:
    await session.execute(delete(PaperPosition).where(PaperPosition.account_id == account_id))
    await session.execute(delete(PaperBalance).where(PaperBalance.account_id == account_id))
    for sym, pdata in derived.get("positions_by_symbol", {}).items():
        session.add(
            PaperPosition(
                account_id=account_id,
                symbol=sym,
                side="long",
                qty=Decimal(str(pdata["qty"])),
                avg_entry_price=Decimal(str(pdata["avg_entry_price"])),
                unrealized_pnl=Decimal("0"),
            )
        )
    session.add(
        PaperBalance(
            account_id=account_id,
            ccy="USDT",
            available=Decimal(str(derived["balances_by_asset"]["USDT"])),
        )
    )
    await session.flush()


@pytest.mark.asyncio
async def test_reconcile_positions_ok(session: AsyncSession):
    cases = [
        [
            {"symbol": "ETH/USDT", "side": "buy", "qty": "1", "price": "100", "fee": "1"},
        ],
        [
            {"symbol": "ETH/USDT", "side": "sell", "qty": "1", "price": "100", "fee": "1"},
            {"symbol": "ETH/USDT", "side": "buy", "qty": "0.4", "price": "95", "fee": "0.5"},
        ],
        [
            {"symbol": "ETH/USDT", "side": "buy", "qty": "1", "price": "100", "fee": "1"},
            {"symbol": "ETH/USDT", "side": "buy", "qty": "1", "price": "110", "fee": "1"},
        ],
        [
            {"symbol": "ETH/USDT", "side": "buy", "qty": "2", "price": "100", "fee": "1"},
            {"symbol": "ETH/USDT", "side": "sell", "qty": "0.5", "price": "120", "fee": "0.5"},
        ],
        [
            {"symbol": "ETH/USDT", "side": "buy", "qty": "1", "price": "100", "fee": "1"},
            {"symbol": "ETH/USDT", "side": "sell", "qty": "1", "price": "110", "fee": "0.5"},
        ],
        [
            {"symbol": "ETH/USDT", "side": "buy", "qty": "1", "price": "100", "fee": "1"},
            {"symbol": "ETH/USDT", "side": "sell", "qty": "2", "price": "90", "fee": "1"},
            {"symbol": "ETH/USDT", "side": "buy", "qty": "1.5", "price": "95", "fee": "1"},
        ],
    ]

    for trades in cases:
        await session.execute(delete(PaperTrade))
        await session.execute(delete(PaperPosition))
        await session.execute(delete(PaperBalance))
        await session.execute(delete(PaperAccount))
        account = await _seed_account(session)
        await _seed_trades(session, account.id, trades)
        await session.commit()

        derived = await compute_derived_state(session, account.id)
        await _apply_derived_to_db(session, account.id, derived)
        await session.commit()

        report = await reconcile_report(session, account.id)
        assert report["ok"] is True
        assert report["summary"]["diff_count"] == 0
