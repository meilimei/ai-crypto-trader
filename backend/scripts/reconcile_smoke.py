import argparse
import asyncio

from ai_crypto_trader.common.database import AsyncSessionLocal
from ai_crypto_trader.services.paper_trader.maintenance import reconcile_apply, reconcile_report


async def _run(account_id: int) -> int:
    async with AsyncSessionLocal() as session:
        before = await reconcile_report(session, account_id)
        before_diff = (before.get("summary") or {}).get("diff_count")
        print(f"before diff_count={before_diff}")
        await reconcile_apply(session, account_id, apply_positions=True, apply_balances=True)
        after = await reconcile_report(session, account_id)
        after_diff = (after.get("summary") or {}).get("diff_count")
        print(f"after diff_count={after_diff}")
        return 0 if not after_diff else 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-id", type=int, default=1)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args.account_id)))


if __name__ == "__main__":
    main()
