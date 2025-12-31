from fastapi import APIRouter

from ai_crypto_trader.common.maintenance import (
    ensure_and_sync_paper_id_sequences,
    fix_all_id_sequences,
)

router = APIRouter(prefix="/admin/db", tags=["admin"])


@router.post("/sync-sequences")
async def sync_sequences() -> dict:
    updated = await ensure_and_sync_paper_id_sequences()
    return {"updated": updated}


@router.post("/fix-paper-sequences")
async def fix_paper_sequences() -> dict:
    updated = await ensure_and_sync_paper_id_sequences()
    return {"updated": updated}


@router.post("/fix-id-sequences")
async def fix_id_sequences() -> dict:
    updated = await fix_all_id_sequences()
    return {"updated": updated}
