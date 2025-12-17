from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ai_crypto_trader.common.database import get_db_session
from ai_crypto_trader.common.models import User

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime


@router.get("/health", response_model=HealthResponse, tags=["system"])
def health_check() -> HealthResponse:
    """
    Lightweight readiness probe for the API service.
    """
    return HealthResponse(status="ok", timestamp=datetime.now(tz=timezone.utc))


@router.get("/health/db", tags=["system"])
async def database_health(session: AsyncSession = Depends(get_db_session)) -> dict:
    """
    Verify database connectivity using the configured async engine.
    """
    await session.execute(text("SELECT 1"))
    return {"database": "ok"}


@router.get("/dev/users/count", tags=["system"])
async def user_count(session: AsyncSession = Depends(get_db_session)) -> dict:
    """
    Simple query using ORM to validate database wiring.
    """
    count = await session.scalar(select(func.count()).select_from(User))
    return {"users": count or 0}
