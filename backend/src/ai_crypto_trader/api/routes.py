from datetime import datetime, timezone

from fastapi import APIRouter
from pydantic import BaseModel

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
