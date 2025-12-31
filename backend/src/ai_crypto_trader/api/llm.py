import logging

from fastapi import APIRouter

from ai_crypto_trader.services.llm_agent.config import LLMConfig
from ai_crypto_trader.services.llm_agent.schemas import AdviceRequest, AdviceResponse
from ai_crypto_trader.services.llm_agent.service import LLMService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["llm"])

_config = LLMConfig.from_env()
_service = LLMService(_config)


@router.post("/advice", response_model=AdviceResponse)
async def generate_advice(payload: AdviceRequest) -> AdviceResponse:
    """
    Generate trading advice from the configured LLM provider.
    """
    return await _service.get_advice(payload)
