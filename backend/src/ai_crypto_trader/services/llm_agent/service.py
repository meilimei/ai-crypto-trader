import json
import logging
import re
import time
from typing import Dict, List

from fastapi import HTTPException, status

from ai_crypto_trader.common.env import init_env
from ai_crypto_trader.services.llm_agent.clients import OpenAICompatibleClient, build_provider_client
from ai_crypto_trader.services.llm_agent.config import LLMConfig
from ai_crypto_trader.services.llm_agent.schemas import AdviceRequest, AdviceResponse, Provider

init_env()

logger = logging.getLogger(__name__)


class LLMService:
    def __init__(self, config: LLMConfig) -> None:
        self.config = config
        self.clients: Dict[Provider, OpenAICompatibleClient] = {
            "deepseek": build_provider_client(
                "deepseek",
                api_key=config.deepseek_api_key,
                base_url=config.deepseek_base_url,
                model=config.deepseek_model,
            ),
            "openai": build_provider_client(
                "openai",
                api_key=config.openai_api_key,
                base_url=config.openai_base_url,
                model=config.openai_model,
            ),
        }
        self._rate_limit_window = 60.0  # seconds
        self._rate_limit_max = 20
        self._request_timestamps: Dict[Provider, List[float]] = {"deepseek": [], "openai": []}

    async def get_advice(self, payload: AdviceRequest) -> AdviceResponse:
        provider: Provider = payload.provider or self._provider_default()
        self._enforce_rate_limit(provider)

        logger.info(
            "LLM advice request",
            extra={"provider": provider, "symbol": payload.symbol, "timeframe": payload.timeframe},
        )

        client = self.clients.get(provider)
        if client is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported provider: {provider}"
            )

        messages = self._build_messages(payload)
        content = await client.chat_completion(messages)
        parsed = self._parse_response_content(content)

        try:
            response = AdviceResponse.model_validate(parsed)
        except Exception:
            logger.error("LLM response validation failed", extra={"provider": provider, "raw": parsed})
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="LLM provider returned an invalid payload",
            )
        return response

    def _provider_default(self) -> Provider:
        default = self.config.provider_default.lower()
        return "deepseek" if default not in {"deepseek", "openai"} else default  # type: ignore

    def _build_messages(self, payload: AdviceRequest) -> List[Dict[str, str]]:
        system_prompt = (
            "You are an expert crypto trading advisor. Respond ONLY with a valid JSON object "
            "matching the schema: {regime, bias, size_score, leverage_score, confidence, comment}. "
            "Scores are between 0 and 1. Regime options: trending_up, trending_down, choppy, panic, unknown. "
            "Bias options: long, short, flat. Comment should be concise (<=240 chars)."
        )
        user_content = (
            f"Symbol: {payload.symbol}\n"
            f"Timeframe: {payload.timeframe}\n"
            f"Market summary: {json.dumps(payload.market_summary.model_dump())}\n"
            f"Performance summary: {json.dumps(payload.performance_summary.model_dump())}"
        )
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

    def _parse_response_content(self, content: str) -> Dict[str, object]:
        # Try direct JSON parse
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # Attempt to extract JSON object from text/code fences
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                logger.warning("LLM returned malformed JSON snippet")

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="LLM provider returned non-JSON content",
        )

    def _enforce_rate_limit(self, provider: Provider) -> None:
        now = time.time()
        window_start = now - self._rate_limit_window
        timestamps = self._request_timestamps[provider]
        # Drop old entries
        self._request_timestamps[provider] = [t for t in timestamps if t >= window_start]
        if len(self._request_timestamps[provider]) >= self._rate_limit_max:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded for provider {provider}",
            )
        self._request_timestamps[provider].append(now)
from ai_crypto_trader.common.env import init_env

init_env()
