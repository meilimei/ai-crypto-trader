import logging
import os
_deepseek_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_KEY")
if _deepseek_key and not os.getenv("DEEPSEEK_API_KEY"):
    os.environ["DEEPSEEK_API_KEY"] = _deepseek_key
from typing import Any, Dict, List

import httpx
from fastapi import HTTPException, status

from ai_crypto_trader.services.llm_agent.schemas import Provider, ProviderConfig

logger = logging.getLogger(__name__)


class OpenAICompatibleClient:
    """
    Minimal async client for OpenAI-compatible chat completions endpoints.
    """

    def __init__(self, config: ProviderConfig) -> None:
        self.config = config
        if config.provider == "deepseek":
            api_key = config.api_key or os.getenv("DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_KEY")
        else:
            api_key = config.api_key
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}" if api_key else "",
            "Content-Type": "application/json",
        }

    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        *,
        temperature: float | None = None,
        timeout_seconds: float | None = None,
    ) -> str:
        if not self.api_key:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"{self.config.provider} API key is not configured",
            )

        payload = {
            "model": self.config.model,
            "messages": messages,
            "response_format": {"type": "json_object"},
            "temperature": 0.2 if temperature is None else float(temperature),
        }

        try:
            async with httpx.AsyncClient(
                base_url=str(self.config.base_url),
                headers=self.headers,
                timeout=30.0 if timeout_seconds is None else float(timeout_seconds),
            ) as client:
                response = await client.post("/v1/chat/completions", json=payload)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.error(
                "LLM provider HTTP error",
                extra={"provider": self.config.provider, "status_code": exc.response.status_code},
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"{self.config.provider} returned an error",
            )
        except httpx.HTTPError:
            logger.exception("LLM provider transport error", extra={"provider": self.config.provider})
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"{self.config.provider} is unreachable",
            )

        data: Dict[str, Any] = response.json()
        choices = data.get("choices") or []
        if not choices:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"{self.config.provider} returned no choices",
            )

        message = choices[0].get("message") or {}
        content = message.get("content")
        if not content or not isinstance(content, str):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"{self.config.provider} returned an invalid response",
            )
        return content


def build_provider_client(
    provider: Provider,
    *,
    api_key: str | None,
    base_url: str,
    model: str,
) -> OpenAICompatibleClient:
    cfg = ProviderConfig(api_key=api_key, base_url=base_url, model=model, provider=provider)
    return OpenAICompatibleClient(cfg)
