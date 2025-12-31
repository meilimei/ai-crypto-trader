import os
from dataclasses import dataclass


@dataclass
class LLMConfig:
    provider_default: str
    deepseek_api_key: str | None
    deepseek_base_url: str
    deepseek_model: str
    openai_api_key: str | None
    openai_base_url: str
    openai_model: str

    @classmethod
    def from_env(cls) -> "LLMConfig":
        return cls(
            provider_default=os.getenv("LLM_PROVIDER_DEFAULT", "deepseek").lower(),
            deepseek_api_key=os.getenv("DEEPSEEK_API_KEY"),
            deepseek_base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            deepseek_model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            openai_base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com"),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        )
