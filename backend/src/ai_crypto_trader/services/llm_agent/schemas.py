from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, HttpUrl, ConfigDict

Provider = Literal["deepseek", "openai"]
Regime = Literal["trending_up", "trending_down", "choppy", "panic", "unknown"]
Bias = Literal["long", "short", "flat"]


class MarketSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    timeframe: str
    recent_stats: Dict[str, Any]


class PerformanceSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recent_trades: List[Dict[str, Any]]
    metrics: Dict[str, Any]


class AdviceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    timeframe: str
    market_summary: MarketSummary
    performance_summary: PerformanceSummary
    provider: Provider = Field(default="deepseek")


class AdviceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    regime: Regime
    bias: Bias
    size_score: float = Field(ge=0.0, le=1.0)
    leverage_score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    comment: str


class ProviderConfig(BaseModel):
    api_key: Optional[str]
    base_url: HttpUrl
    model: str
    provider: Provider
