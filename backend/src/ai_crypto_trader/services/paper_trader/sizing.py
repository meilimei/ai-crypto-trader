from decimal import Decimal

from ai_crypto_trader.services.llm_agent.schemas import AdviceResponse


def target_notional_from_advice(
    balance: Decimal,
    advice: AdviceResponse,
    max_leverage: Decimal,
) -> Decimal:
    size_score = Decimal(str(advice.size_score))
    leverage_score = Decimal(str(advice.leverage_score))

    base = balance * size_score
    leverage_factor = Decimal("1") + leverage_score * (max_leverage - Decimal("1"))
    notional = base * leverage_factor

    if advice.bias == "long":
        return notional
    if advice.bias == "short":
        return -notional
    return Decimal("0")
