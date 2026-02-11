from ai_crypto_trader.agents.auto_symbol import validate_llm_symbol_payload


def test_validate_llm_symbol_payload_falls_back_on_invalid_symbol() -> None:
    decision = validate_llm_symbol_payload(
        {
            "symbol": "DOGEUSDT",
            "confidence": 0.9,
            "rationale": ["momentum"],
            "signals": {"return_1h": 0.03},
        },
        ["ETHUSDT", "BTCUSDT"],
    )
    assert decision["symbol"] == "ETHUSDT"
    assert decision["selected_symbol"] == "ETHUSDT"
    assert decision["reason"] == "invalid_llm_symbol"
    assert decision["confidence"] == 0.9
