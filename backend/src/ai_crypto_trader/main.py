from fastapi import FastAPI

# Import database layer to ensure models are registered and engine is created.
from ai_crypto_trader.common import database  # noqa: F401
from ai_crypto_trader.api.routes import router as api_router


def create_application() -> FastAPI:
    """
    Build the FastAPI application, wiring routers and shared components.
    """
    application = FastAPI(
        title="AI Crypto Trader API",
        version="0.1.0",
        description="Backend for AI-assisted crypto trading with LLM support.",
    )
    application.include_router(api_router, prefix="/api")
    return application


app = create_application()
