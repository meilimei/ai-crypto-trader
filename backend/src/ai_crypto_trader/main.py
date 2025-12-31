from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


# Import database layer to ensure models are registered and engine is created.
from ai_crypto_trader.common.env import init_env
init_env()
from ai_crypto_trader.common import database  # noqa: F401
from ai_crypto_trader.api.routes import router as api_router
from ai_crypto_trader.api.llm import router as llm_router
from ai_crypto_trader.api.candles import router as candles_router
from ai_crypto_trader.api.paper_trader import router as paper_router
from ai_crypto_trader.api.paper_trader_dashboard import router as paper_trader_dashboard_router
from ai_crypto_trader.api.admin_paper_trader import router as admin_paper_trader_router, admin_router as admin_actions_router
from ai_crypto_trader.api.admin_config import router as admin_config_router
from ai_crypto_trader.api.admin_db import router as admin_db_router


def create_application() -> FastAPI:
    """
    Build the FastAPI application, wiring routers and shared components.
    """
    application = FastAPI(
        title="AI Crypto Trader API",
        version="0.1.0",
        description="Backend for AI-assisted crypto trading with LLM support.",
    )

    origins = [
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ]

    application.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.include_router(api_router, prefix="/api")
    application.include_router(llm_router, prefix="/api")
    application.include_router(candles_router, prefix="/api")
    application.include_router(paper_router, prefix="/api")
    application.include_router(paper_trader_dashboard_router, prefix="/api")
    application.include_router(admin_paper_trader_router, prefix="/api")
    application.include_router(admin_actions_router, prefix="/api")
    application.include_router(admin_config_router, prefix="/api")
    application.include_router(admin_db_router, prefix="/api")
    application.add_api_route("/health", lambda: {"status": "ok"}, methods=["GET"], tags=["system"])
    return application


app = create_application()
