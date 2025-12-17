# AI Crypto Trader Backend

FastAPI application that exposes APIs for trading orchestration, data collection, and LLM-assisted decision support. Entry point: `ai_crypto_trader.main:app`.

## Database
- Configure `DATABASE_URL` (SQLite dev example: `sqlite+aiosqlite:///./dev.db`).
- Initialize schema for quick local work: `poetry run python -m ai_crypto_trader.init_db`.
- Or run migrations (recommended as the schema evolves): `poetry run alembic upgrade head`.
