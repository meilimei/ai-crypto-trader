# AI Crypto Trader Backend

FastAPI application that exposes APIs for trading orchestration, data collection, and LLM-assisted decision support. Entry point: `ai_crypto_trader.main:app`.

## Database
- Configure `DATABASE_URL` (SQLite dev example: `sqlite+aiosqlite:///./dev.db`).
- Initialize schema for quick local work: `poetry run python -m ai_crypto_trader.init_db`.
- Or run migrations (recommended as the schema evolves): `poetry run alembic upgrade head`.

### PostgreSQL note for paper trader
If you migrated from SQLite, ensure Postgres sequences for `paper_*` tables are in sync by calling:
`POST /api/admin/db/fix-paper-sequences` once after migration (via curl or your admin UI).
If you see primary key or null-id errors on any tables after migration, run `POST /api/admin/db/fix-id-sequences`
to attach and sync sequences for integer/bigint `id` columns.
