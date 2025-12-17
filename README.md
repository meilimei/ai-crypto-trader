# AI Crypto Trader

AI-assisted crypto trading platform that blends automated trading logic with LLM-driven decision support. This monorepo hosts the frontend (Next.js), backend (FastAPI), infrastructure assets, and shared documentation for running and evolving the system in production.

## Architecture
- Monorepo layout with `frontend/` (Next.js + TypeScript) and `backend/` (FastAPI + Python) plus shared docs and infra.
- Backend organized into services: `trading_engine`, `data_collector`, `llm_agent`, and `common` utilities within a single FastAPI app.
- PostgreSQL as the system of record; exposed via docker-compose in `infra/`.
- Frontend dashboard surfaces health and trading insights; communicates with FastAPI APIs.
- Documentation under `docs/` for product context, architecture decisions, APIs, LLM prompts, and risk policy.

## Getting Started
Prereqs: Node.js (>=18), npm (or pnpm), Python 3.12+, Poetry, Docker (for containerized runs).

### Development without docker-compose
1) Backend  
```bash
cd backend
poetry install
poetry run uvicorn ai_crypto_trader.main:app --reload --host 0.0.0.0 --port 8000
```
Ensure you have a PostgreSQL instance available and configure environment variables (e.g., `DATABASE_URL`) before starting.

2) Frontend  
```bash
cd frontend
npm install
npm run dev
```

### Development with docker-compose
```bash
cd infra
docker-compose up --build
```
This starts PostgreSQL and the backend service. Run the frontend separately with `npm run dev` to leverage hot reload while containers run in the background.

## Contribution Workflow
- Use feature branches off `main`; prefer short-lived branches (e.g., `feature/add-order-endpoint`).
- Open PRs early for feedback; ensure linting/tests pass before requesting review.
- Keep commits scoped and descriptive; rebase onto `main` to stay current.
- Update relevant docs in `docs/` when adding features or changing behavior.

## Repository Structure
- `frontend/` — Next.js App Router dashboard scaffold.
- `backend/` — FastAPI application with service modules for trading, data collection, and LLM orchestration.
- `infra/` — docker-compose stack for PostgreSQL and backend.
- `docs/` — product, architecture, API, prompt, and risk policy references.
- `.gitignore` — ignores common Node/Python artifacts.
