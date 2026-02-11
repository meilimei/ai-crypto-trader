# OpenClaw Autopilot Plan (Backend)

## Executive Summary
Autopilot in this system means moving from manual, per-request trading to an orchestrated loop that can continuously:
1. select symbols,
2. plan trades,
3. preview and enforce policy/risk,
4. execute via the existing paper execution pipeline,
5. evaluate outcomes,
6. adapt universe and strategy behavior,
7. notify operators on exceptions.

Autonomy levels:
- `L0 Manual`: operator triggers smoke/test endpoints.
- `L1 Assisted`: agent proposes (`dry_run`), operator approves execution.
- `L2 Guardrailed Auto`: agent executes automatically but only inside strict policy constraints and budget controls.
- `L3 Adaptive Auto`: agent can change universe and policy versions within pre-approved envelopes.
- `L4 Self-Optimizing`: agent runs periodic experiments and promotes rollouts automatically with confidence gates.

Target for next milestone: stable `L2` with complete observability and deterministic rollback.

## Reuse vs Refactor
Reuse as foundation:
- Binding-first policy resolution with legacy fallback.
- Per-symbol risk and position overrides.
- Pre-trade hard checks in shared execution path.
- Runner loop patterns and per-loop tick audit events.
- `admin_actions` event stream and explainability events (`TRADE_DECISION`, `TRADE_OUTCOME`).
- Notifications outbox + dispatcher (`log`/`webhook`).
- Existing admin endpoints for policies, monitoring, explainability, universe, and metrics.

Refactor/extend:
- Introduce a first-class orchestration state machine across existing loops.
- Standardize decision payload contract and idempotency keys.
- Move from endpoint-specific agent logic to shared tool-based agent pipeline.
- Add decision ledger v2 for high-volume queryability (migration path below).
- Unify metrics computation with one reusable strategy-pair aggregator.

## Multi-Agent Architecture
| Agent | Responsibilities | Inputs | Outputs | Failure Modes | Guardrails |
|---|---|---|---|---|---|
| `UniverseScout` | Build ranked candidate symbol set per strategy | strategy symbols, thresholds, candles, outcomes | `universe_candidates[]`, rationale, freshness | stale candles, empty universe, invalid symbol normalization | fallback to previous symbols, min-candidate floor, explicit skip reason |
| `MarketAnalyst` | Summarize regime/features per symbol | candles, volatility, returns, optional external sources | compact feature snapshot | missing data, outlier spikes | strict freshness checks, capped feature ranges |
| `TradePlanner` | Propose side/qty/priority and rationale | universe + features + policy context | proposed action JSON | invalid JSON, forbidden symbol, nonsensical qty | strict schema validation, allowlist-only symbols |
| `RiskGovernor` | Authoritative approval/rejection | proposal + effective policies + account state | approved/rejected with reject code/details | policy resolution gaps, stale equity/positions | binding-first fallback, conservative reject-on-uncertainty |
| `Executor` | Place paper order/trade through existing path | approved order intent | order/trade ids, execution snapshot | retries, transient DB/API issues | idempotency key, retry budget, reconcile checks |
| `Auditor` | Validate post-trade and accounting sanity | trades/orders/positions/equity/reconcile logs | anomalies, remediation events | drift mismatch, delayed updates | alert + auto-pause thresholds |
| `Reporter` | Produce explainability/performance summaries | decisions + outcomes + metrics | dashboards, periodic reports, alerts | noisy metrics, window bias | minimum sample thresholds, confidence annotations |

## Orchestrator Design
Core loop model:

```text
SCHEDULED_TICK
  -> BUILD_UNIVERSE
  -> ANALYZE_MARKET
  -> PLAN_TRADE
  -> PREVIEW_POLICIES
  -> ACT (optional by autonomy mode)
  -> RECONCILE
  -> EVALUATE_OUTCOME
  -> PUBLISH_METRICS_AND_ALERTS
  -> DONE
```

State machine per proposal:

```text
proposed -> preview_rejected -> terminal
proposed -> approved -> executed -> reconciled -> outcome_evaluated -> terminal
proposed -> approved -> execution_failed -> terminal
```

Idempotency keys:
- Proposal key: `autopilot:{account_id}:{strategy_config_id}:{symbol_normalized}:{tick_bucket}`
- Execution key: reuse existing dedupe/order intent keying.
- Outcome key: `decision_key + horizon_seconds`.

Tool interfaces (service functions):
- `build_universe(...)`
- `analyze_market(...)`
- `plan_trade(...)`
- `resolve_effective_policy_snapshot(...)`
- `validate_order(...)`
- `execute_order(...)`
- `evaluate_outcome(...)`

Scheduling:
- Keep existing runner loops; add orchestrator loop that dispatches one cycle per active strategy.
- Use explicit interval config and per-strategy jitter to avoid stampedes.

## Data Sources Roadmap
Required now:
- Candles (`public.candles`) with symbol normalization (`BTC/USDT` <-> `BTCUSDT`).
- Internal decisions/outcomes (`admin_actions` events).
- Account/position/equity state from existing paper trading tables.

Optional next:
- Order book snapshots (microstructure-aware sizing).
- Funding/open interest (perpetuals context).
- News/sentiment event stream.

Optional sources should be additive. Planner must degrade gracefully when absent.

## Decision Ledger v2 (Explainability)
Current durable store is `admin_actions` with JSON meta, which is effective for audit but increasingly expensive for deep analytics.

Proposed v2 schema (future migration, additive):
- `autopilot_decisions`
  - `id`, `decision_key`, `account_id`, `strategy_config_id`, `symbol_normalized`, `status`, `created_at`
  - `policy_source`, `policy_binding_json`, `limits_json`, `features_json`, `rationale_json`
  - `proposal_json`, `execution_json`, `reject_json`
- `autopilot_outcomes`
  - `id`, `decision_key`, `horizon_seconds`, `evaluated_at`, `return_pct_signed`, `pnl_usdt_est`, `win`, `outcome_json`
- `autopilot_model_calls`
  - `id`, `decision_key`, `model_provider`, `model_name`, `prompt_hash`, `latency_ms`, `token_usage_json`, `cost_usd_est`, `created_at`
- `autopilot_universe_snapshots`
  - `id`, `strategy_config_id`, `candidates_json`, `selected_json`, `scoring_json`, `picked_by`, `created_at`

Migration strategy:
1. Phase A: keep `admin_actions` as source of truth.
2. Phase B: dual-write (`admin_actions` + v2 tables).
3. Phase C: switch read APIs to v2 with fallback to `admin_actions`.
4. Phase D: retain `admin_actions` as immutable audit trail; v2 as primary query layer.

## Performance and Health Metrics
Decision quality:
- decision acceptance rate
- reject distribution by code
- policy-driven reject ratio vs data-driven skip ratio

Outcome quality:
- win rate, avg return, expectancy, total pnl
- per-symbol and per-strategy performance
- horizon-specific performance buckets (5m/15m/60m)

Execution quality:
- slippage vs intended price
- fee impact
- reconcile mismatch frequency

Risk quality:
- drawdown profile
- leverage utilization
- exposure concentration by symbol

## Safety and Controls
Hard controls:
- account-level kill switch
- daily loss cap
- max drawdown cap
- max leverage cap
- max notional/order and max position limits
- symbol allowlist/denylist

Operational controls:
- autonomy mode per strategy (`manual`, `assist`, `auto`)
- shadow mode (`plan+preview`, no execution)
- canary mode (small size cap)
- auto-pause on repeated anomalies

Policy governance controls:
- version promotion flow with rollback endpoints
- immutable policy change audit events
- per-symbol overrides with explicit source in decision records

## Model Strategy
Routing approach:
- Use cheap model for `UniverseScout` and basic summarization.
- Use stronger model for `TradePlanner` only when confidence low or ambiguity high.

Model contract:
- strict JSON schema with required keys
- deterministic parameters (`temperature=0` for planning)
- timeout budget per stage
- retry once on parse failure, then fallback heuristic

Cost and reliability:
- prompt hash for cacheability
- response validator and repair
- token/cost accounting in model call metadata

## Frontend/Admin UI Journey (Backend-facing)
Core pages:
- Autopilot Control
- Universe
- Decisions
- Outcomes
- Policy Governance
- Alerts + Notifications
- Performance

Backend implications:
- Cursor/offset pagination with deterministic sorting.
- Filter dimensions: `account_id`, `strategy_config_id`, `symbol_normalized`, `status`, time windows.
- Detail endpoints should return linked objects: decision -> policy snapshot -> outcome -> alerts.

## Phased Roadmap
Phase 0 (stabilize existing):
- Unify explainability payload schema fields.
- Standardize list/summary endpoints and typed query filters.
- Exit criteria: all explainability endpoints operational and no ambiguous parameter errors.

Phase 1 (agentic planning without auto-execution):
- Introduce Plan -> Preview API flow and UniverseScout scoring.
- Persist structured decision proposals and reasons.
- Exit criteria: operators can run dry-run continuously with reliable summaries.

Phase 2 (guardrailed auto-execution):
- Enable autonomous execution in runner for selected strategies.
- Full RiskGovernor enforcement and throttle controls.
- Exit criteria: auto mode can run with zero manual intervention during normal conditions.

Phase 3 (decision ledger v2 + model routing):
- Dual-write to dedicated ledger tables.
- Multi-model routing + cost controls.
- Exit criteria: analytics queries no longer depend on JSON scans in `admin_actions`.

Phase 4 (adaptive optimization):
- Policy recommendation engine.
- Automated canary promotion and rollback.
- Exit criteria: policy lifecycle managed with measurable uplift and bounded risk.

## Operational Readiness Checklist
- Every loop emits tick event and structured log.
- Every mutation has rollback path and admin action audit.
- Every decision has deterministic key and linked outcome path.
- Every alert has throttle + dedupe.
- Every API list endpoint has stable ordering + pagination + total count.
