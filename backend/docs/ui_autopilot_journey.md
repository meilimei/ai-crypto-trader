# UI Autopilot Journey

## Personas
`Solo Trader`
- Runs one or a few strategies.
- Wants rapid setup and confidence that guardrails are active.
- Uses summaries and alerts more than raw logs.

`Operator`
- Manages multiple strategies/accounts.
- Needs queue-level visibility, rollback, and incident workflows.
- Uses filtering, drill-down, and audit trails heavily.

## Onboarding Flow
1. Open `Autopilot Setup` page.
2. Choose account and strategy config.
3. Select autonomy mode (`assist` first, then `auto`).
4. Configure universe mode:
- manual symbols, or
- auto universe (`AUTO` / `auto_universe=true`) with candidate limits.
5. Configure risk budget and policy bindings:
- risk policy version,
- position policy version,
- per-symbol overrides.
6. Run `Preview` to validate decisions and rejects without execution.
7. Start autopilot.

Expected backend calls:
- `GET /api/admin/strategy-configs/{id}/effective-policies?symbol=...`
- `GET /api/admin/universe/history`
- `POST /api/admin/strategy-policy-bindings/upsert`
- `POST /api/admin/paper-trader/agent/decide` (dry-run)

## Daily Use Flow
1. Open `Autopilot Dashboard`.
2. Review strategy health cards:
- latest decision status,
- latest outcome metrics,
- active alerts.
3. Drill into `Decisions` table:
- inspect rationale, policy snapshot, reject details.
4. Drill into `Outcomes` table:
- validate win/loss and pnl estimates.
5. If needed, adjust per-symbol limits or rollback policy version.
6. Confirm recovery in next ticks.

## Emergency Flow
Trigger condition examples:
- drawdown breach,
- repeated `MAX_LEVERAGE` rejects,
- reconcile anomalies.

Flow:
1. System auto-pauses affected strategy.
2. Notification emitted (webhook/log).
3. Operator lands on `Incident` panel with root-cause summary.
4. Operator actions:
- keep paused,
- rollback policy version,
- reduce exposure limits,
- resume with canary mode.
5. Post-incident report generated from decisions + outcomes + alerts.

## Screen Inventory
`Autopilot Dashboard`
- KPIs: decisions/hour, executed ratio, win rate, pnl, drawdown.
- Filters: account, strategy, symbol, window.

`Decisions`
- Table columns:
- decision key
- created time
- account_id
- strategy_config_id
- symbol_normalized
- side, qty
- status
- policy_source
- reject code
- confidence
- linked outcome
- Filters: status, symbol, strategy, time window, reject code.

`Decision Detail`
- Sections:
- proposal JSON
- features snapshot
- policy binding and computed limits
- execution result or reject details
- model metadata (provider/model/prompt hash/cost)

`Outcomes`
- Table columns:
- decision key
- horizon
- evaluated time
- win
- return_pct_signed
- pnl_usdt_est
- Filters: symbol, strategy, min pnl, win/loss, horizon.

`Universe`
- Current symbols per strategy.
- Candidate ranking and reasons.
- Selection history with rollback action.

`Policies`
- Risk versions, position versions.
- Active/draft status.
- Per-symbol override editor (batch and single update).
- Bindings and rollback controls.

`Alerts`
- `STRATEGY_ALERT`, `STRATEGY_MONITOR_TICK`, `STRATEGY_METRICS_TICK` stream.
- Filters: type, severity/status, account, strategy, symbol, since.

## UX/Backend Contract Expectations
- All list views require `{items, total, limit, offset}`.
- Sorting must be deterministic (`created_at DESC, id ASC` or equivalent tie-breaker).
- All detail endpoints should include normalized symbol fields and policy ids.
- UI should not infer policy math; backend returns computed limits and sources.

## Rollout UX Strategy
Stage 1 (`assist`):
- show proposal and preview rejection reasons before execution.

Stage 2 (`auto`):
- enable execution with visible guardrail status and instant rollback.

Stage 3 (`adaptive`):
- surface recommendation widgets for policy promotion and universe changes, with explicit operator approval gates.
