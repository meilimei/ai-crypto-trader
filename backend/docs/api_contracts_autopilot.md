# API Contracts: Autopilot (Proposed)

## Conventions
- Auth header: `X-Admin-Token: <token>`
- Content type: `application/json`
- Time fields: ISO-8601 UTC strings
- Symbol field in responses: `symbol_normalized`
- Pagination default:
- `limit=50`, max `limit=200`
- `offset=0`
- Common list response:

```json
{
  "items": [],
  "total": 0,
  "limit": 50,
  "offset": 0
}
```

- Common error shape:

```json
{
  "detail": "human readable error"
}
```

## Canonical Record Examples

### Decision Record Example
```json
{
  "id": 17321,
  "action": "TRADE_DECISION",
  "created_at": "2026-02-11T15:04:12.445123Z",
  "meta": {
    "decision_key": "d:5:5:ETHUSDT:202602111504",
    "account_id": "5",
    "strategy_config_id": "5",
    "strategy_id": "5",
    "symbol_in": "ETHUSDT",
    "symbol_normalized": "ETHUSDT",
    "side": "buy",
    "qty_requested": "0.001",
    "status": "executed",
    "policy_source": "binding",
    "policy_binding": {
      "strategy_config_id": 5,
      "legacy_risk_policy_id": 2,
      "bound_risk_policy_id": 2,
      "effective_risk_policy_id": 2,
      "bound_position_policy_id": "7f8155e4-c2b6-4ccf-a313-1ca2b2551a67",
      "effective_position_policy_id": "7f8155e4-c2b6-4ccf-a313-1ca2b2551a67"
    },
    "computed_limits": {
      "risk_limits": {
        "max_order_notional_usdt": "10",
        "max_leverage": "2"
      },
      "risk_limit_source": "risk_policy.per_symbol",
      "risk_policy_overrides": {
        "max_order_notional_usdt": 10
      },
      "symbol_limits": {
        "max_position_notional_usdt": "300",
        "max_position_qty": "0.5",
        "max_position_pct_equity": "0.4"
      },
      "symbol_limit_source": "position_policy.default",
      "position_policy_overrides": null
    },
    "market_price": "2931.25",
    "price_source": "latest_quote",
    "result": {
      "order_id": 441,
      "trade_id": 322,
      "executed_qty": "0.001",
      "executed_price": "2931.25"
    }
  }
}
```

### Outcome Record Example
```json
{
  "id": 17388,
  "action": "TRADE_OUTCOME",
  "created_at": "2026-02-11T15:19:20.201001Z",
  "meta": {
    "decision_key": "d:5:5:ETHUSDT:202602111504",
    "account_id": "5",
    "strategy_config_id": "5",
    "symbol_normalized": "ETHUSDT",
    "horizon_seconds": 900,
    "evaluated_at_utc": "2026-02-11T15:19:20.199000Z",
    "eval_price": "2941.55",
    "return_pct_signed": "0.0035",
    "pnl_usdt_est": "0.0103",
    "win": true,
    "decision_admin_action_id": 17321
  }
}
```

## Explainability Endpoints

### GET `/api/admin/explainability/decisions`
- Purpose: list decision events.
- Query:
- `account_id` (required, int)
- `strategy_config_id` (optional, int)
- `symbol` (optional, string)
- `status` (optional, `executed|rejected|skipped|proposed`)
- `window_minutes` (optional, default 1440)
- `limit`, `offset`
- Response: paginated decision records.
- Errors:
- `400` invalid query values
- `401` invalid admin token
- `500` query failure

### GET `/api/admin/explainability/decisions/{decision_key}`
- Purpose: retrieve full detail for one decision key, including latest linked outcome if present.
- Query:
- `account_id` (required, int)
- Response:
```json
{
  "ok": true,
  "decision": {},
  "latest_outcome": {}
}
```
- Errors: `404` not found, `401`, `500`.

### GET `/api/admin/explainability/outcomes`
- Purpose: list outcome events.
- Query: same filter contract as decisions.
- Response: paginated outcome records.
- Errors: `400`, `401`, `500`.

### GET `/api/admin/explainability/summary`
- Purpose: KPI view over decision/outcome window.
- Query:
- `account_id` (required)
- `strategy_config_id` (optional)
- `symbol` (optional)
- `window_minutes` (optional)
- Response:
```json
{
  "ok": true,
  "filters": {
    "account_id": 5,
    "strategy_config_id": 5,
    "symbol": "ETHUSDT"
  },
  "window": {
    "minutes": 1440,
    "since_utc": "2026-02-10T15:00:00Z"
  },
  "decisions": {
    "decisions_total": 42,
    "executed_total": 20,
    "rejected_total": 18,
    "skipped_total": 4
  },
  "outcomes": {
    "outcomes_total": 17,
    "win_rate": 0.5882,
    "avg_return_pct_signed": 0.0012,
    "total_pnl_usdt_est": 2.31,
    "avg_pnl_usdt_est": 0.1359
  }
}
```
- Errors: `400`, `401`, `500`.

### POST `/api/admin/explainability/emit-outcomes`
- Purpose: run one outcome evaluation tick.
- Body:
```json
{
  "limit": 200,
  "min_age_seconds": 900,
  "horizon_seconds": 900
}
```
- Response:
```json
{
  "ok": true,
  "picked": 25,
  "emitted": 13,
  "skipped_existing": 10,
  "errors": 2
}
```
- Errors: `401`, `500`.

## Universe Endpoints

### GET `/api/admin/universe/current`
- Purpose: current universe per strategy.
- Query:
- `strategy_config_id` (optional)
- `account_id` (optional)
- `limit`, `offset`
- Response:
```json
{
  "items": [
    {
      "strategy_config_id": 5,
      "symbols": ["BTCUSDT", "ETHUSDT"],
      "updated_at": "2026-02-11T15:00:00Z",
      "source": "strategy_config"
    }
  ],
  "total": 1,
  "limit": 50,
  "offset": 0
}
```
- Errors: `401`, `500`.

### POST `/api/admin/universe/select`
- Purpose: run one universe selection and persist update.
- Body:
```json
{
  "strategy_config_id": 5,
  "top_n": 5,
  "window_minutes": 1440
}
```
- Response:
```json
{
  "ok": true,
  "admin_action_id": 17712,
  "strategy_config_id": 5,
  "previous_symbols": ["BTCUSDT", "ETHUSDT"],
  "new_symbols": ["ETHUSDT", "SOLUSDT"],
  "picked_by": "heuristic"
}
```
- Errors: `400`, `401`, `404`, `500`.

### POST `/api/admin/universe/rollback`
- Purpose: restore previous symbol set from a selection event.
- Body:
```json
{
  "admin_action_id": 17712
}
```
- Response:
```json
{
  "ok": true,
  "strategy_config_id": 5,
  "restored_symbols": ["BTCUSDT", "ETHUSDT"]
}
```
- Errors: `400`, `401`, `404`, `500`.

### GET `/api/admin/universe/history`
- Purpose: list universe selection/rollback events.
- Query:
- `strategy_config_id` (optional)
- `limit`, `offset`
- Response: paginated `UNIVERSE_SELECTION`/`UNIVERSE_ROLLBACK` events.
- Errors: `401`, `500`.

## Autopilot Control Endpoints

### POST `/api/admin/autopilot/start`
- Purpose: enable autopilot for strategy.
- Body:
```json
{
  "strategy_config_id": 5,
  "mode": "assist",
  "notes": "start canary"
}
```
- Response:
```json
{
  "ok": true,
  "strategy_config_id": 5,
  "mode": "assist",
  "started_at": "2026-02-11T15:20:00Z"
}
```
- Errors: `400`, `401`, `404`, `500`.

### POST `/api/admin/autopilot/stop`
- Purpose: pause autopilot for strategy.
- Body: `{ "strategy_config_id": 5, "reason": "manual pause" }`
- Response: `{ "ok": true, "strategy_config_id": 5, "stopped_at": "..." }`
- Errors: `400`, `401`, `404`, `500`.

### GET `/api/admin/autopilot/status`
- Purpose: list runtime status per strategy.
- Query: `strategy_config_id` optional, `limit`, `offset`.
- Response: paginated status rows with mode, running flag, last tick, last error.
- Errors: `401`, `500`.

## Policy Governance Endpoints

### GET `/api/admin/risk-policies`
- Query: `name`, `status`, `is_active`, `limit`, `offset`, `order`.
- Response: paginated policy versions.

### GET `/api/admin/position-policies`
- Query: `name`, `status`, `limit`, `offset`, `order`.
- Response: paginated policy versions.

### POST `/api/admin/risk-policies/{id}/clone`
### POST `/api/admin/risk-policies/{id}/publish`
### PATCH `/api/admin/risk-policies/{id}`
### POST `/api/admin/risk-policies/{id}/upsert-symbol-overrides-batch`
### POST `/api/admin/risk-policies/{id}/delete-symbol-overrides-batch`
- Purpose: version + override management.
- Errors: `400`, `401`, `404`, `409`, `500`.

### POST `/api/admin/position-policies/{id}/clone`
### POST `/api/admin/position-policies/{id}/publish`
### PATCH `/api/admin/position-policies/{id}`
### POST `/api/admin/position-policies/{id}/upsert-symbol-limits-batch`
### POST `/api/admin/position-policies/{id}/delete-symbol-limits-batch`
- Purpose: version + override management.
- Errors: `400`, `401`, `404`, `409`, `500`.

### POST `/api/admin/strategy-policy-bindings/upsert`
- Body:
```json
{
  "strategy_config_id": 5,
  "risk_policy_id": 2,
  "position_policy_id": "7f8155e4-c2b6-4ccf-a313-1ca2b2551a67",
  "notes": "rollout v3"
}
```
- Response:
```json
{
  "ok": true,
  "strategy_config_id": 5,
  "risk_policy_id": 2,
  "position_policy_id": "7f8155e4-c2b6-4ccf-a313-1ca2b2551a67",
  "updated_at": "2026-02-11T15:22:00Z"
}
```
- Errors: `400`, `401`, `404`, `500`.

### POST `/api/admin/strategy-policy-bindings/{strategy_config_id}/rollback`
- Body option A:
```json
{
  "risk_policy": {"name": "core-risk", "version": 2},
  "position_policy": {"name": "core-position", "version": 1}
}
```
- Body option B:
```json
{
  "risk_policy_id": 2,
  "position_policy_id": "7f8155e4-c2b6-4ccf-a313-1ca2b2551a67"
}
```
- Response includes before/after ids and versions.
- Errors: `400`, `401`, `404`, `500`.

### GET `/api/admin/strategy-configs/{strategy_config_id}/effective-policies`
- Query: `symbol` optional.
- Response includes:
- `policy_source`
- `policy_binding`
- `computed_limits` with risk and position sources/overrides
- Errors: `401`, `404`, `500`.

## Error Code Reference
- `400`: validation failure, missing required fields, invalid symbol.
- `401`: admin token invalid or missing.
- `404`: target strategy/policy/decision not found.
- `409`: version/publish conflict or invalid state transition.
- `422`: body schema validation error.
- `500`: internal query or execution failure (JSON error response only).
