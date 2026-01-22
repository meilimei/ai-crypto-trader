#!/usr/bin/env bash

# Usage:
#   ADMIN_TOKEN=... BASE_URL=... PSQL_URL=... ./scripts/phase42_smoke.sh

set -u

ADMIN_TOKEN="${ADMIN_TOKEN:-dev-admin-123}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
PSQL_URL="${PSQL_URL:-}"

PASS_COUNT=0
FAIL_COUNT=0

pass() {
  echo "PASS: $1"
  PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
  echo "FAIL: $1"
  FAIL_COUNT=$((FAIL_COUNT + 1))
}

if [ -z "$PSQL_URL" ]; then
  echo "FAIL: PSQL_URL is not set"
  exit 1
fi

echo "BASE_URL=$BASE_URL"
echo "ADMIN_TOKEN length=${#ADMIN_TOKEN}"

tmp_start="/tmp/phase42_start.json"
tmp_status1="/tmp/phase42_status1.json"
tmp_status2="/tmp/phase42_status2.json"
tmp_trade_ok="/tmp/phase42_trade_ok.json"
tmp_trade_reject="/tmp/phase42_trade_reject.json"

# 1) Start paper trader
if curl -sS -X POST "$BASE_URL/api/admin/paper-trader/start" \
  -H "X-Admin-Token: $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -o "$tmp_start"; then
  python - "$tmp_start" <<'PY'
import json,sys
p=sys.argv[1]
try:
    data=json.load(open(p))
    print("start.running:", data.get("running"))
    print("start.started_at:", data.get("started_at"))
except Exception as exc:
    print("start.parse_error:", exc)
PY
  pass "Start paper trader"
else
  fail "Start paper trader"
fi

# 2) Runner stability check
if curl -sS "$BASE_URL/api/admin/paper-trader/status" \
  -H "X-Admin-Token: $ADMIN_TOKEN" \
  -o "$tmp_status1"; then
  sleep 6
  if curl -sS "$BASE_URL/api/admin/paper-trader/status" \
    -H "X-Admin-Token: $ADMIN_TOKEN" \
    -o "$tmp_status2"; then
    if python - "$tmp_status1" "$tmp_status2" <<'PY'
import json,sys
from datetime import datetime

def get(path):
    with open(path) as f:
        return json.load(f)

d1=get(sys.argv[1])
d2=get(sys.argv[2])

lc1=d1.get("last_cycle_at")
lc2=d2.get("last_cycle_at")
le1=d1.get("last_error")
le2=d2.get("last_error")
rt1=d1.get("reconcile_tick_count")
rt2=d2.get("reconcile_tick_count")
ra1=d1.get("last_reconcile_at")
ra2=d2.get("last_reconcile_at")

print("last_cycle_at_1:", lc1)
print("last_cycle_at_2:", lc2)
print("last_error_1:", le1)
print("last_error_2:", le2)
print("reconcile_tick_count_1:", rt1)
print("reconcile_tick_count_2:", rt2)
print("last_reconcile_at_1:", ra1)
print("last_reconcile_at_2:", ra2)

ok_errors = (le1 in (None, "", "null")) and (le2 in (None, "", "null"))
cycle_ok = False
if lc2 and lc1:
    cycle_ok = lc2 != lc1 or lc2 >= lc1
elif lc2 and not lc1:
    cycle_ok = True

reconcile_ok = False
try:
    if isinstance(rt1, int) and isinstance(rt2, int):
        reconcile_ok = rt2 > rt1
except Exception:
    reconcile_ok = False

passed = ok_errors and (cycle_ok or reconcile_ok)

if passed:
    sys.exit(0)
else:
    sys.exit(1)
PY
    then
      pass "Runner stability"
    else
      fail "Runner stability"
    fi
  else
    fail "Runner stability (status2 fetch)"
  fi
else
  fail "Runner stability (status1 fetch)"
fi

# 3) Trade success path (account_id=5)
psql "$PSQL_URL" -X -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO paper_position_policies (
  account_id, min_qty, max_position_notional_per_symbol_usdt, max_total_notional_usdt
) VALUES (5, 0.001, 1000, 5000)
ON CONFLICT (account_id) DO UPDATE SET
  min_qty = EXCLUDED.min_qty,
  max_position_notional_per_symbol_usdt = EXCLUDED.max_position_notional_per_symbol_usdt,
  max_total_notional_usdt = EXCLUDED.max_total_notional_usdt;

INSERT INTO paper_risk_policies (account_id, max_order_notional_usdt)
VALUES (5, NULL)
ON CONFLICT (account_id) DO UPDATE SET
  max_order_notional_usdt = EXCLUDED.max_order_notional_usdt;
SQL

if curl -sS -X POST "$BASE_URL/api/admin/paper-trader/smoke-trade" \
  -H "X-Admin-Token: $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"account_id":5,"symbol":"ETHUSDT","side":"buy","qty":"0.01"}' \
  -o "$tmp_trade_ok"; then
  if python - "$tmp_trade_ok" <<'PY'
import json,sys
from decimal import Decimal

p=sys.argv[1]
try:
    data=json.load(open(p))
except Exception as exc:
    print("trade_ok.parse_error:", exc)
    sys.exit(1)

ok=data.get("ok")
trade_id=(data.get("trade") or {}).get("id")
reconcile=data.get("reconcile") or {}
summary=reconcile.get("summary") or {}
diff_count=summary.get("diff_count")
usdt_diff=summary.get("usdt_diff")
print("trade.ok:", ok)
print("trade.id:", trade_id)
print("reconcile.diff_count:", diff_count)
print("reconcile.usdt_diff:", usdt_diff)

passed=False
if ok is True:
    if diff_count in (0, "0"):
        passed=True
    else:
        try:
            if usdt_diff is not None:
                if abs(Decimal(str(usdt_diff))) <= Decimal("0.01"):
                    passed=True
        except Exception:
            passed=False

sys.exit(0 if passed else 1)
PY
  then
    pass "Smoke-trade success"
  else
    fail "Smoke-trade success"
  fi
else
  fail "Smoke-trade success (request)"
fi

# 4) Reject path + admin_actions logging
psql "$PSQL_URL" -X -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO paper_risk_policies (account_id, max_order_notional_usdt)
VALUES (5, 1)
ON CONFLICT (account_id) DO UPDATE SET
  max_order_notional_usdt = EXCLUDED.max_order_notional_usdt;
SQL

if curl -sS -X POST "$BASE_URL/api/admin/paper-trader/smoke-trade" \
  -H "X-Admin-Token: $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"account_id":5,"symbol":"ETHUSDT","side":"buy","qty":"0.01"}' \
  -o "$tmp_trade_reject"; then
  if python - "$tmp_trade_reject" <<'PY'
import json,sys
p=sys.argv[1]
try:
    data=json.load(open(p))
except Exception as exc:
    print("trade_reject.parse_error:", exc)
    sys.exit(1)

ok=data.get("ok")
reject=data.get("reject") or {}
code=reject.get("code")
print("reject.ok:", ok)
print("reject.code:", code)

passed = (ok is False and code == "MAX_ORDER_NOTIONAL")

sys.exit(0 if passed else 1)
PY
  then
    pass "Reject MAX_ORDER_NOTIONAL"
  else
    fail "Reject MAX_ORDER_NOTIONAL"
  fi
else
  fail "Reject MAX_ORDER_NOTIONAL (request)"
fi

count_reject=$(psql "$PSQL_URL" -X -q -t -A -c "SELECT count(*) FROM admin_actions WHERE action='ORDER_REJECTED' AND status='MAX_ORDER_NOTIONAL' AND created_at >= now() - interval '120 seconds';")
count_reject="${count_reject//[[:space:]]/}"
if [ "$count_reject" = "1" ]; then
  pass "ORDER_REJECTED logged once (120s window)"
else
  echo "ORDER_REJECTED count last 120s: $count_reject"
  fail "ORDER_REJECTED logged once (120s window)"
fi

# 5) Reconcile report throttling sanity for account 1
count_warn=$(psql "$PSQL_URL" -X -q -t -A -c "SELECT count(*) FROM admin_actions WHERE action='RECONCILE_REPORT' AND status='warn' AND meta->>'account_id'='1' AND created_at >= now() - interval '12 minutes';")
count_warn="${count_warn//[[:space:]]/}"
latest_warn=$(psql "$PSQL_URL" -X -q -t -A -c "SELECT id || ' ' || created_at FROM admin_actions WHERE action='RECONCILE_REPORT' AND status='warn' AND meta->>'account_id'='1' ORDER BY created_at DESC LIMIT 1;")
latest_warn="${latest_warn//$'\n'/}"

echo "account_id=1 warn count last 12m: $count_warn"
echo "latest warn row: ${latest_warn:-<none>}"

if [ -n "$count_warn" ] && [ "$count_warn" -le 2 ]; then
  pass "Reconcile warn throttled (<=2 in 12m)"
else
  fail "Reconcile warn throttled (<=2 in 12m)"
fi

# 6) Summary

echo ""
echo "Summary: PASS=$PASS_COUNT FAIL=$FAIL_COUNT"
if [ "$FAIL_COUNT" -eq 0 ]; then
  echo "PASS"
  exit 0
else
  echo "FAIL"
  exit 1
fi
