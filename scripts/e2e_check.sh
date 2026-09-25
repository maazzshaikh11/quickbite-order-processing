#!/usr/bin/env bash
# End-to-end verification against a running system (API + workers + broker + DB).
# Usage: ./scripts/e2e_check.sh [API_URL]
# Exits non-zero on the first failed assertion.
set -euo pipefail

API="${1:-http://localhost:8000}"
RABBIT_API="http://localhost:15672/api"
RABBIT_CREDS="${RABBITMQ_USER:-guest}:${RABBITMQ_PASSWORD:-guest}"

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }

wait_for_status() { # order_id, want_status, timeout_secs
  local id="$1" want="$2" timeout="$3" status=""
  for _ in $(seq 1 "$timeout"); do
    status=$(curl -sf "$API/orders/$id" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
    [[ "$status" == "$want" ]] && { echo "$status"; return 0; }
    sleep 1
  done
  echo "$status"; return 1
}

echo "== health =="
curl -sf "$API/health" | python3 -c "
import sys, json
h = json.load(sys.stdin)
assert h['status'] == 'healthy', h
assert h['rabbitmq_connected'] and h['database_connected'], h
print('  PASS: /health healthy, broker+db connected')
"

echo "== happy path: PENDING -> PAID -> CONFIRMED -> DRIVER_ASSIGNED =="
ID=$(curl -sf -X POST "$API/orders" -H 'Content-Type: application/json' -d '{
  "customer_name": "Amina Khan",
  "restaurant_id": "rest_123",
  "payment_behavior": "never_fail",
  "items": [
    {"name": "Chicken Biryani", "quantity": 2, "price": 12.5},
    {"name": "Mango Lassi", "quantity": 1, "price": 4.0}
  ]}' | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
echo "  order: $ID"
wait_for_status "$ID" "DRIVER_ASSIGNED" 60 >/dev/null \
  && pass "order $ID reached DRIVER_ASSIGNED" \
  || fail "order $ID did not reach DRIVER_ASSIGNED"
DRIVER=$(curl -sf "$API/orders/$ID" | python3 -c "import sys,json; print(json.load(sys.stdin)['driver_id'])")
[[ -n "$DRIVER" && "$DRIVER" != "None" ]] && pass "driver assigned: $DRIVER" || fail "no driver assigned"

echo "== retry then success (fail_twice) =="
ID2=$(curl -sf -X POST "$API/orders" -H 'Content-Type: application/json' -d '{
  "customer_name": "Retry Demo",
  "restaurant_id": "rest_123",
  "payment_behavior": "fail_twice",
  "items": [{"name": "Naan", "quantity": 1, "price": 2.0}]}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
echo "  order: $ID2 (expect 2 failures, then success)"
wait_for_status "$ID2" "DRIVER_ASSIGNED" 90 >/dev/null \
  && pass "order $ID2 recovered after retries" \
  || fail "order $ID2 did not recover"

echo "== permanent failure -> FAILED + dead-letter queue =="
ID3=$(curl -sf -X POST "$API/orders" -H 'Content-Type: application/json' -d '{
  "customer_name": "Fail Demo",
  "restaurant_id": "rest_123",
  "payment_behavior": "always_fail",
  "items": [{"name": "Samosa", "quantity": 1, "price": 3.0}]}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
echo "  order: $ID3 (expect retries exhausted -> FAILED)"
wait_for_status "$ID3" "FAILED" 120 >/dev/null \
  && pass "order $ID3 marked FAILED after retries exhausted" \
  || fail "order $ID3 did not reach FAILED"
# Poll: management stats can lag a few seconds behind the actual dead-lettering.
DLQ_DEPTH=0
for _ in $(seq 1 15); do
  DLQ_DEPTH=$(curl -sf -u "$RABBIT_CREDS" "$RABBIT_API/queues/%2F/orders.dead-letter.queue" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['messages'])")
  [[ "$DLQ_DEPTH" -ge 1 ]] && break
  sleep 1
done
[[ "$DLQ_DEPTH" -ge 1 ]] && pass "dead-letter queue holds $DLQ_DEPTH message(s)" \
  || fail "dead-letter queue is empty"

echo "== notifications persisted =="
NOTIF_COUNT=$(PGPASSWORD="${POSTGRES_PASSWORD:-quickbite}" psql -h localhost -U "${POSTGRES_USER:-quickbite}" \
  -d "${POSTGRES_DB:-quickbite}" -tAc "SELECT COUNT(*) FROM notifications WHERE order_id='$ID';" 2>/dev/null \
  || echo 0)
[[ "$NOTIF_COUNT" -ge 4 ]] && pass "$NOTIF_COUNT notifications stored for happy-path order" \
  || fail "expected >=4 notifications, got $NOTIF_COUNT"

echo
echo "ALL E2E CHECKS PASSED"
