#!/usr/bin/env bash
# Narrated happy-path demo: create an order and watch it flow through workers.
# Requires the API + all workers running (see README / docs/DEMO.md).
set -euo pipefail
API="${1:-http://localhost:8000}"

echo "### 1. Health check"
curl -s "$API/health"; echo; echo

echo "### 2. Create an order (API responds immediately)"
START=$(date +%s.%N)
RESP=$(curl -s -X POST "$API/orders" -H 'Content-Type: application/json' -d '{
  "customer_name": "Amina Khan",
  "restaurant_id": "rest_123",
  "items": [
    {"name": "Chicken Biryani", "quantity": 2, "price": 12.5},
    {"name": "Mango Lassi", "quantity": 1, "price": 4.0}
  ]}')
END=$(date +%s.%N)
echo "$RESP" | python3 -m json.tool
echo "API responded in $(python3 -c "print(f'$END - $START' )" 2>/dev/null || echo '?')s (workers still processing...)"
ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
echo

echo "### 3. Watch the status evolve: PENDING -> PAID -> CONFIRMED -> DRIVER_ASSIGNED"
for _ in $(seq 1 30); do
  STATUS=$(curl -s "$API/orders/$ID" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  printf '\r  status: %-15s' "$STATUS"
  [[ "$STATUS" == "DRIVER_ASSIGNED" || "$STATUS" == "FAILED" ]] && break
  sleep 1
done
echo; echo
echo "### 4. Final order state"
curl -s "$API/orders/$ID" | python3 -m json.tool
