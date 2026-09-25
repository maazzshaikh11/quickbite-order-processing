# Demo walkthrough

Follows the assignment's demonstration requirements (§13). Assumes `docker compose up --build` is running (or the local equivalent: API on :8000, four workers, RabbitMQ management at http://localhost:15672, guest/guest).

> Tip: the API accepts an optional `payment_behavior` field per order — `never_fail`, `fail_twice`, `always_fail` — so every scenario below is deterministic. Without it, payments fail randomly (`PAYMENT_FAILURE_MODE=random`, 30% rate).

## 13.1 Happy path

```bash
# 1. Create an order — note how fast this returns
time curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_name":"Amina Khan","restaurant_id":"rest_123",
       "items":[{"name":"Chicken Biryani","quantity":2,"price":12.5},
                {"name":"Mango Lassi","quantity":1,"price":4.0}],
       "payment_behavior":"never_fail"}'
# {"order_id":"order_...","status":"PENDING","total_amount":29.0,
#  "message":"Order accepted and queued for processing."}

# 2. Watch the status flow
watch -n1 'curl -s http://localhost:8000/orders/order_... | python3 -c "import sys,json;print(json.load(sys.stdin)[\"status\"])"'
# PENDING → PAID → CONFIRMED → DRIVER_ASSIGNED

# 3. In the RabbitMQ management UI, watch messages flow through
#    payment.queue → restaurant.queue → delivery.queue, and every event
#    also landing in notification.queue.
```

Expected: API responds in milliseconds; order reaches `DRIVER_ASSIGNED` in a few seconds; four notifications are stored (`order.created`, `order.paid`, `order.confirmed`, `order.driver.assigned`).

## 13.2 Worker failure (notification worker stopped)

```bash
# 1. Stop the notification worker
docker compose stop notification-worker   # or kill the process

# 2. Create several orders
for i in 1 2 3; do curl -s -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_name":"Demo","restaurant_id":"rest_1","payment_behavior":"never_fail",
       "items":[{"name":"X","quantity":1,"price":1.0}]}' > /dev/null; done

# 3. Show messages waiting: notification.queue depth grows in the management UI
#    (or: curl -s -u guest:guest http://localhost:15672/api/queues | ...),
#    while payment/restaurant/delivery keep working — orders still reach DRIVER_ASSIGNED.

# 4. Restart the worker and watch the backlog drain
docker compose start notification-worker
# notification.queue depth → 0; notifications appear in the database/log.
```

Expected: no order processing is blocked; notifications are delivered after restart; no duplicates.

## 13.3 Retry and dead-letter queue

```bash
# 1. Force temporary failures — fails twice, then succeeds
curl -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_name":"Demo","restaurant_id":"rest_1","payment_behavior":"fail_twice",
       "items":[{"name":"X","quantity":1,"price":1.0}]}'
# Watch the payment worker log: "attempt 1 ... failed; retrying in 5.0s",
# "attempt 2 ... retrying in 10.0s", then success. In the management UI,
# payment.queue.retry briefly holds the message with a TTL countdown.

# 2. Force permanent failure
curl -X POST http://localhost:8000/orders -H 'Content-Type: application/json' \
  -d '{"customer_name":"Demo","restaurant_id":"rest_1","payment_behavior":"always_fail",
       "items":[{"name":"X","quantity":1,"price":1.0}]}'
# Log: attempts 1-4 with backoff 5s/10s/20s, then
# "failed permanently ... dead-lettering". Order → FAILED.
# orders.dead-letter.queue depth → 1. Inspect the message in the management UI:
# "Get messages" shows the original order.created with its x-death headers.
```

Expected: bounded retries with visible backoff; the dead message is preserved for review, not dropped.

## 13.4 Scaling

```bash
# 1. With one payment worker, submit several orders and note the log lines
#    all coming from the single worker.

# 2. Start a second payment worker
docker compose up --build --scale payment-worker=2 -d
# or locally: python -m quickbite.workers.payment   (in another terminal)

# 3. Submit more orders; both workers' logs show "payment succeeded",
#    i.e. the broker distributes payment.queue across the two consumers.
#    Every order still completes exactly once (idempotency guard).
```

Expected: messages distributed across workers; no duplicate charges; throughput increases.

## Bonus: crash mid-processing

```bash
# Give the payment worker a long simulated delay, create an order, then
# kill -9 the worker while it sleeps (message unacked):
PAYMENT_MIN_DELAY_SECONDS=15 PAYMENT_MAX_DELAY_SECONDS=15 \
  python -m quickbite.workers.payment & echo $! > /tmp/pw.pid
curl -X POST ... # create order
sleep 4; kill -9 $(cat /tmp/pw.pid)
# payment.queue depth → 1 (broker requeued the unacked message);
# order stays PENDING. Restart the worker → order completes to
# DRIVER_ASSIGNED with exactly one payment_id.
```

## Recording the demo

Suggested shots: (1) `time curl POST /orders` showing the instant response; (2) RabbitMQ management UI queues tab during the happy path; (3) worker logs side-by-side during retry/DLQ; (4) `watch` on order status; (5) DLQ message inspection in the UI. `scripts/e2e_check.sh` runs the scripted version of 13.1 + 13.3 end-to-end.
