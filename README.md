# QuickBite Order Processing System

A scalable food delivery order processing system built with **FastAPI**, **RabbitMQ** (aio-pika), **PostgreSQL**, and async Python workers — demonstrating event-driven architecture, retries with exponential backoff, dead-letter queues, idempotent consumers, and horizontal scaling.

A customer places an order via `POST /orders`. The API validates it, saves it as `PENDING`, publishes an `order.created` event, and returns immediately. Four independent workers then drive the order through `PAID → CONFIRMED → DRIVER_ASSIGNED`, notifying the customer at each step. If payment fails, the order is retried with backoff and eventually lands in a dead-letter queue for manual review.

## Quick start (Docker Compose)

```bash
docker compose up --build
```

This starts PostgreSQL, RabbitMQ (management UI at http://localhost:15672, guest/guest), the FastAPI service (http://localhost:8000), and the four workers. Then:

```bash
# Create an order (returns immediately)
curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_name":"Amina Khan","restaurant_id":"rest_123",
       "items":[{"name":"Chicken Biryani","quantity":2,"price":12.5},
                {"name":"Mango Lassi","quantity":1,"price":4.0}]}'
# {"order_id":"order_abc123","status":"PENDING","total_amount":29.0,
#  "message":"Order accepted and queued for processing."}

# Watch it flow through the pipeline
curl http://localhost:8000/orders/order_abc123
# {"order_id":"order_abc123","status":"DRIVER_ASSIGNED", ...}

# Health (broker + database connectivity)
curl http://localhost:8000/health
# {"status":"healthy","rabbitmq_connected":true,"database_connected":true}
```

See [docs/DEMO.md](docs/DEMO.md) for the full guided walkthrough (happy path, worker failure, retries/DLQ, scaling) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design.

## Running locally (without Docker)

Requires Python 3.12+, PostgreSQL 16, and RabbitMQ 3.x with the management plugin.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

export DATABASE_URL="postgresql+asyncpg://quickbite:quickbite@localhost:5432/quickbite"
export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"

# Terminal 1: API
python -m quickbite.api.main
# Terminals 2-5: workers
python -m quickbite.workers.payment
python -m quickbite.workers.restaurant
python -m quickbite.workers.delivery
python -m quickbite.workers.notification
```

## API reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/orders` | Create an order. Body: `customer_name`, `restaurant_id`, `items[]` (`name`, `quantity`, `price`), optional `payment_behavior` (`auto`/`never_fail`/`fail_twice`/`always_fail`, demo hook). Returns `order_id`, `status=PENDING`, `total_amount`. |
| `GET` | `/orders/{order_id}` | Order status, driver, totals, timestamps. |
| `GET` | `/orders` | List orders (newest first, `limit`/`offset`). |
| `GET` | `/health` | `{"status","rabbitmq_connected","database_connected"}`. |

Interactive docs: http://localhost:8000/docs

## Architecture

```
POST /orders → FastAPI ──order.created──▶ payment.queue ──▶ payment worker ──order.paid──▶ restaurant.queue ──▶ ...
```

Topic exchange `quickbite.orders`; per-worker queues with DLX `quickbite.dlx` → `orders.dead-letter.queue`; per-queue `.retry` queues with per-message TTL for exponential backoff. Full topology, the reliability protocol (at-least-once delivery + idempotency guard + publish-after-commit), and the failure model are documented in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Reliability at a glance

- **Durable** exchanges/queues, **persistent** messages, **publisher confirms**.
- **Manual acks** — a crashed worker's in-flight message is requeued, never lost.
- **Bounded retries** with exponential backoff via TTL-based retry queues (5s → 10s → 20s); exhausted messages go to the **dead-letter queue**.
- **Idempotent consumers** — a `processed_events` table (atomic insert in the same transaction as the work) makes duplicate deliveries harmless; outgoing events are stored on that row and published *after* commit, so a crash between commit and publish loses nothing.
- **Poison messages** (bad JSON/schema) are dead-lettered immediately, never retried.

## Testing

```bash
pytest tests/ -q            # 29 tests: API, workers, retry/DLQ/idempotency/poison
bash scripts/e2e_check.sh   # end-to-end against live RabbitMQ + Postgres
ruff check src tests && ruff format --check src tests
```

## Configuration

All settings are environment variables (see `src/quickbite/common/config.py`): `DATABASE_URL`, `RABBITMQ_URL`, `MAX_RETRIES` (default 3), `RETRY_BASE_DELAY_SECONDS` (5), `PREFETCH_COUNT` (5), `PAYMENT_FAILURE_MODE` (`random`|`never_fail`|`always_fail`|`fail_twice`), `PAYMENT_FAILURE_RATE`, `PAYMENT_MIN/MAX_DELAY_SECONDS`, `RESTAURANT_DELAY_SECONDS`, `DELIVERY_DELAY_SECONDS`.

## Project structure

```
src/quickbite/
  api/            # FastAPI app: routes, schemas, lifespan
  workers/        # base.py (reliability backbone) + payment/restaurant/delivery/notification
  common/         # config, db models, messaging topology, event schemas
tests/            # unit + reliability tests (no broker needed)
scripts/e2e_check.sh  # end-to-end verification against live services
docs/
  ARCHITECTURE.md # topology, message flow, reliability protocol
  REPORT.md       # why queues / retries / DLQ / scaling / challenges
  DEMO.md         # guided demo walkthrough (§13)
```

## One deliberate deviation from the assignment text

The assignment suggests binding the notification queue with `order.*`. In AMQP topic matching `*` spans exactly one word, so `order.*` cannot match the three-word keys `order.driver.assigned` and `order.payment.failed` — the customer would never be told a driver was assigned. The binding is `order.#`, which implements the stated intent ("receives all order events"). This is documented in `docs/ARCHITECTURE.md`.
