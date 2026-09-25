# Project report

## 1. Why queues are used

The order pipeline (payment gateway call → restaurant confirmation → driver matching → notifications) is slow and failure-prone. Doing it synchronously inside `POST /orders` would make the API as slow and as fragile as the weakest downstream step: one sluggish payment gateway and every customer stares at a spinner; one crashed worker and orders are lost.

Queues decouple *accepting* work from *doing* work:

- **Responsiveness.** `POST /orders` does two fast things — insert one row, publish one message — and returns. A burst of 30 orders was accepted without degradation while workers drained the backlog asynchronously.
- **Fault isolation.** If the notification worker is down, payment/restaurant/delivery keep flowing; notification messages simply wait in `notification.queue` and are processed after restart (demonstrated).
- **Load levelling.** The queue absorbs bursts; workers drain at their own pace. `prefetch_count` provides backpressure so a slow worker is never overwhelmed.
- **Independent scaling.** Each stage scales separately — run five payment workers without touching anything else.

## 2. How retries work

Failures are split into three classes:

1. **Poison** (invalid JSON, missing fields): never retried — nacked straight to the dead-letter queue. Retrying garbage is pure waste.
2. **Transient** (simulated payment failure, broker hiccup, unexpected exception): republished to `<queue>.retry` with a per-message TTL implementing exponential backoff (5s → 10s → 20s, configurable). On TTL expiry the broker dead-letters the message back to the topic exchange for redelivery, with an `x-retry-count` header tracking attempts. Bounded by `MAX_RETRIES` (default 3, so 4 attempts total).
3. **Upstream-not-ready** (the API's `order.created` arriving before its insert commits): cheap 2s rechecks, up to 10, that don't consume the main retry budget. This is a narrow safety net for one tiny race; worker-to-worker races are impossible by construction (see §4).

The retry queues are deliberately *separate* from the work queues so a failing message never head-of-line-blocks healthy ones.

## 3. How the dead-letter queue is used

`quickbite.dlx` (direct exchange) receives messages from any worker queue that are nacked with `requeue=False`:

- messages whose retries are exhausted (e.g. payment failed 4 times → order marked `FAILED`, `order.payment.failed` published, original message dead-lettered);
- poison messages;
- messages whose upstream state never materialised after the notready budget.

`orders.dead-letter.queue` is the manual-review inbox: each message keeps its original body and headers (`x-death` records the journey), so an operator can inspect, fix, and replay it. Nothing is silently dropped anywhere in the system.

## 4. How idempotency and the outbox work (stretch goal)

At-least-once delivery means duplicates happen (redeliveries after crashes, retry-queue fan-out). Two mechanisms make them harmless:

- **Idempotency guard.** Each worker atomically inserts `(idempotency_key, worker)` into `processed_events` in the same transaction as its work. A duplicate delivery finds the row and is acked without re-running anything.
- **Outbox on the guard row.** Workers *return* their outgoing events instead of publishing them; the base worker stores them on the guard row, commits, and only then publishes. A crash between commit and publish is recovered on redelivery by republishing the stored events — never by reprocessing. This is the outbox pattern: the business write and the event log commit atomically, so events are neither lost nor (in effect) duplicated. A dedicated test kills the publish step and verifies exactly-once payment with exactly-once delivery after recovery.

## 5. How scaling was demonstrated

Two payment worker processes were run against one `payment.queue`; the broker distributed orders across both (verified in logs: worker 1 and worker 2 each processed a share). Correctness under concurrency comes from the atomic idempotency insert — two instances racing on the same redelivered message can't both process it; the loser requeues. The API stayed responsive during a 30-order burst while four workers drained the pipeline to `DRIVER_ASSIGNED` for every order.

## 6. Challenges encountered

1. **No Docker in the build environment.** The sandbox kernel lacks the networking support Docker needs, and image pulls fail. The system was verified end-to-end against locally installed RabbitMQ 3.12 and PostgreSQL 16 instead; `docker compose config` validates the Compose file, and the topology code declares everything idempotently so `docker compose up` works wherever Docker runs. This is a known limitation: a live `docker compose up` was not runnable here.
2. **`order.*` doesn't match three-word routing keys.** AMQP `*` spans exactly one word, so the suggested binding would have silently dropped `order.driver.assigned` notifications. Found by test, fixed with `order.#`, documented as a deliberate deviation.
3. **Publish-then-commit races.** The first design published events before committing, which let downstream workers observe pre-commit state (a restaurant worker saw `PENDING` for a just-paid order). Fixed properly with the outbox design (§4) rather than papering over it: workers now publish strictly after commit.
4. **Library API drift.** aio-pika 10 removed `channel.default_exchange` and requires `timedelta` (not a string) for per-message `expiration`. Adapted with `channel.get_exchange("", ensure=False)` and `timedelta(milliseconds=...)`.
5. **Concurrent schema init.** Five processes creating tables simultaneously raced in `init_db()`; fixed with a retry loop.
6. **No demo video recorded in this environment.** The sandbox has no screen-capture capability, so the §13 demo video could not be recorded here. `scripts/demo.sh` (narrated happy path) and `scripts/e2e_check.sh` (scripted §13.1 + §13.3) are the reproducible equivalents; `docs/DEMO.md` lists the suggested shots for recording on a normal machine.

## 7. Verification summary

- **29 unit/reliability tests** (`pytest tests/`): API validation, worker state transitions, retry/backoff headers, DLQ routing, idempotency, poison messages, crash-between-commit-and-publish recovery, upstream-not-ready rechecks.
- **`scripts/e2e_check.sh`**: live end-to-end — happy path `PENDING → PAID → CONFIRMED → DRIVER_ASSIGNED`, `fail_twice` recovery, `always_fail` → `FAILED` + DLQ depth, 4 notifications per order. All green.
- **Failure scenarios** (assignment §11 — all six demonstrated, four required): worker `kill -9` mid-processing → requeued, recovered, charged exactly once; temporary payment failure → retried → recovered; repeated failure → DLQ; notification worker stopped → backlog drained on restart; duplicate delivery → no duplicate side effects; 30-order burst → API responsive, all completed.
- **Lint/format**: `ruff check` and `ruff format --check` clean.
