"""Reliability tests: retries, backoff, DLQ, idempotency, poison messages.

These drive ``BaseWorker._handle_message`` with fake AMQP messages so the
retry/dead-letter/idempotency machinery is tested without a broker.
"""

import json

import pytest
from sqlalchemy import func, select

from quickbite.common.messaging import (
    NOTREADY_COUNT_HEADER,
    RETRY_COUNT_HEADER,
    retry_delay_seconds,
)
from quickbite.common.models import Notification, Order, ProcessedEvent
from quickbite.workers.base import BaseWorker
from quickbite.workers.notification import NotificationWorker
from quickbite.workers.payment import PaymentWorker


class FakeDefaultExchange:
    def __init__(self):
        self.published: list[tuple[object, str]] = []

    async def publish(self, message, routing_key: str):
        self.published.append((message, routing_key))


class FakeMessage:
    """Minimal stand-in for aio_pika's AbstractIncomingMessage."""

    def __init__(self, payload, headers=None, routing_key="order.created"):
        if isinstance(payload, (dict, list)):
            self.body = json.dumps(payload).encode()
        else:
            self.body = payload  # raw bytes -> poison message
        self.headers = headers or {}
        self.routing_key = routing_key
        self.acked = False
        self.nack_requeue: bool | None = None

    async def ack(self):
        self.acked = True

    async def nack(self, requeue=False):
        self.nack_requeue = requeue


async def seed_pending(session_factory, order_id="order_test1", behavior="fail_twice"):
    async with session_factory() as s:
        s.add(
            Order(
                id=order_id,
                customer_name="Amina Khan",
                restaurant_id="rest_123",
                items=[{"name": "X", "quantity": 1, "price": 1.0}],
                total_amount=1.0,
                status="PENDING",
                payment_behavior=behavior,
            )
        )
        await s.commit()


def make_event(order_id="order_test1", behavior="fail_twice"):
    return {
        "event_type": "order.created",
        "order_id": order_id,
        "idempotency_key": f"{order_id}:created",
        "payment_behavior": behavior,
    }


async def drive(worker: BaseWorker, message: FakeMessage, publish):
    """Run one message through the worker; return (message, retry_exchange)."""
    retry_exchange = FakeDefaultExchange()
    await worker._handle_message(message, publish, retry_exchange)
    return message, retry_exchange


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------


def test_backoff_doubles_each_attempt():
    assert retry_delay_seconds(1, 5.0) == 5.0
    assert retry_delay_seconds(2, 5.0) == 10.0
    assert retry_delay_seconds(3, 5.0) == 20.0


async def test_transient_failure_triggers_delayed_retry(session_factory, test_settings):
    """fail_twice: attempt 1 fails -> republished to payment.queue.retry."""
    await seed_pending(session_factory)
    worker = PaymentWorker(test_settings, session_factory)
    published: list = []

    async def publish(rk, payload):
        published.append((rk, payload))

    msg, retry_exchange = await drive(worker, FakeMessage(make_event()), publish)

    assert msg.acked  # original acked after being moved to the retry queue
    assert msg.nack_requeue is None
    default = retry_exchange.published
    assert len(default) == 1
    retry_message, routing_key = default[0]
    assert routing_key == "payment.queue.retry"
    assert retry_message.headers[RETRY_COUNT_HEADER] == 1
    assert retry_message.expiration.total_seconds() > 0  # delayed, not immediate
    assert published == []  # no order.paid yet

    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == "PENDING"  # unchanged until success


async def test_retry_eventually_succeeds(session_factory, test_settings):
    """fail_twice: attempts 1-2 fail, attempt 3 succeeds -> PAID + order.paid."""
    await seed_pending(session_factory)
    worker = PaymentWorker(test_settings, session_factory)
    downstream: list = []

    async def publish(rk, payload):
        downstream.append((rk, payload))

    # Simulate the broker redelivering the expired retry messages.
    headers: dict = {}
    for expected_retry_count in (0, 1):
        msg, retry_exchange = await drive(
            worker, FakeMessage(make_event(), headers=headers), publish
        )
        assert msg.acked
        retry_message, _ = retry_exchange.published[-1]
        assert retry_message.headers[RETRY_COUNT_HEADER] == expected_retry_count + 1
        body = json.loads(retry_message.body.decode())
        headers = {RETRY_COUNT_HEADER: retry_message.headers[RETRY_COUNT_HEADER]}
        assert body["idempotency_key"] == "order_test1:created"

    # Third attempt (retry_count=2 header) succeeds.
    msg, _ = await drive(worker, FakeMessage(make_event(), headers=headers), publish)
    assert msg.acked
    assert [rk for rk, _ in downstream] == ["order.paid"]
    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == "PAID"


async def test_exhausted_retries_dead_letter(session_factory, test_settings):
    """always_fail with max_retries=2 -> FAILED + order.payment.failed + DLQ."""
    await seed_pending(session_factory, behavior="always_fail")
    worker = PaymentWorker(test_settings, session_factory)
    downstream: list = []

    async def publish(rk, payload):
        downstream.append((rk, payload))

    headers: dict = {}
    last_msg = None
    for _ in range(3):  # attempts 1, 2, then the final attempt
        last_msg, retry_exchange = await drive(
            worker, FakeMessage(make_event(behavior="always_fail"), headers=headers), publish
        )
        if last_msg.nack_requeue is not None:
            break
        retry_message, routing_key = retry_exchange.published[-1]
        assert routing_key == "payment.queue.retry"
        headers = {RETRY_COUNT_HEADER: retry_message.headers[RETRY_COUNT_HEADER]}

    assert last_msg is not None
    assert last_msg.nack_requeue is False  # -> dead-letter exchange -> DLQ
    assert not last_msg.acked

    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == "FAILED"
    routing_keys = [rk for rk, _ in downstream]
    assert routing_keys == ["order.payment.failed"]
    _, payload = downstream[0]
    assert payload["idempotency_key"] == "order_test1:payment.failed"


# ---------------------------------------------------------------------------
# Upstream-not-ready (API publish/commit race)
# ---------------------------------------------------------------------------


async def test_upstream_not_ready_triggers_quick_recheck(session_factory, test_settings):
    """order.created arriving before the API's insert commits -> short recheck
    that doesn't consume the retry budget; once committed, the redelivery
    succeeds."""
    worker = PaymentWorker(test_settings, session_factory)  # no order seeded
    event = make_event(behavior="never_fail")

    msg, retry_exchange = await drive(worker, FakeMessage(event), lambda rk, p: None)
    assert msg.acked
    assert msg.nack_requeue is None
    assert len(retry_exchange.published) == 1
    retry_message, routing_key = retry_exchange.published[0]
    assert routing_key == "payment.queue.retry"
    assert retry_message.headers[NOTREADY_COUNT_HEADER] == 1
    assert retry_message.headers[RETRY_COUNT_HEADER] == 0  # main budget untouched
    assert retry_message.expiration.total_seconds() == pytest.approx(
        test_settings.notready_delay_seconds
    )

    # The API commit lands; the rechecked message now processes normally.
    await seed_pending(session_factory, behavior="never_fail")
    downstream: list = []

    async def publish(rk, payload):
        downstream.append((rk, payload))

    msg2, _ = await drive(
        worker,
        FakeMessage(event, headers={NOTREADY_COUNT_HEADER: 1}),
        publish,
    )
    assert msg2.acked
    assert [rk for rk, _ in downstream] == ["order.paid"]
    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == "PAID"


async def test_upstream_never_ready_dead_letters(session_factory, test_settings):
    """If the order row never appears, the message is dead-lettered after the
    notready budget is exhausted (not retried forever)."""
    test_settings.notready_max_retries = 1
    worker = PaymentWorker(test_settings, session_factory)  # no order seeded
    event = make_event(behavior="never_fail")
    msg, _ = await drive(worker, FakeMessage(event), lambda rk, p: None)
    assert msg.acked  # first notready -> recheck scheduled
    msg2, _ = await drive(
        worker,
        FakeMessage(event, headers={NOTREADY_COUNT_HEADER: 1}),
        lambda rk, p: None,
    )
    assert msg2.nack_requeue is False  # budget exhausted -> DLQ
    assert not msg2.acked


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


async def test_duplicate_delivery_has_no_side_effects(
    session_factory, test_settings, order_created_event
):
    """Same event delivered twice -> one notification row, both acked."""
    worker = NotificationWorker(test_settings, session_factory)

    async def publish(rk, payload):
        pass

    msg1, _ = await drive(worker, FakeMessage(order_created_event), publish)
    msg2, _ = await drive(worker, FakeMessage(order_created_event), publish)
    assert msg1.acked and msg2.acked

    async with session_factory() as s:
        count = (await s.execute(select(func.count()).select_from(Notification))).scalar()
        assert count == 1
        processed = (await s.execute(select(func.count()).select_from(ProcessedEvent))).scalar()
        assert processed == 1


async def test_duplicate_payment_event_does_not_double_charge(session_factory, test_settings):
    """Redelivered order.created after success -> the stored order.paid is
    republished (at-least-once), but the payment itself is never re-run: the
    order is charged exactly once."""
    await seed_pending(session_factory, behavior="never_fail")
    worker = PaymentWorker(test_settings, session_factory)
    downstream: list = []

    async def publish(rk, payload):
        downstream.append((rk, payload))

    event = make_event(behavior="never_fail")
    await drive(worker, FakeMessage(event), publish)
    async with session_factory() as s:
        first_payment_id = (await s.get(Order, "order_test1")).payment_id

    await drive(worker, FakeMessage(event), publish)  # redelivery

    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == "PAID"
        assert order.payment_id == first_payment_id  # charged exactly once
        processed = (await s.execute(select(func.count()).select_from(ProcessedEvent))).scalar()
        assert processed == 1
    # The redelivery republishes the stored event (same idempotency key, so
    # downstream dedupes it); the business logic ran only once.
    assert [rk for rk, _ in downstream] == ["order.paid", "order.paid"]
    assert downstream[0][1]["idempotency_key"] == downstream[1][1]["idempotency_key"]


async def test_crash_between_commit_and_publish_recovers(session_factory, test_settings):
    """If the worker crashes after committing but before publishing/acking,
    the redelivered message republishes the stored outgoing events instead of
    reprocessing: no lost event, no double charge."""
    await seed_pending(session_factory, behavior="never_fail")
    worker = PaymentWorker(test_settings, session_factory)
    event = make_event(behavior="never_fail")

    async def failing_publish(rk, payload):
        raise ConnectionError("broker died mid-publish")

    # First delivery: work commits, then publish "crashes".
    msg1, retry_exchange = await drive(worker, FakeMessage(event), failing_publish)
    assert msg1.acked  # moved to the retry queue for redelivery
    assert len(retry_exchange.published) == 1  # scheduled retry, not order.paid
    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == "PAID"  # work is durable despite the crash
        first_payment_id = order.payment_id

    # Redelivery (e.g. retry-queue expiry after restart): must NOT reprocess.
    downstream: list = []

    async def publish(rk, payload):
        downstream.append((rk, payload))

    retry_message, _ = retry_exchange.published[0]
    headers = {RETRY_COUNT_HEADER: retry_message.headers[RETRY_COUNT_HEADER]}
    msg2, _ = await drive(worker, FakeMessage(event, headers=headers), publish)
    assert msg2.acked
    assert [rk for rk, _ in downstream] == ["order.paid"]  # exactly one publish
    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.payment_id == first_payment_id  # no double charge


# ---------------------------------------------------------------------------
# Poison messages
# ---------------------------------------------------------------------------


async def test_invalid_json_is_dead_lettered(session_factory, test_settings):
    worker = PaymentWorker(test_settings, session_factory)
    msg = FakeMessage(b"not-json{{{")
    await drive(worker, msg, lambda rk, p: None)
    assert msg.nack_requeue is False
    assert not msg.acked


async def test_missing_fields_are_dead_lettered(session_factory, test_settings):
    worker = PaymentWorker(test_settings, session_factory)
    msg = FakeMessage({"event_type": "order.created"})  # no order_id/key
    await drive(worker, msg, lambda rk, p: None)
    assert msg.nack_requeue is False
