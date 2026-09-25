"""Worker handler tests: each worker's state transitions and event chaining."""

import pytest
from sqlalchemy import select

from quickbite.common.enums import OrderStatus
from quickbite.common.models import Notification, Order
from quickbite.workers.base import UpstreamNotReady
from quickbite.workers.delivery import DeliveryWorker
from quickbite.workers.notification import NotificationWorker
from quickbite.workers.payment import PaymentWorker
from quickbite.workers.restaurant import RestaurantWorker


async def seed_order(session_factory, order_id="order_test1", status="PENDING"):
    async with session_factory() as s:
        s.add(
            Order(
                id=order_id,
                customer_name="Amina Khan",
                restaurant_id="rest_123",
                items=[{"name": "Chicken Biryani", "quantity": 2, "price": 12.5}],
                total_amount=25.0,
                status=status,
                payment_behavior="never_fail",
            )
        )
        await s.commit()


async def test_payment_success_transitions_to_paid(
    session_factory, test_settings, order_created_event
):
    await seed_order(session_factory)
    worker = PaymentWorker(test_settings, session_factory)
    async with session_factory() as s:
        outgoing = await worker.process(s, order_created_event, attempt=1)
        await s.commit()

    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == OrderStatus.PAID.value
        assert order.payment_id.startswith("pay_")
    assert len(outgoing) == 1
    rk, payload = outgoing[0]
    assert rk == "order.paid"
    assert payload["idempotency_key"] == "order_test1:paid"


async def test_payment_skips_non_pending_orders(session_factory, test_settings):
    await seed_order(session_factory, status="PAID")
    worker = PaymentWorker(test_settings, session_factory)
    event = {
        "event_type": "order.created",
        "order_id": "order_test1",
        "idempotency_key": "order_test1:created",
        "payment_behavior": "never_fail",
    }
    async with session_factory() as s:
        outgoing = await worker.process(s, event, attempt=1)
        await s.commit()
    # No duplicate order.paid produced.
    assert outgoing == []


async def test_payment_missing_order_raises_upstream_not_ready(session_factory, test_settings):
    """order.created arriving before the API's insert commits -> UpstreamNotReady
    (the base worker rechecks cheaply instead of dropping the order)."""
    worker = PaymentWorker(test_settings, session_factory)
    event = {
        "event_type": "order.created",
        "order_id": "order_ghost",
        "idempotency_key": "order_ghost:created",
        "payment_behavior": "never_fail",
    }
    async with session_factory() as s:
        with pytest.raises(UpstreamNotReady):
            await worker.process(s, event, attempt=1)


async def test_restaurant_confirms_paid_order(session_factory, test_settings):
    await seed_order(session_factory, status="PAID")
    worker = RestaurantWorker(test_settings, session_factory)
    event = {
        "event_type": "order.paid",
        "order_id": "order_test1",
        "idempotency_key": "order_test1:paid",
        "payment_id": "pay_1",
    }
    async with session_factory() as s:
        outgoing = await worker.process(s, event, attempt=1)
        await s.commit()
    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == OrderStatus.CONFIRMED.value
    rk, payload = outgoing[0]
    assert rk == "order.confirmed"
    assert payload["idempotency_key"] == "order_test1:confirmed"


async def test_delivery_assigns_driver(session_factory, test_settings):
    await seed_order(session_factory, status="CONFIRMED")
    worker = DeliveryWorker(test_settings, session_factory)
    event = {
        "event_type": "order.confirmed",
        "order_id": "order_test1",
        "idempotency_key": "order_test1:confirmed",
    }
    async with session_factory() as s:
        outgoing = await worker.process(s, event, attempt=1)
        await s.commit()
    async with session_factory() as s:
        order = await s.get(Order, "order_test1")
        assert order.status == OrderStatus.DRIVER_ASSIGNED.value
        assert order.driver_id.startswith("driver_")
    rk, payload = outgoing[0]
    assert rk == "order.driver.assigned"
    assert payload["driver_id"] == order.driver_id
    assert payload["idempotency_key"] == "order_test1:driver.assigned"


async def test_notification_stores_and_logs_all_event_types(session_factory, test_settings, caplog):
    worker = NotificationWorker(test_settings, session_factory)
    events = [
        ("order.created", "Your order has been received."),
        ("order.paid", "Payment successful."),
        ("order.confirmed", "Restaurant confirmed your order."),
        ("order.driver.assigned", "Driver has been assigned."),
        ("order.payment.failed", "Payment failed."),
    ]
    async with session_factory() as s:
        for event_type, _ in events:
            outgoing = await worker.process(
                s,
                {
                    "event_type": event_type,
                    "order_id": "order_test1",
                    "idempotency_key": f"order_test1:{event_type}",
                },
                attempt=1,
            )
            assert outgoing == []
        await s.commit()
    async with session_factory() as s:
        rows = (await s.execute(select(Notification).order_by(Notification.id))).scalars().all()
        assert len(rows) == 5
        assert rows[0].message == "Your order has been received."
        assert rows[1].message == "Payment successful."
        assert rows[2].message == "Restaurant confirmed your order."
        assert rows[3].message == "Driver has been assigned."
        assert "Payment failed" in rows[4].message
