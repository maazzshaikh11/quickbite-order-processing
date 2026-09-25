"""Unit tests for Pydantic API + message schemas (incl. PDF examples)."""

import pytest
from pydantic import ValidationError

from quickbite.common.schemas import (
    CreateOrderRequest,
    OrderCreatedEvent,
    OrderPaidEvent,
    idempotency_key_for,
)


def test_pdf_order_example_validates():
    """The exact request body from the assignment PDF must validate."""
    req = CreateOrderRequest(
        customer_name="Amina Khan",
        restaurant_id="rest_123",
        items=[
            {"name": "Chicken Biryani", "quantity": 2, "price": 12.5},
            {"name": "Mango Lassi", "quantity": 1, "price": 4.0},
        ],
    )
    assert req.items[0].name == "Chicken Biryani"
    assert req.payment_behavior.value == "auto"


def test_order_total_matches_pdf_example():
    req = CreateOrderRequest(
        customer_name="Amina Khan",
        restaurant_id="rest_123",
        items=[
            {"name": "Chicken Biryani", "quantity": 2, "price": 12.5},
            {"name": "Mango Lassi", "quantity": 1, "price": 4.0},
        ],
    )
    total = round(sum(i.quantity * i.price for i in req.items), 2)
    assert total == 29.0


def test_create_order_rejects_bad_input():
    with pytest.raises(ValidationError):
        CreateOrderRequest(customer_name="  ", restaurant_id="r", items=[])
    with pytest.raises(ValidationError):
        CreateOrderRequest(
            customer_name="A",
            restaurant_id="r",
            items=[{"name": "X", "quantity": 0, "price": 1.0}],
        )
    with pytest.raises(ValidationError):
        CreateOrderRequest(
            customer_name="A",
            restaurant_id="r",
            items=[{"name": "X", "quantity": 1, "price": -5.0}],
        )


def test_pdf_message_examples_validate():
    created = OrderCreatedEvent(
        order_id="order_abc123",
        idempotency_key="order_abc123:created",
        customer_name="Amina Khan",
        restaurant_id="rest_123",
        items=[{"name": "Chicken Biryani", "quantity": 2, "price": 12.5}],
        total_amount=25.0,
        created_at="2026-09-19T12:00:00Z",
    )
    assert created.event_type == "order.created"

    paid = OrderPaidEvent(
        order_id="order_abc123",
        idempotency_key="order_abc123:paid",
        payment_id="pay_555",
        paid_at="2026-09-19T12:00:03Z",
    )
    assert paid.event_type == "order.paid"


def test_idempotency_key_format():
    assert idempotency_key_for("order_abc123", "created") == "order_abc123:created"
    assert idempotency_key_for("order_abc123", "paid") == "order_abc123:paid"
