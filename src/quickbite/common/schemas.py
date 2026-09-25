"""Pydantic schemas for the HTTP API and the RabbitMQ message contracts."""

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from .enums import PaymentBehavior

# ---------------------------------------------------------------------------
# HTTP API schemas
# ---------------------------------------------------------------------------


class OrderItemIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    quantity: int = Field(gt=0, le=100)
    price: float = Field(ge=0, le=100_000)


class CreateOrderRequest(BaseModel):
    customer_name: str = Field(min_length=1, max_length=120)
    restaurant_id: str = Field(min_length=1, max_length=64)
    items: list[OrderItemIn] = Field(min_length=1, max_length=50)
    # Demo hook: lets a single request force a deterministic payment outcome
    # (useful for demonstrating retries and the dead-letter queue).
    payment_behavior: PaymentBehavior = PaymentBehavior.AUTO

    @field_validator("customer_name", "restaurant_id")
    @classmethod
    def _strip_not_empty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


class OrderCreatedResponse(BaseModel):
    order_id: str
    status: str
    total_amount: float
    message: str


class OrderStatusResponse(BaseModel):
    order_id: str
    status: str
    restaurant_id: str
    driver_id: str | None = None
    total_amount: float
    created_at: datetime
    updated_at: datetime


class OrderSummary(BaseModel):
    order_id: str
    status: str
    total_amount: float
    created_at: datetime


class HealthResponse(BaseModel):
    status: Literal["healthy", "degraded"]
    rabbitmq_connected: bool
    database_connected: bool


# ---------------------------------------------------------------------------
# RabbitMQ message schemas (JSON bodies)
# ---------------------------------------------------------------------------


class OrderCreatedEvent(BaseModel):
    event_type: Literal["order.created"] = "order.created"
    order_id: str
    idempotency_key: str
    customer_name: str
    restaurant_id: str
    items: list[dict]
    total_amount: float
    payment_behavior: str = PaymentBehavior.AUTO.value
    created_at: str


class OrderPaidEvent(BaseModel):
    event_type: Literal["order.paid"] = "order.paid"
    order_id: str
    idempotency_key: str
    payment_id: str
    paid_at: str


class OrderPaymentFailedEvent(BaseModel):
    event_type: Literal["order.payment.failed"] = "order.payment.failed"
    order_id: str
    idempotency_key: str
    reason: str
    failed_at: str


class OrderConfirmedEvent(BaseModel):
    event_type: Literal["order.confirmed"] = "order.confirmed"
    order_id: str
    idempotency_key: str
    restaurant_id: str
    confirmed_at: str


class OrderDriverAssignedEvent(BaseModel):
    event_type: Literal["order.driver.assigned"] = "order.driver.assigned"
    order_id: str
    idempotency_key: str
    driver_id: str
    assigned_at: str


def utc_now_iso() -> str:

    return datetime.now(UTC).isoformat()


def idempotency_key_for(order_id: str, event: str) -> str:
    """Deterministic idempotency key, e.g. ``order_a1b2c3d4:created``."""
    return f"{order_id}:{event}"
