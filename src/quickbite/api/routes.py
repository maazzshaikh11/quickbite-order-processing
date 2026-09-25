"""HTTP routes for the order service."""

import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..common import db
from ..common.enums import OrderStatus
from ..common.messaging import EventPublisher
from ..common.models import Order
from ..common.schemas import (
    CreateOrderRequest,
    HealthResponse,
    OrderCreatedEvent,
    OrderCreatedResponse,
    OrderStatusResponse,
    OrderSummary,
    idempotency_key_for,
    utc_now_iso,
)

log = logging.getLogger("quickbite.api")
router = APIRouter()


async def get_session() -> AsyncSession:
    async with db.get_session_factory()() as session:
        yield session


def get_publisher(request: Request) -> EventPublisher:
    publisher = getattr(request.app.state, "publisher", None)
    if publisher is None or not publisher.connected:
        raise HTTPException(status_code=503, detail="message broker unavailable; try again shortly")
    return publisher


@router.post("/orders", response_model=OrderCreatedResponse, status_code=201)
async def create_order(
    body: CreateOrderRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
    publisher: EventPublisher = Depends(get_publisher),
) -> OrderCreatedResponse:
    """Accept an order, persist it as PENDING, and queue it for processing.

    The response is returned immediately; payment, restaurant confirmation,
    driver assignment and notifications all happen asynchronously in workers.
    """
    order_id = f"order_{secrets.token_hex(4)}"
    total_amount = round(sum(item.quantity * item.price for item in body.items), 2)
    event = OrderCreatedEvent(
        order_id=order_id,
        idempotency_key=idempotency_key_for(order_id, "created"),
        customer_name=body.customer_name,
        restaurant_id=body.restaurant_id,
        items=[item.model_dump() for item in body.items],
        total_amount=total_amount,
        payment_behavior=body.payment_behavior.value,
        created_at=utc_now_iso(),
    ).model_dump()
    order = Order(
        id=order_id,
        customer_name=body.customer_name,
        restaurant_id=body.restaurant_id,
        items=[item.model_dump() for item in body.items],
        total_amount=total_amount,
        status=OrderStatus.PENDING.value,
        payment_behavior=body.payment_behavior.value,
    )
    try:
        # Publish with confirms inside the DB transaction: if publishing fails
        # the order is rolled back and the client gets a 503 (safe to retry).
        session.add(order)
        await session.flush()
        await publisher.publish_event("order.created", event)
        await session.commit()
    except Exception as exc:
        await session.rollback()
        log.exception("failed to accept order: %s", exc)
        raise HTTPException(
            status_code=503, detail="order could not be queued; please retry"
        ) from exc

    log.info("order %s accepted (total=%.2f)", order_id, total_amount)
    return OrderCreatedResponse(
        order_id=order_id,
        status=OrderStatus.PENDING.value,
        total_amount=total_amount,
        message="Order accepted and queued for processing.",
    )


@router.get("/orders", response_model=list[OrderSummary])
async def list_orders(
    limit: int = 20, session: AsyncSession = Depends(get_session)
) -> list[OrderSummary]:
    """List the most recent orders (newest first)."""
    limit = max(1, min(limit, 100))
    result = await session.execute(select(Order).order_by(desc(Order.created_at)).limit(limit))
    return [
        OrderSummary(
            order_id=o.id,
            status=o.status,
            total_amount=o.total_amount,
            created_at=o.created_at,
        )
        for o in result.scalars()
    ]


@router.get("/orders/{order_id}", response_model=OrderStatusResponse)
async def get_order(
    order_id: str, session: AsyncSession = Depends(get_session)
) -> OrderStatusResponse:
    """Retrieve the current status of an order."""
    order = await session.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail=f"order {order_id} not found")
    return OrderStatusResponse(
        order_id=order.id,
        status=order.status,
        restaurant_id=order.restaurant_id,
        driver_id=order.driver_id,
        total_amount=order.total_amount,
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    """Liveness/readiness probe reporting broker and database connectivity."""
    publisher = getattr(request.app.state, "publisher", None)
    rabbitmq_connected = bool(publisher and publisher.connected)
    database_connected = await db.check_db()
    status = "healthy" if (rabbitmq_connected and database_connected) else "degraded"
    return HealthResponse(
        status=status,
        rabbitmq_connected=rabbitmq_connected,
        database_connected=database_connected,
    )
