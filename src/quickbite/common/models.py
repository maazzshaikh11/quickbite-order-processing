"""SQLAlchemy ORM models.

Tables:
  orders           - the order aggregate and its lifecycle status.
  notifications    - customer notifications emitted by the notification worker.
  processed_events - idempotency ledger; one row per (worker, idempotency_key)
                     successfully processed, giving effectively-once semantics.
"""

from datetime import UTC, datetime

from sqlalchemy import JSON, DateTime, Float, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .enums import OrderStatus


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    customer_name: Mapped[str] = mapped_column(String(120), nullable=False)
    restaurant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    items: Mapped[list] = mapped_column(JSON, nullable=False)
    total_amount: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default=OrderStatus.PENDING.value
    )
    driver_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payment_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Demo hook: per-order override of the payment simulation behaviour.
    payment_behavior: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    # Unique so a redelivered event can never create a duplicate notification.
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )


class ProcessedEvent(Base):
    """Idempotency ledger: records events a worker has fully processed.

    ``outgoing_events`` stores the downstream events produced while processing
    (list of ``{"routing_key": ..., "payload": ...}``). They are published
    *after* the transaction commits; if the worker crashes between commit and
    publish/ack, a redelivery republishes them instead of reprocessing, so
    downstream events are never lost and never duplicated in effect.
    """

    __tablename__ = "processed_events"

    idempotency_key: Mapped[str] = mapped_column(String(128), primary_key=True)
    worker: Mapped[str] = mapped_column(String(64), primary_key=True)
    outgoing_events: Mapped[list | None] = mapped_column(JSON, nullable=True)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
