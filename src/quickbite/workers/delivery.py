"""Delivery worker: consumes ``order.confirmed`` -> assigns a driver.

On success: order -> DRIVER_ASSIGNED, publishes ``order.driver.assigned``.
"""

import asyncio
import logging
import random
import signal

from sqlalchemy.ext.asyncio import AsyncSession

from ..common import db
from ..common.config import settings
from ..common.enums import OrderStatus
from ..common.models import Order
from ..common.schemas import (
    OrderDriverAssignedEvent,
    idempotency_key_for,
    utc_now_iso,
)
from .base import BaseWorker

# Simulated fleet of available drivers.
DRIVER_POOL = [f"driver_{i:03d}" for i in range(1, 11)]


class DeliveryWorker(BaseWorker):
    worker_name = "delivery"

    async def process(
        self, session: AsyncSession, event: dict, attempt: int
    ) -> list[tuple[str, dict]]:
        order_id = event["order_id"]
        order = await session.get(Order, order_id)
        if order is None:
            self.log.warning("order %s not found; skipping dispatch", order_id)
            return []
        if order.status != OrderStatus.CONFIRMED.value:
            # Defensive: with publish-after-commit this should not happen, but
            # never dispatch twice.
            self.log.info(
                "order %s already %s; skipping duplicate dispatch",
                order_id,
                order.status,
            )
            return []

        # Simulate driver matching latency.
        await asyncio.sleep(self.settings.delivery_delay_seconds)

        order.driver_id = random.choice(DRIVER_POOL)
        order.status = OrderStatus.DRIVER_ASSIGNED.value
        assigned_event = OrderDriverAssignedEvent(
            order_id=order_id,
            idempotency_key=idempotency_key_for(order_id, "driver.assigned"),
            driver_id=order.driver_id,
            assigned_at=utc_now_iso(),
        ).model_dump()
        self.log.info("driver %s assigned to order %s", order.driver_id, order_id)
        return [("order.driver.assigned", assigned_event)]


async def main() -> None:
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    await db.init_db()
    worker = DeliveryWorker(settings, db.get_session_factory())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
