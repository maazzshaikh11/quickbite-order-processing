"""Restaurant worker: consumes ``order.paid`` -> confirms with restaurant.

On success: order -> CONFIRMED, publishes ``order.confirmed``.
"""

import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import AsyncSession

from ..common import db
from ..common.config import settings
from ..common.enums import OrderStatus
from ..common.models import Order
from ..common.schemas import OrderConfirmedEvent, idempotency_key_for, utc_now_iso
from .base import BaseWorker


class RestaurantWorker(BaseWorker):
    worker_name = "restaurant"

    async def process(
        self, session: AsyncSession, event: dict, attempt: int
    ) -> list[tuple[str, dict]]:
        order_id = event["order_id"]
        order = await session.get(Order, order_id)
        if order is None:
            self.log.warning("order %s not found; skipping confirmation", order_id)
            return []
        if order.status != OrderStatus.PAID.value:
            # Defensive: with publish-after-commit this should not happen, but
            # never confirm twice.
            self.log.info(
                "order %s already %s; skipping duplicate confirmation",
                order_id,
                order.status,
            )
            return []

        # Simulate calling the restaurant's tablet/API.
        await asyncio.sleep(self.settings.restaurant_delay_seconds)

        order.status = OrderStatus.CONFIRMED.value
        confirmed_event = OrderConfirmedEvent(
            order_id=order_id,
            idempotency_key=idempotency_key_for(order_id, "confirmed"),
            restaurant_id=order.restaurant_id,
            confirmed_at=utc_now_iso(),
        ).model_dump()
        self.log.info("restaurant confirmed order %s", order_id)
        return [("order.confirmed", confirmed_event)]


async def main() -> None:
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    await db.init_db()
    worker = RestaurantWorker(settings, db.get_session_factory())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
