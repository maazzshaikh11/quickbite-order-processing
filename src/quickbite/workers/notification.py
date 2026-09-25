"""Notification worker: consumes ``order.#`` (all order events) -> notifies the customer.

Persists one notification row per event and logs it. Delivery is idempotent:
the base worker's ``processed_events`` guard plus the unique
``notifications.idempotency_key`` constraint make duplicate deliveries
harmless.
"""

import asyncio
import logging
import signal

from sqlalchemy.ext.asyncio import AsyncSession

from ..common import db
from ..common.config import settings
from ..common.models import Notification
from .base import BaseWorker

NOTIFICATION_TEXTS = {
    "order.created": "Your order has been received.",
    "order.paid": "Payment successful.",
    "order.confirmed": "Restaurant confirmed your order.",
    "order.driver.assigned": "Driver has been assigned.",
    "order.payment.failed": "Payment failed. Please try again or use a different payment method.",
    "order.failed": "Your order could not be completed and needs manual review.",
}


class NotificationWorker(BaseWorker):
    worker_name = "notification"

    async def process(
        self, session: AsyncSession, event: dict, attempt: int
    ) -> list[tuple[str, dict]]:
        event_type = event["event_type"]
        order_id = event["order_id"]
        text = NOTIFICATION_TEXTS.get(event_type, f"Update on your order: {event_type}.")
        session.add(
            Notification(
                order_id=order_id,
                event_type=event_type,
                message=text,
                idempotency_key=event["idempotency_key"],
            )
        )
        # Flush here so a duplicate idempotency_key raises IntegrityError
        # inside process(), which the base worker suppresses as a duplicate.
        await session.flush()
        self.log.info("notify customer of order %s: %s", order_id, text)
        return []


async def main() -> None:
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    await db.init_db()
    worker = NotificationWorker(settings, db.get_session_factory())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
