"""Payment worker: consumes ``order.created`` -> simulates payment.

On success: order -> PAID, publishes ``order.paid``.
On transient failure: raises -> BaseWorker retries with backoff.
On permanent failure: order -> FAILED, publishes ``order.payment.failed``,
and the message is dead-lettered for manual review.
"""

import asyncio
import logging
import random
import secrets
import signal

from sqlalchemy.ext.asyncio import AsyncSession

from ..common import db
from ..common.config import Settings, settings
from ..common.enums import OrderStatus
from ..common.models import Order
from ..common.schemas import (
    OrderPaidEvent,
    OrderPaymentFailedEvent,
    idempotency_key_for,
    utc_now_iso,
)
from .base import BaseWorker, PublishFn, TransientError, UpstreamNotReady


class PaymentWorker(BaseWorker):
    worker_name = "payment"

    def __init__(self, settings: Settings, session_factory) -> None:
        super().__init__(settings, session_factory)
        self.failure_mode = settings.payment_failure_mode
        self.failure_rate = settings.payment_failure_rate

    def _should_fail(self, behavior: str, attempt: int) -> bool:
        if behavior == "always_fail":
            return True
        if behavior == "fail_twice":
            return attempt <= 2
        if behavior == "never_fail":
            return False
        # "auto" (or anything unknown): fall back to the configured simulation.
        if self.failure_mode == "always_fail":
            return True
        if self.failure_mode == "fail_twice":
            return attempt <= 2
        if self.failure_mode == "never_fail":
            return False
        return random.random() < self.failure_rate

    async def process(
        self, session: AsyncSession, event: dict, attempt: int
    ) -> list[tuple[str, dict]]:
        order_id = event["order_id"]
        order = await session.get(Order, order_id)
        if order is None:
            # The API publishes order.created just before its insert commits;
            # a very fast worker may briefly not see the row. Recheck cheaply.
            raise UpstreamNotReady(f"order {order_id} not committed yet")
        if order.status != OrderStatus.PENDING.value:
            self.log.info("order %s already %s; skipping duplicate payment", order_id, order.status)
            return []

        # Simulate a slow payment gateway call.
        await asyncio.sleep(
            random.uniform(
                self.settings.payment_min_delay_seconds,
                self.settings.payment_max_delay_seconds,
            )
        )

        behavior = event.get("payment_behavior") or "auto"
        if self._should_fail(behavior, attempt):
            raise TransientError(
                f"simulated payment failure (behavior={behavior}, attempt={attempt})"
            )

        order.payment_id = f"pay_{secrets.token_hex(3)}"
        order.status = OrderStatus.PAID.value
        paid_event = OrderPaidEvent(
            order_id=order_id,
            idempotency_key=idempotency_key_for(order_id, "paid"),
            payment_id=order.payment_id,
            paid_at=utc_now_iso(),
        ).model_dump()
        self.log.info(
            "payment succeeded for order %s (payment_id=%s)",
            order_id,
            order.payment_id,
        )
        return [("order.paid", paid_event)]

    async def on_permanent_failure(
        self, session: AsyncSession, event: dict, publish: PublishFn
    ) -> None:
        order_id = event["order_id"]
        order = await session.get(Order, order_id)
        if order is not None and order.status == OrderStatus.PENDING.value:
            order.status = OrderStatus.FAILED.value
        reason = (
            f"payment failed after {self.settings.max_retries} retries "
            "(see orders.dead-letter.queue for the original message)"
        )
        await publish(
            "order.payment.failed",
            OrderPaymentFailedEvent(
                order_id=order_id,
                idempotency_key=idempotency_key_for(order_id, "payment.failed"),
                reason=reason,
                failed_at=utc_now_iso(),
            ).model_dump(),
        )
        self.log.error("order %s marked FAILED: %s", order_id, reason)


async def main() -> None:
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    await db.init_db()
    worker = PaymentWorker(settings, db.get_session_factory())
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
