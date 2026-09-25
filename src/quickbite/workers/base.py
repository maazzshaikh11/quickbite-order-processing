"""Base worker: the reliability backbone shared by all workers.

Consume loop per message::

    1. Parse + validate the JSON body. Poison -> nack (dead-lettered).
    2. Idempotency guard: atomically insert (idempotency_key, worker) into
       ``processed_events``. On conflict the message is a duplicate ->
       republish the stored outgoing events and ack (never reprocess).
    3. Run ``process()`` inside the same DB transaction; it returns outgoing
       events, which are stored on the guard row. Commit, *then* publish the
       outgoing events, then ack. Publishing after commit means a consumer can
       never observe uncommitted state; a crash between commit and publish/ack
       is recovered on redelivery via step 2, so no event is ever lost.
    4. On failure: roll back, then either
       a. republish to ``<queue>.retry`` with an incremented ``x-retry-count``
          header and a per-message TTL (exponential backoff), then ack; or
       b. when retries are exhausted, run ``on_permanent_failure()`` and nack
          (the queue's dead-letter config routes the message to
          ``orders.dead-letter.queue``).

Unacknowledged messages are requeued by RabbitMQ if a worker crashes, so no
message is lost on worker failure.
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import ClassVar

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ..common.config import Settings
from ..common.messaging import (
    NOTREADY_COUNT_HEADER,
    RETRY_COUNT_HEADER,
    TOPIC_EXCHANGE,
    WORKER_QUEUES,
    build_message,
    declare_topology,
    retry_delay_seconds,
    retry_queue_name,
)
from ..common.models import ProcessedEvent

PublishFn = Callable[[str, dict], Awaitable[None]]

REQUIRED_FIELDS = ("event_type", "order_id", "idempotency_key")


class TransientError(Exception):
    """A retryable processing failure (raised by workers on transient faults)."""


class UpstreamNotReady(TransientError):
    """The event arrived before the upstream state was committed.

    Workers publish outgoing events only *after* their transaction commits, so
    this cannot happen between workers. It can still (very rarely) happen for
    ``order.created``: the API publishes before its insert commits, so a fast
    payment worker may briefly not see the order row. Raising this triggers a
    short, cheap retry that does not consume the main retry budget.
    """


class BaseWorker(ABC):
    worker_name: ClassVar[str]

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.log = logging.getLogger(f"quickbite.workers.{self.worker_name}")
        self._stopping = asyncio.Event()

    @property
    def queue_name(self) -> str:
        return WORKER_QUEUES[self.worker_name]["queue"]

    def stop(self) -> None:
        """Signal the consume loop to finish the in-flight message and exit."""
        self._stopping.set()

    # ------------------------------------------------------------------
    # Hooks implemented by concrete workers
    # ------------------------------------------------------------------
    @abstractmethod
    async def process(
        self,
        session: AsyncSession,
        event: dict,
        attempt: int,
    ) -> list[tuple[str, dict]]:
        """Do the unit of work for one event; return outgoing ``(routing_key, payload)`` events.

        Outgoing events are returned, not published: the base class stores them
        on the idempotency-guard row, commits the transaction, and only then
        publishes them, so consumers can never observe uncommitted state.
        Raise :class:`TransientError` on retryable failure.
        """

    async def on_permanent_failure(  # noqa: B027 - intentional optional hook
        self, session: AsyncSession, event: dict, publish: PublishFn
    ) -> None:
        """Called once retries are exhausted, inside its own transaction."""

    # ------------------------------------------------------------------
    # Runtime
    # ------------------------------------------------------------------
    async def run(self) -> None:
        # connect_robust keeps retrying until the broker is reachable.
        connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
        try:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self.settings.prefetch_count)
            queues = await declare_topology(channel)
            queue = queues[self.worker_name]
            exchange = await channel.get_exchange(TOPIC_EXCHANGE, ensure=False)
            # The nameless default exchange routes directly to a queue whose
            # name matches the routing key; used for the retry queues.
            default_exchange = await channel.get_exchange("", ensure=False)

            async def publish(routing_key: str, payload: dict) -> None:
                await exchange.publish(build_message(payload), routing_key=routing_key)

            self.log.info(
                "consuming from %s (prefetch=%d, max_retries=%d)",
                self.queue_name,
                self.settings.prefetch_count,
                self.settings.max_retries,
            )
            # Poll with a timeout (instead of an infinite iterator) so the
            # worker can shut down gracefully between messages on SIGTERM.
            while not self._stopping.is_set():
                message = await queue.get(timeout=1.0, fail=False)
                if message is None:
                    continue
                try:
                    await self._handle_message(message, publish, default_exchange)
                except Exception:
                    self.log.exception("unexpected error handling message; dead-lettering it")
                    try:
                        await message.nack(requeue=False)
                    except Exception:
                        self.log.exception("failed to nack poison message")
        finally:
            await connection.close()
        self.log.info("worker stopped")

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------
    async def _handle_message(
        self,
        message: AbstractIncomingMessage,
        publish: PublishFn,
        default_exchange: aio_pika.abc.AbstractExchange,
    ) -> None:
        try:
            event = json.loads(message.body.decode("utf-8"))
        except Exception:
            self.log.warning("poison message (invalid JSON); dead-lettering")
            await message.nack(requeue=False)
            return

        if not isinstance(event, dict) or not all(
            isinstance(event.get(f), str) for f in REQUIRED_FIELDS
        ):
            self.log.warning("poison message (bad schema); dead-lettering: %r", event)
            await message.nack(requeue=False)
            return

        headers = message.headers or {}
        retry_count = int(headers.get(RETRY_COUNT_HEADER, 0) or 0)
        notready_count = int(headers.get(NOTREADY_COUNT_HEADER, 0) or 0)
        attempt = retry_count + 1
        idempotency_key: str = event["idempotency_key"]

        async with self.session_factory() as session:
            # Atomic idempotency guard: the (key, worker) row is inserted in
            # the same transaction as the work itself, so a crash between
            # "work done" and "ack" can only cause a harmless redelivery.
            row = ProcessedEvent(
                idempotency_key=idempotency_key,
                worker=self.worker_name,
                outgoing_events=None,
            )
            session.add(row)
            try:
                await session.flush()
            except IntegrityError:
                # (a) Duplicate delivery (redelivery after a crash between
                # commit and ack, or a broker duplicate): the winner already
                # committed, so republish its stored outgoing events instead of
                # re-running the work, then ack. This is what makes "publish
                # after commit" lossless.
                # (b) Lost a race with another worker instance processing the
                # same message concurrently: the winner will publish. Requeue
                # so this copy is retried after the winner commits.
                await session.rollback()
                existing = await session.get(
                    ProcessedEvent,
                    {"idempotency_key": idempotency_key, "worker": self.worker_name},
                )
                if existing is not None and existing.outgoing_events is not None:
                    self.log.info(
                        "duplicate delivery of %s ignored (idempotent); "
                        "republishing %d stored outgoing event(s)",
                        idempotency_key,
                        len(existing.outgoing_events),
                    )
                    try:
                        for stored in existing.outgoing_events:
                            await publish(stored["routing_key"], stored["payload"])
                    except Exception:
                        self.log.exception(
                            "failed to republish stored events for %s; requeueing",
                            idempotency_key,
                        )
                        await message.nack(requeue=True)
                        return
                    await message.ack()
                    return
                await session.close()
                await asyncio.sleep(2)
                await message.nack(requeue=True)
                return

            try:
                outgoing = await self.process(session, event, attempt)
            except IntegrityError:
                # A duplicate side effect slipped past the guard (e.g. the
                # notification unique key); suppress it rather than retrying.
                await session.rollback()
                self.log.info("duplicate side effect suppressed for %s", idempotency_key)
                await message.ack()
                return
            except Exception as exc:
                await session.rollback()
                await self._handle_failure(
                    message,
                    event,
                    publish,
                    default_exchange,
                    retry_count,
                    notready_count,
                    exc,
                )
                return

            row.outgoing_events = [
                {"routing_key": rk, "payload": payload} for rk, payload in outgoing
            ]
            await session.commit()

        # Publish only after the transaction is durable: consumers can never
        # observe uncommitted state. If we crash here, the redelivered message
        # hits the idempotency guard above and its stored outgoing events are
        # republished (never reprocessed), so no event is lost.
        try:
            for routing_key, payload in outgoing:
                await publish(routing_key, payload)
        except Exception as exc:
            self.log.exception(
                "failed to publish outgoing events for %s; will recover on redelivery",
                idempotency_key,
            )
            await self._handle_failure(
                message, event, publish, default_exchange, retry_count, notready_count, exc
            )
            return

        await message.ack()
        self.log.info("processed %s (attempt %d)", idempotency_key, attempt)

    async def _handle_failure(
        self,
        message: AbstractIncomingMessage,
        event: dict,
        publish: PublishFn,
        default_exchange: aio_pika.abc.AbstractExchange,
        retry_count: int,
        notready_count: int,
        exc: Exception,
    ) -> None:
        order_id = event.get("order_id", "?")

        # Fast, cheap retries when the event arrived before the upstream
        # worker committed. These do not consume the main retry budget.
        if isinstance(exc, UpstreamNotReady):
            if notready_count < self.settings.notready_max_retries:
                delay = self.settings.notready_delay_seconds
                await default_exchange.publish(
                    build_message(
                        event,
                        retry_count=retry_count,
                        notready_count=notready_count + 1,
                        expiration_ms=int(delay * 1000),
                    ),
                    routing_key=retry_queue_name(self.queue_name),
                )
                await message.ack()
                self.log.info(
                    "order %s not yet committed upstream; rechecking in %.1fs "
                    "(notready attempt %d)",
                    order_id,
                    delay,
                    notready_count + 1,
                )
                return
            self.log.error(
                "order %s never became ready after %d rechecks; dead-lettering",
                order_id,
                notready_count,
            )
            await message.nack(requeue=False)
            return

        if retry_count < self.settings.max_retries:
            delay = retry_delay_seconds(retry_count + 1, self.settings.retry_base_delay_seconds)
            retry_message = build_message(
                event,
                retry_count=retry_count + 1,
                expiration_ms=int(delay * 1000),
            )
            # Publish straight to the retry queue via the default exchange.
            # When the per-message TTL expires, the queue dead-letters the
            # message back to the topic exchange for redelivery.
            await default_exchange.publish(
                retry_message, routing_key=retry_queue_name(self.queue_name)
            )
            await message.ack()
            self.log.warning(
                "attempt %d for order %s failed (%s); retrying in %.1fs",
                retry_count + 1,
                order_id,
                exc,
                delay,
            )
            return

        self.log.error(
            "order %s failed permanently after %d attempts (%s); dead-lettering",
            order_id,
            retry_count + 1,
            exc,
        )
        async with self.session_factory() as session:
            try:
                await self.on_permanent_failure(session, event, publish)
                await session.commit()
            except Exception as hook_exc:
                await session.rollback()
                self.log.exception(
                    "on_permanent_failure hook failed for order %s: %s",
                    order_id,
                    hook_exc,
                )
        # The queue's x-dead-letter-exchange routes this to orders.dead-letter.queue.
        await message.nack(requeue=False)
