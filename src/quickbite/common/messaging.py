"""RabbitMQ topology, publisher, and shared messaging helpers.

Topology (all durable):

    exchange  quickbite.orders (topic)
        order.created        -> payment.queue        -> payment worker
        order.paid           -> restaurant.queue     -> restaurant worker
        order.confirmed      -> delivery.queue       -> delivery worker
        order.#              -> notification.queue   -> notification worker

    exchange  quickbite.dlx (direct)                       # dead-letter exchange
        payment.queue        -> orders.dead-letter.queue   # poison messages

    retry queues: <queue>.retry (durable, per-message TTL via `expiration`)
        on expiry -> dead-lettered back to quickbite.orders with a fixed
        routing key that routes the message to its original queue, giving
        delayed retries without blocking any worker.

Reliability properties: durable exchanges/queues, persistent messages,
publisher confirms, manual acknowledgements, bounded retries, DLQ.
"""

import json
import logging
from datetime import UTC, datetime, timedelta

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message

log = logging.getLogger("quickbite.messaging")

TOPIC_EXCHANGE = "quickbite.orders"
DLX_NAME = "quickbite.dlx"
DEAD_LETTER_QUEUE = "orders.dead-letter.queue"

# worker_name -> queue configuration
WORKER_QUEUES: dict[str, dict] = {
    "payment": {
        "queue": "payment.queue",
        "routing_keys": ["order.created"],
        # Routing key used when a message expires out of payment.queue.retry.
        # It matches payment.queue's binding (and notification.queue's order.#,
        # which is harmless because notification delivery is idempotent).
        "retry_dl_routing_key": "order.created",
    },
    "restaurant": {
        "queue": "restaurant.queue",
        "routing_keys": ["order.paid"],
        "retry_dl_routing_key": "order.paid",
    },
    "delivery": {
        "queue": "delivery.queue",
        "routing_keys": ["order.confirmed"],
        "retry_dl_routing_key": "order.confirmed",
    },
    "notification": {
        "queue": "notification.queue",
        # `order.#` (not `order.*`): `*` matches exactly one word, so `order.*`
        # would miss three-word keys like `order.driver.assigned` and
        # `order.payment.failed`. `order.#` matches every order event.
        "routing_keys": ["order.#"],
        # Internal routing key: matches ONLY notification.queue's `order.#`
        # binding, so retried notification events don't fan out elsewhere.
        "retry_dl_routing_key": "order.retry",
    },
}

RETRY_QUEUE_SUFFIX = ".retry"
RETRY_COUNT_HEADER = "x-retry-count"
NOTREADY_COUNT_HEADER = "x-notready-count"


def retry_queue_name(queue_name: str) -> str:
    return f"{queue_name}{RETRY_QUEUE_SUFFIX}"


def retry_delay_seconds(attempt: int, base_delay: float) -> float:
    """Exponential backoff: base, 2*base, 4*base, ... (attempt is 1-based)."""
    return base_delay * (2 ** (attempt - 1))


async def declare_topology(
    channel: aio_pika.abc.AbstractChannel,
) -> dict[str, aio_pika.abc.AbstractQueue]:
    """Declare exchanges, queues, retry queues and bindings. Idempotent."""
    topic = await channel.declare_exchange(TOPIC_EXCHANGE, ExchangeType.TOPIC, durable=True)
    dlx = await channel.declare_exchange(DLX_NAME, ExchangeType.DIRECT, durable=True)

    # Dead-letter queue: collects messages that exhausted their retries
    # (or were poison). Bound per source queue name for easy triage.
    dlq = await channel.declare_queue(DEAD_LETTER_QUEUE, durable=True)
    queues: dict[str, aio_pika.abc.AbstractQueue] = {}

    for worker_name, cfg in WORKER_QUEUES.items():
        queue_name: str = cfg["queue"]
        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DLX_NAME,
                "x-dead-letter-routing-key": queue_name,
            },
        )
        for routing_key in cfg["routing_keys"]:
            await queue.bind(topic, routing_key=routing_key)
        await dlq.bind(dlx, routing_key=queue_name)

        # Retry queue: per-message TTL (`expiration`); on expiry the message is
        # dead-lettered back to the topic exchange with a fixed routing key.
        await channel.declare_queue(
            retry_queue_name(queue_name),
            durable=True,
            arguments={
                "x-dead-letter-exchange": TOPIC_EXCHANGE,
                "x-dead-letter-routing-key": cfg["retry_dl_routing_key"],
            },
        )
        queues[worker_name] = queue

    log.info(
        "RabbitMQ topology declared: exchange=%s dlx=%s queues=%s",
        TOPIC_EXCHANGE,
        DLX_NAME,
        [cfg["queue"] for cfg in WORKER_QUEUES.values()],
    )
    return queues


def build_message(
    payload: dict,
    *,
    retry_count: int = 0,
    notready_count: int = 0,
    expiration_ms: int | None = None,
) -> Message:
    """Build a persistent JSON message with idempotency-friendly metadata."""
    headers = {
        RETRY_COUNT_HEADER: retry_count,
        NOTREADY_COUNT_HEADER: notready_count,
    }
    return Message(
        body=json.dumps(payload).encode("utf-8"),
        content_type="application/json",
        delivery_mode=DeliveryMode.PERSISTENT,
        headers=headers,
        message_id=payload.get("idempotency_key", ""),
        timestamp=datetime.now(UTC),
        # Per-message TTL (aio-pika accepts a timedelta for expiration).
        expiration=timedelta(milliseconds=expiration_ms) if expiration_ms else None,
    )


class EventPublisher:
    """Publisher with confirms, used by the API (and workers for chaining)."""

    def __init__(self, amqp_url: str):
        self._amqp_url = amqp_url
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self._amqp_url)
        # Publisher confirms: publish() raises if the broker does not confirm.
        self._channel = await self._connection.channel(publisher_confirms=True)
        await declare_topology(self._channel)
        self._exchange = await self._channel.get_exchange(TOPIC_EXCHANGE, ensure=False)
        log.info("EventPublisher connected (publisher confirms enabled)")

    @property
    def connected(self) -> bool:
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
        )

    async def publish_event(self, routing_key: str, payload: dict) -> None:
        if not self.connected or self._exchange is None:
            raise RuntimeError("Publisher is not connected to RabbitMQ")
        message = build_message(payload)
        await self._exchange.publish(message, routing_key=routing_key)
        log.debug("Published %s for order %s", routing_key, payload.get("order_id"))

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None
            self._exchange = None
