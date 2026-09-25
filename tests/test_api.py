"""API tests: order creation, status lookup, validation, health."""

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from quickbite.api import routes
from quickbite.api.main import create_app
from quickbite.common.enums import OrderStatus
from quickbite.common.models import Order


class StubPublisher:
    connected = True

    def __init__(self):
        self.published: list[tuple[str, dict]] = []

    async def publish_event(self, routing_key: str, payload: dict) -> None:
        self.published.append((routing_key, payload))


@pytest.fixture
async def client(session_factory):
    app = create_app()
    publisher = StubPublisher()
    app.state.publisher = publisher

    async def override_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[routes.get_session] = override_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, publisher, session_factory


ORDER_BODY = {
    "customer_name": "Amina Khan",
    "restaurant_id": "rest_123",
    "items": [
        {"name": "Chicken Biryani", "quantity": 2, "price": 12.5},
        {"name": "Mango Lassi", "quantity": 1, "price": 4.0},
    ],
}


async def test_create_order_happy_path(client):
    ac, publisher, session_factory = client
    resp = await ac.post("/orders", json=ORDER_BODY)
    assert resp.status_code == 201
    data = resp.json()
    assert data["order_id"].startswith("order_")
    assert data["status"] == "PENDING"
    assert data["total_amount"] == 29.0
    assert data["message"] == "Order accepted and queued for processing."

    # Event was published to the broker with the PDF's routing key + schema.
    assert len(publisher.published) == 1
    routing_key, payload = publisher.published[0]
    assert routing_key == "order.created"
    assert payload["order_id"] == data["order_id"]
    assert payload["idempotency_key"] == f"{data['order_id']}:created"
    assert payload["total_amount"] == 29.0

    # Order persisted as PENDING.
    async with session_factory() as s:
        order = await s.get(Order, data["order_id"])
        assert order is not None
        assert order.status == OrderStatus.PENDING.value


async def test_get_order_status(client):
    ac, _, _ = client
    created = (await ac.post("/orders", json=ORDER_BODY)).json()
    resp = await ac.get(f"/orders/{created['order_id']}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["order_id"] == created["order_id"]
    assert data["status"] == "PENDING"
    assert data["restaurant_id"] == "rest_123"
    assert data["driver_id"] is None
    assert "created_at" in data and "updated_at" in data


async def test_get_order_not_found(client):
    ac, _, _ = client
    resp = await ac.get("/orders/order_nope")
    assert resp.status_code == 404


async def test_create_order_validation(client):
    ac, _, _ = client
    resp = await ac.post("/orders", json={"customer_name": "A"})
    assert resp.status_code == 422
    resp = await ac.post(
        "/orders",
        json={"customer_name": "A", "restaurant_id": "r", "items": []},
    )
    assert resp.status_code == 422


async def test_list_orders(client):
    ac, _, _ = client
    await ac.post("/orders", json=ORDER_BODY)
    await ac.post("/orders", json=ORDER_BODY)
    resp = await ac.get("/orders?limit=10")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


async def test_health_reports_dependencies(client):
    ac, publisher, _ = client
    resp = await ac.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["rabbitmq_connected"] is True
    assert data["database_connected"] is True

    publisher.connected = False
    data = (await ac.get("/health")).json()
    assert data["status"] == "degraded"
    assert data["rabbitmq_connected"] is False


async def test_create_order_broker_down_returns_503(client):
    ac, publisher, session_factory = client
    publisher.connected = False
    resp = await ac.post("/orders", json=ORDER_BODY)
    assert resp.status_code == 503
    # Nothing persisted.
    async with session_factory() as s:
        rows = (await s.execute(select(Order))).scalars().all()
        assert rows == []
