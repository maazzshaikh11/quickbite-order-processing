"""Shared pytest fixtures: isolated SQLite DB + test settings per test."""

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from quickbite.common.config import Settings
from quickbite.common.models import Base


@pytest.fixture
def test_settings(tmp_path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/test.db",
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        max_retries=2,
        retry_base_delay_seconds=0.01,
        prefetch_count=5,
        payment_failure_mode="never_fail",
        payment_failure_rate=0.0,
        payment_min_delay_seconds=0.0,
        payment_max_delay_seconds=0.0,
        restaurant_delay_seconds=0.0,
        delivery_delay_seconds=0.0,
        log_level="WARNING",
    )


@pytest.fixture
async def session_factory(test_settings):
    engine = create_async_engine(test_settings.database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest.fixture
def order_created_event():
    return {
        "event_type": "order.created",
        "order_id": "order_test1",
        "idempotency_key": "order_test1:created",
        "customer_name": "Amina Khan",
        "restaurant_id": "rest_123",
        "items": [
            {"name": "Chicken Biryani", "quantity": 2, "price": 12.5},
            {"name": "Mango Lassi", "quantity": 1, "price": 4.0},
        ],
        "total_amount": 29.0,
        "payment_behavior": "never_fail",
        "created_at": "2026-09-19T12:00:00+00:00",
    }
