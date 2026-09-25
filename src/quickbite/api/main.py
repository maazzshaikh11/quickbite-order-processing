"""FastAPI application entrypoint: ``python -m quickbite.api.main``."""

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from ..common import db
from ..common.config import settings
from ..common.messaging import EventPublisher
from .routes import router

log = logging.getLogger("quickbite.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    await db.init_db()

    publisher = EventPublisher(settings.rabbitmq_url)
    # Bounded connect retries so a cold broker fails fast (compose restarts us).
    for attempt in range(1, 31):
        try:
            await publisher.connect()
            break
        except Exception as exc:
            log.warning("RabbitMQ not ready (attempt %d/30): %s", attempt, exc)
            await asyncio.sleep(2)
    else:
        raise RuntimeError("could not connect to RabbitMQ after 30 attempts")
    app.state.publisher = publisher
    log.info("order service ready")
    yield
    await publisher.close()
    await db.dispose_engine()


def create_app() -> FastAPI:
    app = FastAPI(
        title="QuickBite Order Service",
        description=(
            "Food delivery order processing: accepts orders over HTTP and "
            "processes them asynchronously via RabbitMQ workers."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "quickbite.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
