"""Async database engine / session factory.

Supports PostgreSQL (production / Docker Compose) and SQLite (local dev and
tests) through the DATABASE_URL setting, e.g.:

    postgresql+asyncpg://quickbite:quickbite@localhost:5432/quickbite
    sqlite+aiosqlite:///./quickbite.db
"""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .config import settings
from .models import Base

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        connect_args = {}
        if settings.database_url.startswith("sqlite"):
            # Needed because async sessions may hop threads.
            connect_args["check_same_thread"] = False
        _engine = create_async_engine(
            settings.database_url, connect_args=connect_args, pool_pre_ping=True
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(), class_=AsyncSession, expire_on_commit=False
        )
    return _session_factory


async def init_db(retries: int = 10) -> None:
    """Create tables if they do not exist (dev/test convenience).

    Production deployments would use Alembic migrations instead.

    Multiple processes (API + workers) may call this concurrently at startup;
    concurrent CREATE TABLE can race in PostgreSQL, so transient failures are
    retried with a short backoff.
    """
    import asyncio
    import logging

    log = logging.getLogger("quickbite.db")
    engine = get_engine()
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            return
        except Exception as exc:
            last_exc = exc
            log.warning("init_db attempt %d/%d failed: %s", attempt, retries, exc)
            await asyncio.sleep(1)
    raise RuntimeError(f"init_db failed after {retries} attempts") from last_exc


async def dispose_engine() -> None:
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


async def check_db() -> bool:
    """Lightweight connectivity probe used by the /health endpoint."""
    from sqlalchemy import text

    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def session_scope() -> AsyncIterator[AsyncSession]:
    """Yield a session; provided as a helper for non-FastAPI code."""
    async with get_session_factory()() as session:
        yield session
