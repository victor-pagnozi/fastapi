from sqlalchemy.ext.asyncio.session import AsyncSession

import os
import asyncio
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy import text


def _build_database_url() -> str:
    user = os.getenv("POSTGRES_USER", "admin")
    password = os.getenv("POSTGRES_PASSWORD", "admin")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "fastapi")

    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"


_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


async def init_db() -> None:
    global _engine, _session_factory

    if _engine is not None:
        return

    database_url = _build_database_url()
    _engine = create_async_engine(database_url, pool_pre_ping=True)
    _session_factory = async_sessionmaker[AsyncSession](_engine,
                                                        expire_on_commit=False)

    max_retries = int(os.getenv("DB_INIT_MAX_RETRIES", "5"))
    sleep_seconds = float(os.getenv("DB_INIT_SLEEP_SECONDS", "0.5"))
    last_exc: Exception | None = None

    for _ in range(max_retries):
        try:
            async with _engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            last_exc = None
            break

        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(sleep_seconds)

    if last_exc is not None:
        raise last_exc


async def close_db() -> None:
    global _engine, _session_factory

    if _engine is not None:
        await _engine.dispose()

    _engine = None
    _session_factory = None


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    if _session_factory is None:
        await init_db()

    assert _session_factory is not None
    session: AsyncSession = _session_factory()

    try:
        yield session
    finally:
        await session.close()


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError(
            "Database engine not initialized. Ensure init_db() ran on startup."
        )

    return _engine


async def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory

    if _session_factory is None:
        await init_db()

    assert _session_factory is not None

    return _session_factory
