"""PostgreSQL engine, session, and health-check primitives."""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app_api.config import Settings

SessionFactory = async_sessionmaker[AsyncSession]


def create_engine(settings: Settings) -> AsyncEngine:
    """Create a bounded async PostgreSQL pool without opening a connection."""

    return create_async_engine(
        settings.database_dsn,
        pool_pre_ping=True,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_recycle=1800,
        connect_args={
            "timeout": settings.dependency_timeout_seconds,
            "server_settings": {"application_name": "driftguard-api"},
        },
    )


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def ping_database(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise RuntimeError("application runtime is not initialized")
    async with runtime.session_factory() as session:
        yield session

