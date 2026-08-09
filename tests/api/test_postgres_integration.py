"""Opt-in PostgreSQL integration coverage for the real AsyncSession boundary."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app_api.database import get_session
from app_api.db_schema import projects, telemetry_outbox, telemetry_runs
from app_api.ingest import get_dispatcher
from app_api.main import create_app
from app_api.project_keys import ensure_project, provision_project
from app_api.rate_limit import get_rate_limiter


class RecordingDispatcher:
    def __init__(self):
        self.events = []

    async def dispatch_event(self, event_id):
        self.events.append(event_id)


class AllowingRateLimiter:
    async def check(self, project_id):
        return None


def _test_database_url() -> str:
    configured = os.getenv("DRIFTGUARD_TEST_DATABASE_URL")
    if not configured:
        pytest.skip("set DRIFTGUARD_TEST_DATABASE_URL to a migrated PostgreSQL database")
    if configured.startswith("postgresql://"):
        return configured.replace("postgresql://", "postgresql+asyncpg://", 1)
    return configured


@pytest.mark.asyncio
async def test_real_async_session_auth_and_atomic_ingest() -> None:
    engine = create_async_engine(_test_database_url())
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    raw_key = "dg_live_integration_" + uuid4().hex
    dispatcher = RecordingDispatcher()
    project_id = None

    try:
        async with session_factory() as bootstrap_session:
            credentials = await provision_project(
                bootstrap_session,
                name="integration-test",
                raw_api_key=raw_key,
            )
        project_id = credentials.project_id

        app = create_app()

        async def session_override():
            async with session_factory() as session:
                yield session

        app.dependency_overrides[get_session] = session_override
        app.dependency_overrides[get_dispatcher] = lambda: dispatcher
        app.dependency_overrides[get_rate_limiter] = lambda: AllowingRateLimiter()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.post(
                "/api/v1/logs",
                headers={"X-API-Key": raw_key},
                json={
                    "session_id": "integration-session",
                    "prompt_text": "integration prompt",
                    "output_text": "integration output",
                    "metadata": {"source": "pytest"},
                },
            )

        assert response.status_code == 202, response.text
        run_id = response.json()["run_id"]
        async with session_factory() as session:
            run = (
                await session.execute(
                    select(telemetry_runs).where(telemetry_runs.c.id == run_id)
                )
            ).mappings().one()
            event = (
                await session.execute(
                    select(telemetry_outbox).where(telemetry_outbox.c.run_id == run_id)
                )
            ).mappings().one()

        assert run["project_id"] == project_id
        assert run["raw_metadata"] == {"source": "pytest"}
        assert event["event_type"] == "TELEMETRY_INGESTED"
        assert event["status"] == "PENDING"
        assert event["payload"] == {
            "event_id": str(event["id"]),
            "run_id": str(run["id"]),
        }
        assert dispatcher.events == [event["id"]]
    finally:
        if project_id is not None:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(projects).where(projects.c.id == project_id)
                )
        await engine.dispose()


@pytest.mark.asyncio
async def test_real_concurrent_bootstrap_is_idempotent_and_rotatable() -> None:
    engine = create_async_engine(_test_database_url())
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    project_name = "bootstrap-integration-" + uuid4().hex
    first_key = "dg_live_bootstrap_" + uuid4().hex
    second_key = "dg_live_bootstrap_" + uuid4().hex
    project_id = None

    async def ensure(raw_key: str):
        async with session_factory() as session:
            return await ensure_project(
                session,
                name=project_name,
                raw_api_key=raw_key,
            )

    try:
        first, second = await asyncio.gather(ensure(first_key), ensure(first_key))
        project_id = first.project_id
        assert second.project_id == project_id
        assert sum((first.created, second.created)) == 1

        rotated = await ensure(second_key)
        assert rotated.project_id == project_id
        assert rotated.created is False
        assert rotated.key_updated is True

        unchanged = await ensure(second_key)
        assert unchanged.key_updated is False

        async with session_factory() as session:
            count = await session.scalar(
                select(func.count(projects.c.id)).where(projects.c.name == project_name)
            )
        assert count == 1
    finally:
        if project_id is not None:
            async with engine.begin() as connection:
                await connection.execute(
                    delete(projects).where(projects.c.id == project_id)
                )
        await engine.dispose()
