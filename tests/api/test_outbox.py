from __future__ import annotations

import json
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from app_api.config import Settings
from app_api.outbox import DispatchOutcome, OutboxDispatcher

from .fakes import FakeResult, FakeSession, FakeSessionFactory, FakeValkey


def _settings(**overrides) -> Settings:
    return Settings(
        database_url="postgresql://user:password@db:5432/driftguard",
        qdrant_host="qdrant",
        qdrant_api_key="qdrant-secret",
        dependency_timeout_seconds=1,
        **overrides,
    )


def _compiled_params(statement) -> dict:
    return statement.compile(dialect=postgresql.dialect()).params


@pytest.mark.asyncio
async def test_pending_event_is_lpush_published_and_marked_dispatched() -> None:
    event_id = uuid4()
    payload = {"event_id": str(event_id), "run_id": str(uuid4())}
    session = FakeSession(
        results=[
            FakeResult(
                rows=[
                    {
                        "id": event_id,
                        "payload": payload,
                        "retry_count": 0,
                        "status": "PENDING",
                    }
                ]
            )
        ]
    )
    valkey = FakeValkey()
    dispatcher = OutboxDispatcher(FakeSessionFactory(session), valkey, _settings())

    outcome = await dispatcher.dispatch_pending_once()

    assert outcome is DispatchOutcome.DISPATCHED
    assert json.loads(valkey.queues["drift_eval_queue"][0]) == payload
    assert ":delivery:0:published" in valkey.eval_calls[0][2]
    update_params = _compiled_params(session.statements[1])
    assert "DISPATCHED" in update_params.values()
    assert 0 in update_params.values()
    assert session.events.count("transaction.commit") == 1


@pytest.mark.asyncio
async def test_valkey_lua_publish_is_idempotent_within_delivery_generation() -> None:
    event_id = uuid4()
    payload = {"event_id": str(event_id), "run_id": str(uuid4())}
    valkey = FakeValkey()
    dispatcher = OutboxDispatcher(FakeSessionFactory(FakeSession()), valkey, _settings())

    first = await dispatcher._publish_once(event_id, 3, payload)
    second = await dispatcher._publish_once(event_id, 3, payload)

    assert first is True
    assert second is False
    assert len(valkey.queues["drift_eval_queue"]) == 1


@pytest.mark.asyncio
async def test_stale_dispatched_run_without_evaluation_gets_new_delivery_generation() -> None:
    event_id = uuid4()
    run_id = uuid4()
    payload = {"event_id": str(event_id), "run_id": str(run_id)}
    session = FakeSession(
        results=[
            FakeResult(
                rows=[
                    {
                        "id": event_id,
                        "payload": payload,
                        "retry_count": 0,
                        "status": "DISPATCHED",
                    }
                ]
            )
        ]
    )
    valkey = FakeValkey()
    dispatcher = OutboxDispatcher(FakeSessionFactory(session), valkey, _settings())

    outcome = await dispatcher.dispatch_pending_once()

    assert outcome is DispatchOutcome.DISPATCHED
    select_sql = str(
        session.statements[0].compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "telemetry_outbox.status = 'DISPATCHED'" in select_sql
    assert "telemetry_outbox.dispatch_time" in select_sql
    assert "telemetry_runs.status IN ('queued', 'processing')" in select_sql
    assert "NOT (EXISTS" in select_sql
    assert "FOR UPDATE OF telemetry_outbox SKIP LOCKED" in select_sql
    assert ":delivery:1:published" in valkey.eval_calls[0][2]
    update_params = _compiled_params(session.statements[1])
    assert 1 in update_params.values()


@pytest.mark.asyncio
async def test_publish_failure_records_safe_exponential_retry_without_secret_text() -> None:
    event_id = uuid4()
    session = FakeSession(
        results=[
            FakeResult(
                rows=[
                    {
                        "id": event_id,
                        "payload": {"event_id": str(event_id)},
                        "retry_count": 0,
                        "status": "PENDING",
                    }
                ]
            )
        ]
    )
    valkey = FakeValkey()
    valkey.raise_on_eval = ConnectionError("redis://:super-secret@cache:6379")
    dispatcher = OutboxDispatcher(FakeSessionFactory(session), valkey, _settings())

    outcome = await dispatcher.dispatch_pending_once()

    assert outcome is DispatchOutcome.DEFERRED
    params = _compiled_params(session.statements[1])
    assert "PENDING" in params.values()
    assert 1 in params.values()
    assert "queue_publish:ConnectionError" in params.values()
    assert all("super-secret" not in str(value) for value in params.values())


def test_retry_delay_is_bounded_exponential_backoff() -> None:
    dispatcher = OutboxDispatcher(
        FakeSessionFactory(FakeSession()),
        FakeValkey(),
        _settings(outbox_retry_base_seconds=2, outbox_retry_max_seconds=10),
    )
    assert [dispatcher._retry_delay(attempt) for attempt in range(1, 6)] == [2, 4, 8, 10, 10]
