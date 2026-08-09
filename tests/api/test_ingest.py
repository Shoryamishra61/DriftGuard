from __future__ import annotations

from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy.exc import SQLAlchemyError

from app_api.auth import AuthenticatedProject, api_key_hash_from_header, resolve_project
from app_api.database import get_session
from app_api.ingest import get_dispatcher
from app_api.main import create_app
from app_api.rate_limit import get_rate_limiter

from .fakes import FakeResult, FakeSession

VALID_PAYLOAD = {
    "session_id": "sess-123",
    "prompt_text": "What is the standard dose?",
    "output_text": "Use the clinically approved weight-based dose.",
    "metadata": {"model": "example", "latency_ms": 12},
}


class RecordingDispatcher:
    def __init__(self, events: list[str]):
        self.events = events
        self.event_ids: list[UUID] = []

    async def dispatch_event(self, event_id: UUID):
        self.events.append("dispatch")
        self.event_ids.append(event_id)


class AllowingRateLimiter:
    def __init__(self, events: list[str]):
        self.events = events
        self.project_ids = []

    async def check(self, project_id):
        self.events.append("rate_limit")
        self.project_ids.append(project_id)


async def _request(app, payload=VALID_PAYLOAD):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        return await client.post(
            "/api/v1/logs",
            headers={"X-API-Key": "not-used-by-override"},
            json=payload,
        )


@pytest.mark.asyncio
async def test_ingest_commits_run_and_outbox_before_dispatching() -> None:
    events: list[str] = []
    project_id = uuid4()
    session = FakeSession(events=events, results=[FakeResult(scalar=project_id)])
    dispatcher = RecordingDispatcher(events)
    limiter = AllowingRateLimiter(events)
    app = create_app()

    async def session_override():
        yield session

    async def project_override():
        return AuthenticatedProject(id=project_id)

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[resolve_project] = project_override
    app.dependency_overrides[get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_rate_limiter] = lambda: limiter

    response = await _request(app)

    assert response.status_code == 202
    run_id = UUID(response.json()["run_id"])
    assert [statement.table.name for statement in session.statements[1:]] == [
        "telemetry_runs",
        "telemetry_outbox",
    ]
    auth_values = session.statements[0].compile().params
    run_values = session.statements[1].compile().params
    outbox_values = session.statements[2].compile().params
    assert auth_values["api_key_hash_1"] == api_key_hash_from_header(
        "not-used-by-override"
    )
    assert run_values["id"] == run_id
    assert run_values["project_id"] == project_id
    assert run_values["raw_metadata"] == VALID_PAYLOAD["metadata"]
    assert outbox_values["run_id"] == run_id
    assert outbox_values["event_type"] == "TELEMETRY_INGESTED"
    assert outbox_values["status"] == "PENDING"
    assert outbox_values["payload"] == {
        "event_id": str(outbox_values["id"]),
        "run_id": str(run_id),
    }
    assert VALID_PAYLOAD["output_text"] not in str(outbox_values["payload"])
    assert events.index("transaction.commit") < events.index("dispatch")
    assert events.index("rate_limit") < events.index("transaction.begin")
    assert dispatcher.event_ids == [outbox_values["id"]]
    assert limiter.project_ids == [project_id]


@pytest.mark.asyncio
async def test_ingest_rolls_back_both_writes_and_does_not_dispatch() -> None:
    events: list[str] = []
    session = FakeSession(
        events=events,
        results=[FakeResult(scalar=uuid4())],
        failure=SQLAlchemyError("database DSN with secret"),
        fail_on_execute=3,
    )
    dispatcher = RecordingDispatcher(events)
    limiter = AllowingRateLimiter(events)
    app = create_app()

    async def session_override():
        yield session

    async def project_override():
        return AuthenticatedProject(id=uuid4())

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[resolve_project] = project_override
    app.dependency_overrides[get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_rate_limiter] = lambda: limiter

    response = await _request(app)

    assert response.status_code == 503
    assert response.json() == {"detail": "telemetry storage temporarily unavailable"}
    assert "transaction.rollback" in events
    assert "dispatch" not in events
    assert "secret" not in response.text


@pytest.mark.asyncio
async def test_text_limit_counts_utf8_bytes_and_sanitizes_validation_response() -> None:
    session = FakeSession(results=[FakeResult(scalar=uuid4())])
    dispatcher = RecordingDispatcher([])
    limiter = AllowingRateLimiter([])
    app = create_app()

    async def session_override():
        yield session

    async def project_override():
        return AuthenticatedProject(id=uuid4())

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[resolve_project] = project_override
    app.dependency_overrides[get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    oversized = "é" * ((50 * 1024 // 2) + 1)

    response = await _request(app, {**VALID_PAYLOAD, "output_text": oversized})

    assert response.status_code == 422
    assert response.json()["detail"] == "request validation failed"
    assert oversized[:100] not in response.text
    assert session.statements == []
    assert dispatcher.event_ids == []


@pytest.mark.asyncio
async def test_unknown_fields_are_rejected() -> None:
    session = FakeSession(results=[FakeResult(scalar=uuid4())])
    dispatcher = RecordingDispatcher([])
    limiter = AllowingRateLimiter([])
    app = create_app()

    async def session_override():
        yield session

    async def project_override():
        return AuthenticatedProject(id=uuid4())

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[resolve_project] = project_override
    app.dependency_overrides[get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    response = await _request(app, {**VALID_PAYLOAD, "project_id": str(uuid4())})
    assert response.status_code == 422
    assert any(error["code"] == "extra_forbidden" for error in response.json()["errors"])
