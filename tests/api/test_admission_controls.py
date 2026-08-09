from __future__ import annotations

from collections import defaultdict
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from app_api.auth import AuthenticatedProject, resolve_project
from app_api.config import Settings
from app_api.database import get_session
from app_api.ingest import get_dispatcher
from app_api.main import create_app
from app_api.rate_limit import (
    ProjectRateLimiter,
    RateLimitExceeded,
    get_rate_limiter,
)
from app_api.schemas import TelemetryPayload

from .fakes import FakeResult, FakeSession

VALID_PAYLOAD = {
    "session_id": "sess-admission",
    "prompt_text": "prompt",
    "output_text": "output",
    "metadata": {"source": "test"},
}


class CounterValkey:
    def __init__(self, events=None):
        self.counts = defaultdict(int)
        self.keys = []
        self.failure = None
        self.events = events

    async def eval(self, script, numkeys, key, window):
        del script, numkeys
        if self.events is not None:
            self.events.append("rate_limit.attempt")
        if self.failure is not None:
            raise self.failure
        self.keys.append(key)
        self.counts[key] += 1
        return [self.counts[key], window]


class RecordingDispatcher:
    def __init__(self):
        self.events = []

    async def dispatch_event(self, event_id):
        self.events.append(event_id)


class RejectingLimiter:
    async def check(self, project_id):
        raise RateLimitExceeded(retry_after_seconds=7)


class RecordingLimiter:
    def __init__(self):
        self.projects = []

    async def check(self, project_id):
        self.projects.append(project_id)


async def _post(app, *, headers=None, content=None, json=None):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        return await client.post(
            "/api/v1/logs",
            headers=headers,
            content=content,
            json=json,
        )


@pytest.mark.asyncio
async def test_atomic_rate_limiter_uses_only_project_scoped_key() -> None:
    project_id = uuid4()
    events = []
    valkey = CounterValkey(events)
    settings = Settings(ingest_rate_limit_requests=1)
    limiter = ProjectRateLimiter(valkey, settings)

    await limiter.check(project_id)
    with pytest.raises(RateLimitExceeded) as exceeded:
        await limiter.check(project_id)

    assert exceeded.value.retry_after_seconds == 1
    assert valkey.keys == [
        f"driftguard:rate-limit:ingest:{project_id}",
        f"driftguard:rate-limit:ingest:{project_id}",
    ]


@pytest.mark.asyncio
async def test_rate_limit_response_is_429_with_retry_after() -> None:
    app = create_app()
    session = FakeSession()

    async def project_override():
        return AuthenticatedProject(id=uuid4())

    async def session_override():
        yield session

    app.dependency_overrides[resolve_project] = project_override
    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_rate_limiter] = lambda: RejectingLimiter()
    app.dependency_overrides[get_dispatcher] = lambda: RecordingDispatcher()

    response = await _post(
        app,
        headers={"X-API-Key": "valid-project-key"},
        json=VALID_PAYLOAD,
    )

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "7"
    assert session.statements == []


@pytest.mark.asyncio
async def test_limiter_outage_fails_open_without_holding_write_transaction() -> None:
    project_id = uuid4()
    events = []
    valkey = CounterValkey(events)
    valkey.failure = ConnectionError("redis://:secret@cache:6379")
    settings = Settings(rate_limit_fail_open=True, dependency_timeout_seconds=0.1)
    limiter = ProjectRateLimiter(valkey, settings)
    session = FakeSession(results=[FakeResult(scalar=project_id)], events=events)
    dispatcher = RecordingDispatcher()
    app = create_app(settings)

    async def project_override():
        return AuthenticatedProject(id=project_id)

    async def session_override():
        yield session

    app.dependency_overrides[resolve_project] = project_override
    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    app.dependency_overrides[get_dispatcher] = lambda: dispatcher

    response = await _post(
        app,
        headers={"X-API-Key": "valid-project-key"},
        json=VALID_PAYLOAD,
    )

    assert response.status_code == 202
    assert events.index("rate_limit.attempt") < events.index("transaction.begin")
    assert dispatcher.events


@pytest.mark.asyncio
async def test_unknown_api_key_never_creates_rate_limit_key() -> None:
    session = FakeSession(results=[FakeResult(scalar=None)])
    limiter = RecordingLimiter()
    app = create_app()

    async def session_override():
        yield session

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    app.dependency_overrides[get_dispatcher] = lambda: RecordingDispatcher()

    response = await _post(
        app,
        headers={"X-API-Key": "attacker-controlled-random-key"},
        json=VALID_PAYLOAD,
    )

    assert response.status_code == 401
    assert limiter.projects == []


@pytest.mark.asyncio
async def test_declared_oversized_body_is_rejected_before_parsing() -> None:
    limit = 128 * 1024
    app = create_app(Settings(max_request_bytes=limit))

    response = await _post(
        app,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(limit + 1),
            "X-API-Key": "unused",
        },
        content=b"{}",
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "request body too large"}


@pytest.mark.asyncio
async def test_chunked_oversized_body_is_rejected_while_streaming() -> None:
    limit = 128 * 1024
    app = create_app(Settings(max_request_bytes=limit))

    async def chunks():
        yield b"{" + (b" " * (70 * 1024))
        yield b" " * (70 * 1024) + b"}"

    response = await _post(
        app,
        headers={"Content-Type": "application/json", "X-API-Key": "unused"},
        content=chunks(),
    )

    assert response.status_code == 413
    assert response.json() == {"detail": "request body too large"}


def test_metadata_size_and_depth_are_bounded() -> None:
    with pytest.raises(ValidationError):
        TelemetryPayload.model_validate(
            {**VALID_PAYLOAD, "metadata": {"payload": "x" * (64 * 1024)}}
        )

    nested = "leaf"
    for _ in range(9):
        nested = {"child": nested}
    with pytest.raises(ValidationError):
        TelemetryPayload.model_validate({**VALID_PAYLOAD, "metadata": nested})
