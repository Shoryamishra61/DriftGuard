from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from app_api.auth import AuthenticatedProject, resolve_project
from app_api.config import Settings
from app_api.database import get_session
from app_api.main import create_app
from app_api.qdrant import get_baseline_text_resolver, get_vector_projection_reader

from .fakes import FakeResult, FakeSession

ADMIN_TOKEN = "admin-token-" + "x" * 32


class EmptyBaselineResolver:
    async def resolve(self, project_id, baseline_ids):
        return {}


async def _request(app, method: str, path: str, headers=None, json=None):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        return await client.request(method, path, headers=headers, json=json)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", "/api/v1/alerts", None),
        (
            "POST",
            "/api/v1/alert-rules",
            {
                "rule_name": "critical",
                "threshold": 0.45,
                "action_type": "NOTIFY",
                "notification_target": "https://example.com/driftguard",
                "is_active": True,
            },
        ),
        ("GET", "/api/v1/diagnostics/pulse", None),
        ("GET", "/api/v1/vectors/projection", None),
    ],
)
async def test_ingestion_key_alone_cannot_read_or_mutate_admin_routes(
    method: str,
    path: str,
    body,
) -> None:
    app = create_app(Settings(admin_token=ADMIN_TOKEN))

    async def project_override():
        return AuthenticatedProject(id=uuid4())

    async def session_override():
        yield FakeSession()

    app.dependency_overrides[resolve_project] = project_override
    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_baseline_text_resolver] = lambda: EmptyBaselineResolver()
    app.dependency_overrides[get_vector_projection_reader] = lambda: object()

    response = await _request(
        app,
        method,
        path,
        headers={"X-API-Key": "valid-ingestion-key"},
        json=body,
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "invalid admin token"}


@pytest.mark.asyncio
async def test_dashboard_session_requires_and_accepts_both_credentials() -> None:
    project_id = uuid4()
    session = FakeSession(results=[FakeResult(scalar=project_id)])
    app = create_app(Settings(admin_token=ADMIN_TOKEN))

    async def session_override():
        yield session

    app.dependency_overrides[get_session] = session_override

    valid = await _request(
        app,
        "GET",
        "/api/v1/dashboard/session",
        headers={
            "X-API-Key": "valid-project-key",
            "X-DriftGuard-Admin-Token": ADMIN_TOKEN,
        },
    )

    assert valid.status_code == 204
    assert valid.content == b""


@pytest.mark.asyncio
async def test_dashboard_session_rejects_wrong_project_key_with_valid_admin_token() -> None:
    session = FakeSession(results=[FakeResult(scalar=None)])
    app = create_app(Settings(admin_token=ADMIN_TOKEN))

    async def session_override():
        yield session

    app.dependency_overrides[get_session] = session_override

    response = await _request(
        app,
        "GET",
        "/api/v1/dashboard/session",
        headers={
            "X-API-Key": "wrong-project-key",
            "X-DriftGuard-Admin-Token": ADMIN_TOKEN,
        },
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_rule_endpoint_enforces_deployment_webhook_allowlist() -> None:
    project_id = uuid4()
    rejected_session = FakeSession()
    app = create_app(Settings(admin_token=ADMIN_TOKEN))
    app.dependency_overrides[resolve_project] = lambda: AuthenticatedProject(id=project_id)

    async def rejected_session_override():
        yield rejected_session

    app.dependency_overrides[get_session] = rejected_session_override
    payload = {
        "rule_name": "critical",
        "threshold": 0.45,
        "action_type": "NOTIFY",
        "notification_target": "https://hooks.example.com/driftguard",
        "is_active": True,
    }
    rejected = await _request(
        app,
        "POST",
        "/api/v1/alert-rules",
        headers={
            "X-API-Key": "project-key",
            "X-DriftGuard-Admin-Token": ADMIN_TOKEN,
        },
        json=payload,
    )

    assert rejected.status_code == 422
    assert rejected_session.statements == []

    created_at = datetime(2026, 8, 9, tzinfo=timezone.utc)  # noqa: UP017
    accepted_session = FakeSession(
        results=[
            FakeResult(
                rows=[
                    {
                        "id": 1,
                        **payload,
                        "created_at": created_at,
                    }
                ]
            )
        ]
    )
    allowed_app = create_app(
        Settings(
            admin_token=ADMIN_TOKEN,
            webhook_allowed_hosts_csv="example.com",
        )
    )
    allowed_app.dependency_overrides[resolve_project] = lambda: AuthenticatedProject(
        id=project_id
    )

    async def accepted_session_override():
        yield accepted_session

    allowed_app.dependency_overrides[get_session] = accepted_session_override
    accepted = await _request(
        allowed_app,
        "POST",
        "/api/v1/alert-rules",
        headers={
            "X-API-Key": "project-key",
            "X-DriftGuard-Admin-Token": ADMIN_TOKEN,
        },
        json=payload,
    )

    assert accepted.status_code == 201
    assert accepted.json()["notification_target"] == payload["notification_target"]
