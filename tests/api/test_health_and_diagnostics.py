from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from app_api.auth import AuthenticatedProject, resolve_project
from app_api.config import Settings
from app_api.diagnostics import InfrastructureProbe, get_infrastructure_probe
from app_api.health import get_readiness_probe
from app_api.main import create_app
from app_api.qdrant import ping_qdrant
from app_api.runtime import RuntimeResources

from .fakes import FakeValkey


class FakeConnection:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def execute(self, statement):
        return None


class FakePool:
    def size(self):
        return 10

    def checkedin(self):
        return 7

    def checkedout(self):
        return 3

    def overflow(self):
        return 0


class FakeEngine:
    def __init__(self):
        self.sync_engine = SimpleNamespace(pool=FakePool())

    def connect(self):
        return FakeConnection()


async def _get(app, path: str, headers: dict[str, str] | None = None):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        return await client.get(path, headers=headers)


@pytest.mark.asyncio
async def test_liveness_does_not_claim_dependency_readiness() -> None:
    app = create_app()
    live = await _get(app, "/healthz")
    ready = await _get(app, "/status")

    assert live.status_code == 200
    assert live.json() == {"status": "alive"}
    assert live.headers["server-timing"].startswith("app;dur=")
    assert ready.status_code == 503
    assert ready.json()["status"] == "not_ready"
    assert ready.json()["services"] == {
        "postgres": "unavailable",
        "valkey": "unavailable",
        "qdrant": "unavailable",
    }


@pytest.mark.asyncio
async def test_status_reports_all_three_dependencies() -> None:
    class HealthyProbe:
        async def snapshot(self):
            return True, {
                "postgres": "healthy",
                "valkey": "healthy",
                "qdrant": "healthy",
            }

    app = create_app()
    app.dependency_overrides[get_readiness_probe] = lambda: HealthyProbe()

    response = await _get(app, "/status")

    assert response.status_code == 200
    assert response.json()["services"] == {
        "postgres": "healthy",
        "valkey": "healthy",
        "qdrant": "healthy",
    }


@pytest.mark.asyncio
async def test_qdrant_probe_uses_private_http_and_api_key() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["api_key"] = request.headers["api-key"]
        return httpx.Response(200, json={"result": {"collections": []}})

    settings = Settings(qdrant_host="qdrant", qdrant_api_key="secret")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        trust_env=False,
    ) as client:
        await ping_qdrant(client, settings)

    assert captured == {
        "url": "http://qdrant:6333/collections",
        "api_key": "secret",
    }


@pytest.mark.asyncio
async def test_infrastructure_pulse_reports_real_probe_values() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/collections/drift_baselines"
        return httpx.Response(200, json={"result": {"points_count": 321}})

    settings = Settings(
        database_url="postgresql://user:password@db:5432/driftguard",
        qdrant_host="qdrant",
        qdrant_api_key="secret",
    )
    valkey = FakeValkey()
    valkey.queues[settings.queue_name].extend(["a", "b"])
    valkey.values["driftguard:worker:heartbeat"] = json.dumps(
        {
            "worker_id": "worker-1",
            "status": "idle",
            "active_threads": 1,
            "current_run_id": None,
            "last_completed_at": "2026-08-09T00:00:00Z",
            "timestamp": "2026-08-09T00:00:01Z",
        }
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        runtime = RuntimeResources(
            settings=settings,
            engine=FakeEngine(),
            session_factory=None,
            valkey=valkey,
            http_client=client,
            dispatcher=None,
            ready=True,
        )
        snapshot = await InfrastructureProbe(runtime).snapshot()

    assert snapshot.status == "healthy"
    assert snapshot.services["postgres"]["pool"]["checked_out"] == 3
    assert snapshot.services["valkey"]["queue_depth"] == 2
    assert snapshot.services["qdrant"]["vector_count"] == 321
    assert snapshot.services["worker"]["worker_id"] == "worker-1"


@pytest.mark.asyncio
async def test_pulse_endpoint_is_authenticated_and_uses_probe_dependency() -> None:
    class Probe:
        async def snapshot(self):
            return {
                "timestamp": "2026-08-09T00:00:00Z",
                "status": "degraded",
                "services": {"qdrant": {"status": "unavailable"}},
            }

    admin_token = "a" * 32
    app = create_app(Settings(admin_token=admin_token))

    async def project_override():
        return AuthenticatedProject(id=__import__("uuid").uuid4())

    app.dependency_overrides[resolve_project] = project_override
    app.dependency_overrides[get_infrastructure_probe] = lambda: Probe()

    response = await _get(
        app,
        "/api/v1/diagnostics/pulse",
        headers={
            "X-API-Key": "unused",
            "X-DriftGuard-Admin-Token": admin_token,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"


@pytest.mark.asyncio
async def test_lifespan_gates_startup_on_postgres_valkey_and_qdrant(monkeypatch) -> None:
    calls = []

    async def postgres_probe(engine):
        calls.append("postgres")

    async def valkey_probe(client):
        calls.append("valkey")

    async def qdrant_probe(client, settings):
        calls.append("qdrant")

    class IdleDispatcher:
        def __init__(self, *args):
            self.stop_event = asyncio.Event()

        async def run(self):
            await self.stop_event.wait()

        def stop(self):
            self.stop_event.set()

    monkeypatch.setattr("app_api.main.ping_database", postgres_probe)
    monkeypatch.setattr("app_api.main.ping_valkey", valkey_probe)
    monkeypatch.setattr("app_api.main.ping_qdrant", qdrant_probe)
    monkeypatch.setattr("app_api.main.OutboxDispatcher", IdleDispatcher)
    settings = Settings(
        database_url="postgresql://user:password@db:5432/driftguard",
        qdrant_host="qdrant",
        qdrant_api_key="qdrant-secret",
        admin_token="a" * 32,
    )
    app = create_app(settings)

    async with app.router.lifespan_context(app):
        assert app.state.runtime.ready is True

    assert sorted(calls) == ["postgres", "qdrant", "valkey"]
    assert app.state.runtime.ready is False
