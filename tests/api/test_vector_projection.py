from __future__ import annotations

import json
from uuid import uuid4

import httpx
import pytest

from app_api.auth import AuthenticatedProject, resolve_project
from app_api.config import Settings
from app_api.main import create_app
from app_api.qdrant import (
    VectorProjectionReader,
    VectorProjectionUnavailable,
    deterministic_projection,
    get_vector_projection_reader,
)

ADMIN_TOKEN = "admin-token-" + "x" * 32


def _vector(value: float) -> list[float]:
    return [value + index / 10_000 for index in range(384)]


@pytest.mark.asyncio
async def test_projection_scroll_is_filtered_and_revalidates_every_tenant_payload() -> None:
    project_id = uuid4()
    baseline_id = uuid4()
    evaluation_id = uuid4()
    other_tenant_id = uuid4()
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "result": {
                    "points": [
                        {
                            "id": str(baseline_id),
                            "vector": _vector(0.01),
                            "payload": {
                                "project_id": str(project_id),
                                "point_type": "baseline",
                                "baseline_set": "gold-v1",
                            },
                        },
                        {
                            "id": str(evaluation_id),
                            "vector": _vector(-0.02),
                            "payload": {
                                "project_id": str(project_id),
                                "point_type": "evaluation",
                                "run_id": str(evaluation_id),
                                "drift_distance": 0.42,
                                "matched_baseline_id": str(baseline_id),
                            },
                        },
                        {
                            "id": str(other_tenant_id),
                            "vector": _vector(0.03),
                            "payload": {
                                "project_id": str(uuid4()),
                                "point_type": "baseline",
                                "baseline_set": "private-other-tenant",
                            },
                        },
                    ],
                    "next_page_offset": str(uuid4()),
                }
            },
        )

    settings = Settings(qdrant_host="qdrant", qdrant_api_key="secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        page = await VectorProjectionReader(client, settings).fetch(project_id, 25)

    assert [point.id for point in page.points] == [baseline_id, evaluation_id]
    assert page.points[0].baseline_set == "gold-v1"
    assert page.points[1].run_id == evaluation_id
    assert page.points[1].drift_distance == 0.42
    assert page.has_more is True
    assert captured["url"] == (
        "http://qdrant:6333/collections/drift_baselines/points/scroll"
    )
    request_body = captured["body"]
    assert isinstance(request_body, dict)
    assert request_body["filter"]["must"] == [
        {"key": "project_id", "match": {"value": str(project_id)}},
        {
            "key": "point_type",
            "match": {"any": ["baseline", "evaluation"]},
        },
    ]
    assert request_body["limit"] == 25
    assert request_body["with_vector"] is True


def test_fallback_projection_is_stable_and_rejects_malformed_vectors() -> None:
    vector = _vector(0.01)

    first = deterministic_projection(vector)
    second = deterministic_projection(list(vector))

    assert first == second
    assert first is not None
    assert deterministic_projection(vector[:-1]) is None
    invalid = list(vector)
    invalid[10] = float("nan")
    assert deterministic_projection(invalid) is None


@pytest.mark.asyncio
async def test_projection_endpoint_never_serializes_raw_vectors() -> None:
    project_id = uuid4()
    point_id = uuid4()

    class Reader:
        async def fetch(self, requested_project_id, limit):
            assert requested_project_id == project_id
            assert limit == 1
            from app_api.qdrant import ProjectedVector, ProjectionPage

            return ProjectionPage(
                points=[
                    ProjectedVector(
                        id=point_id,
                        point_type="baseline",
                        x=1.25,
                        y=-0.75,
                        run_id=None,
                        baseline_set="gold",
                        drift_distance=None,
                        matched_baseline_id=None,
                    )
                ],
                has_more=False,
            )

    app = create_app(Settings(admin_token=ADMIN_TOKEN))
    app.dependency_overrides[resolve_project] = lambda: AuthenticatedProject(id=project_id)
    app.dependency_overrides[get_vector_projection_reader] = lambda: Reader()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.get(
            "/api/v1/vectors/projection?limit=1",
            headers={
                "X-API-Key": "project-key",
                "X-DriftGuard-Admin-Token": ADMIN_TOKEN,
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "points": [
            {
                "id": str(point_id),
                "point_type": "baseline",
                "x": 1.25,
                "y": -0.75,
                "run_id": None,
                "baseline_set": "gold",
                "drift_distance": None,
                "matched_baseline_id": None,
            }
        ],
        "count": 1,
        "limit": 1,
        "has_more": False,
    }
    assert "vector" not in response.text


@pytest.mark.asyncio
async def test_projection_dependency_failure_is_generic_503() -> None:
    class Reader:
        async def fetch(self, project_id, limit):
            raise VectorProjectionUnavailable("secret qdrant details")

    app = create_app(Settings(admin_token=ADMIN_TOKEN))
    app.dependency_overrides[resolve_project] = lambda: AuthenticatedProject(id=uuid4())
    app.dependency_overrides[get_vector_projection_reader] = lambda: Reader()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.get(
            "/api/v1/vectors/projection",
            headers={
                "X-API-Key": "project-key",
                "X-DriftGuard-Admin-Token": ADMIN_TOKEN,
            },
        )

    assert response.status_code == 503
    assert response.json() == {"detail": "vector projection temporarily unavailable"}
    assert "secret qdrant details" not in response.text
