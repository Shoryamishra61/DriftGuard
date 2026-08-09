from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from app_api.config import Settings
from app_api.qdrant import BaselineTextResolver


@pytest.mark.asyncio
async def test_baseline_text_batch_enforces_project_and_point_type() -> None:
    project_id = uuid4()
    valid_id = uuid4()
    cross_tenant_id = uuid4()
    evaluation_id = uuid4()
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "result": [
                    {
                        "id": str(valid_id),
                        "payload": {
                            "project_id": str(project_id),
                            "point_type": "baseline",
                            "text": "Expected answer",
                        },
                    },
                    {
                        "id": str(cross_tenant_id),
                        "payload": {
                            "project_id": str(uuid4()),
                            "point_type": "baseline",
                            "text": "Other tenant secret",
                        },
                    },
                    {
                        "id": str(evaluation_id),
                        "payload": {
                            "project_id": str(project_id),
                            "point_type": "evaluation",
                            "text": "Incoming telemetry",
                        },
                    },
                ]
            },
        )

    settings = Settings(qdrant_host="qdrant", qdrant_api_key="secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        resolved = await BaselineTextResolver(client, settings).resolve(
            project_id,
            [valid_id, cross_tenant_id, evaluation_id],
        )

    assert resolved == {valid_id: "Expected answer"}
    assert len(requests) == 1
    assert requests[0].url == httpx.URL(
        "http://qdrant:6333/collections/drift_baselines/points"
    )
    assert requests[0].headers["api-key"] == "secret"


@pytest.mark.asyncio
async def test_baseline_lookup_failure_degrades_to_empty_mapping() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("qdrant unavailable", request=request)

    settings = Settings(qdrant_host="qdrant", qdrant_api_key="secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        resolved = await BaselineTextResolver(client, settings).resolve(
            uuid4(),
            [uuid4()],
        )

    assert resolved == {}
