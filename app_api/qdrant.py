"""Authenticated private-network Qdrant connectivity checks."""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import httpx
from fastapi import Request

from app_api.config import ConfigurationError, Settings

logger = logging.getLogger("driftguard.qdrant")
MAX_BASELINE_BATCH = 100
MAX_BASELINE_TEXT_BYTES = 50 * 1024
VECTOR_DIMENSION = 384
MAX_PROJECTION_POINTS = 500
_MASK_64 = (1 << 64) - 1


class VectorProjectionUnavailable(RuntimeError):
    """Raised when Qdrant cannot provide an authoritative projection."""


@dataclass(frozen=True, slots=True)
class ProjectedVector:
    id: UUID
    point_type: str
    x: float
    y: float
    run_id: UUID | None
    baseline_set: str | None
    drift_distance: float | None
    matched_baseline_id: UUID | None


@dataclass(frozen=True, slots=True)
class ProjectionPage:
    points: list[ProjectedVector]
    has_more: bool


def _projection_sign(index: int, seed: int) -> float:
    """Return one fixed Rademacher coefficient using SplitMix64 mixing."""

    value = (index + seed) & _MASK_64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & _MASK_64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & _MASK_64
    value ^= value >> 31
    return 1.0 if value & 1 else -1.0


_PROJECTION_X = tuple(
    _projection_sign(index, 0x9E3779B97F4A7C15) for index in range(VECTOR_DIMENSION)
)
_PROJECTION_Y = tuple(
    _projection_sign(index, 0xD1B54A32D192ED03) for index in range(VECTOR_DIMENSION)
)
_PROJECTION_SCALE = 1.0 / math.sqrt(VECTOR_DIMENSION)


def deterministic_projection(vector: Any) -> tuple[float, float] | None:
    """Apply fixed random axes so coordinates are stable across pages and deploys."""

    if not isinstance(vector, list) or len(vector) != VECTOR_DIMENSION:
        return None
    components: list[float] = []
    for raw_component in vector:
        if isinstance(raw_component, bool):
            return None
        try:
            component = float(raw_component)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(component):
            return None
        components.append(component)
    return (
        math.fsum(
            component * weight
            for component, weight in zip(components, _PROJECTION_X, strict=True)
        )
        * _PROJECTION_SCALE,
        math.fsum(
            component * weight
            for component, weight in zip(components, _PROJECTION_Y, strict=True)
        )
        * _PROJECTION_SCALE,
    )


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _stored_projection(payload: dict[str, Any]) -> tuple[float, float] | None:
    projection = payload.get("projection")
    if isinstance(projection, dict):
        x = _finite_float(projection.get("x"))
        y = _finite_float(projection.get("y"))
    else:
        x = _finite_float(payload.get("projection_x"))
        y = _finite_float(payload.get("projection_y"))
    return (x, y) if x is not None and y is not None else None


def _optional_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _projection_point(point: Any, project_id: UUID) -> ProjectedVector | None:
    if not isinstance(point, dict):
        return None
    payload = point.get("payload")
    if not isinstance(payload, dict) or str(payload.get("project_id")) != str(project_id):
        return None
    point_type = payload.get("point_type")
    if point_type not in {"baseline", "evaluation"}:
        return None
    try:
        point_id = UUID(str(point.get("id")))
    except (TypeError, ValueError):
        return None

    coordinates = _stored_projection(payload) or deterministic_projection(point.get("vector"))
    if coordinates is None:
        return None

    baseline_set = payload.get("baseline_set") if point_type == "baseline" else None
    if not isinstance(baseline_set, str) or not 1 <= len(baseline_set) <= 100:
        baseline_set = None
    drift_distance = (
        _finite_float(payload.get("drift_distance")) if point_type == "evaluation" else None
    )
    if drift_distance is not None and not 0.0 <= drift_distance <= 2.0:
        drift_distance = None
    return ProjectedVector(
        id=point_id,
        point_type=point_type,
        x=coordinates[0],
        y=coordinates[1],
        run_id=(
            _optional_uuid(payload.get("run_id")) if point_type == "evaluation" else None
        ),
        baseline_set=baseline_set,
        drift_distance=drift_distance,
        matched_baseline_id=(
            _optional_uuid(payload.get("matched_baseline_id"))
            if point_type == "evaluation"
            else None
        ),
    )


def qdrant_auth_headers(settings: Settings) -> dict[str, str]:
    if settings.qdrant_base_url is None or settings.qdrant_api_key is None:
        raise ConfigurationError("Qdrant URL/host and API key are required")
    return {"api-key": settings.qdrant_api_key.get_secret_value()}


async def ping_qdrant(client: httpx.AsyncClient, settings: Settings) -> None:
    """Verify the Qdrant service without depending on collection creation order."""

    base_url = settings.qdrant_base_url
    if base_url is None:
        raise ConfigurationError("Qdrant URL/host is required")
    response = await client.get(
        f"{base_url}/collections",
        headers=qdrant_auth_headers(settings),
    )
    response.raise_for_status()


class BaselineTextResolver:
    def __init__(self, client: httpx.AsyncClient, settings: Settings):
        self._client = client
        self._settings = settings

    async def resolve(
        self,
        project_id: UUID,
        baseline_ids: list[UUID],
    ) -> dict[UUID, str]:
        unique_ids = list(dict.fromkeys(baseline_ids))[:MAX_BASELINE_BATCH]
        if not unique_ids:
            return {}
        base_url = self._settings.qdrant_base_url
        if base_url is None or self._settings.qdrant_api_key is None:
            return {}

        try:
            response = await asyncio.wait_for(
                self._client.post(
                    f"{base_url}/collections/{self._settings.qdrant_collection}/points",
                    headers=qdrant_auth_headers(self._settings),
                    json={
                        "ids": [str(point_id) for point_id in unique_ids],
                        "with_payload": True,
                        "with_vector": False,
                    },
                ),
                timeout=self._settings.dependency_timeout_seconds,
            )
            response.raise_for_status()
            body = response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("baseline text lookup unavailable (%s)", type(exc).__name__)
            return {}

        raw_result = body.get("result") if isinstance(body, dict) else None
        points: Any = (
            raw_result.get("points") if isinstance(raw_result, dict) else raw_result
        )
        if not isinstance(points, list):
            return {}

        requested_ids = set(unique_ids)
        resolved: dict[UUID, str] = {}
        for point in points[:MAX_BASELINE_BATCH]:
            if not isinstance(point, dict):
                continue
            try:
                point_id = UUID(str(point.get("id")))
            except (TypeError, ValueError):
                continue
            payload = point.get("payload")
            if point_id not in requested_ids or not isinstance(payload, dict):
                continue
            if (
                str(payload.get("project_id")) != str(project_id)
                or payload.get("point_type") != "baseline"
            ):
                continue
            text_value = payload.get("text")
            if not isinstance(text_value, str):
                continue
            if len(text_value.encode("utf-8")) > MAX_BASELINE_TEXT_BYTES:
                continue
            resolved[point_id] = text_value
        return resolved


class VectorProjectionReader:
    def __init__(self, client: httpx.AsyncClient, settings: Settings):
        self._client = client
        self._settings = settings

    async def fetch(self, project_id: UUID, limit: int) -> ProjectionPage:
        if not 1 <= limit <= MAX_PROJECTION_POINTS:
            raise ValueError("projection limit must be between 1 and 500")
        base_url = self._settings.qdrant_base_url
        if base_url is None or self._settings.qdrant_api_key is None:
            raise VectorProjectionUnavailable("Qdrant is not configured")

        try:
            response = await asyncio.wait_for(
                self._client.post(
                    f"{base_url}/collections/"
                    f"{self._settings.qdrant_collection}/points/scroll",
                    headers=qdrant_auth_headers(self._settings),
                    json={
                        "filter": {
                            "must": [
                                {
                                    "key": "project_id",
                                    "match": {"value": str(project_id)},
                                },
                                {
                                    "key": "point_type",
                                    "match": {"any": ["baseline", "evaluation"]},
                                },
                            ]
                        },
                        "limit": limit,
                        "with_payload": True,
                        "with_vector": True,
                    },
                ),
                timeout=self._settings.dependency_timeout_seconds,
            )
            response.raise_for_status()
            body = response.json()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("vector projection lookup unavailable (%s)", type(exc).__name__)
            raise VectorProjectionUnavailable("Qdrant projection unavailable") from None

        raw_result = body.get("result") if isinstance(body, dict) else None
        if not isinstance(raw_result, dict) or not isinstance(
            raw_result.get("points"), list
        ):
            logger.warning("vector projection lookup returned an invalid response")
            raise VectorProjectionUnavailable("Qdrant projection response invalid")

        points = [
            projected
            for raw_point in raw_result["points"][:limit]
            if (projected := _projection_point(raw_point, project_id)) is not None
        ]
        return ProjectionPage(
            points=points,
            has_more=raw_result.get("next_page_offset") is not None,
        )


def get_baseline_text_resolver(request: Request) -> BaselineTextResolver:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise RuntimeError("application runtime is not initialized")
    return BaselineTextResolver(runtime.http_client, runtime.settings)


def get_vector_projection_reader(request: Request) -> VectorProjectionReader:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise RuntimeError("application runtime is not initialized")
    return VectorProjectionReader(runtime.http_client, runtime.settings)
