"""Unauthenticated liveness and readiness endpoints for Zerops probes."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import JSONResponse

from app_api.database import ping_database
from app_api.qdrant import ping_qdrant
from app_api.runtime import RuntimeResources
from app_api.valkey import ping_valkey

router = APIRouter(tags=["health"])


class ReadinessProbe:
    def __init__(self, runtime: RuntimeResources | None):
        self._runtime = runtime

    async def snapshot(self) -> tuple[bool, dict[str, str]]:
        if self._runtime is None or not self._runtime.ready:
            return False, {
                "postgres": "unavailable",
                "valkey": "unavailable",
                "qdrant": "unavailable",
            }

        timeout = self._runtime.settings.dependency_timeout_seconds

        async def check(operation) -> str:
            try:
                await asyncio.wait_for(operation(), timeout=timeout)
                return "healthy"
            except asyncio.CancelledError:
                raise
            except Exception:
                return "unavailable"

        postgres, valkey, qdrant = await asyncio.gather(
            check(lambda: ping_database(self._runtime.engine)),
            check(lambda: ping_valkey(self._runtime.valkey)),
            check(
                lambda: ping_qdrant(
                    self._runtime.http_client,
                    self._runtime.settings,
                )
            ),
        )
        services = {"postgres": postgres, "valkey": valkey, "qdrant": qdrant}
        return all(value == "healthy" for value in services.values()), services


def get_readiness_probe(request: Request) -> ReadinessProbe:
    return ReadinessProbe(getattr(request.app.state, "runtime", None))


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/status")
async def readiness_status(
    probe: Annotated[ReadinessProbe, Depends(get_readiness_probe)],
) -> JSONResponse:
    ready, services = await probe.snapshot()
    payload: dict[str, Any] = {
        "status": "ready" if ready else "not_ready",
        "services": services,
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        content=payload,
    )
