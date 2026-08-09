"""Real dependency diagnostics for the Infrastructure Pulse dashboard."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request
from sqlalchemy import text

from app_api.auth import ProjectDependency
from app_api.runtime import RuntimeResources
from app_api.schemas import PulseResponse, ServiceStatus
from app_api.security import require_admin_token

router = APIRouter(
    prefix="/api/v1/diagnostics",
    tags=["diagnostics"],
    dependencies=[Depends(require_admin_token)],
)


def _latency_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 2)


class InfrastructureProbe:
    def __init__(self, runtime: RuntimeResources):
        self._runtime = runtime

    async def _postgres(self) -> dict[str, Any]:
        started_at = time.perf_counter()
        try:
            async def execute_probe() -> None:
                async with self._runtime.engine.connect() as connection:
                    await connection.execute(text("SELECT 1"))

            await asyncio.wait_for(
                execute_probe(),
                timeout=self._runtime.settings.dependency_timeout_seconds,
            )
            pool = self._runtime.engine.sync_engine.pool
            return {
                "status": ServiceStatus.HEALTHY.value,
                "latency_ms": _latency_ms(started_at),
                "pool": {
                    "size": int(pool.size()),
                    "checked_in": int(pool.checkedin()),
                    "checked_out": int(pool.checkedout()),
                    "overflow": int(pool.overflow()),
                },
            }
        except asyncio.CancelledError:
            raise
        except Exception:
            return {
                "status": ServiceStatus.DEGRADED.value,
                "latency_ms": _latency_ms(started_at),
            }

    async def _valkey(self) -> dict[str, Any]:
        started_at = time.perf_counter()
        try:
            queue_depth = await asyncio.wait_for(
                self._runtime.valkey.llen(self._runtime.settings.queue_name),
                timeout=self._runtime.settings.dependency_timeout_seconds,
            )
            return {
                "status": ServiceStatus.HEALTHY.value,
                "latency_ms": _latency_ms(started_at),
                "queue_depth": int(queue_depth),
            }
        except asyncio.CancelledError:
            raise
        except Exception:
            return {
                "status": ServiceStatus.DEGRADED.value,
                "latency_ms": _latency_ms(started_at),
            }

    async def _qdrant(self) -> dict[str, Any]:
        settings = self._runtime.settings
        base_url = settings.qdrant_base_url
        if base_url is None or settings.qdrant_api_key is None:
            return {
                "status": ServiceStatus.UNAVAILABLE.value,
                "configured": False,
            }

        started_at = time.perf_counter()
        try:
            response = await self._runtime.http_client.get(
                f"{base_url}/collections/{settings.qdrant_collection}",
                headers={"api-key": settings.qdrant_api_key.get_secret_value()},
            )
            response.raise_for_status()
            body = response.json()
            result = body.get("result") if isinstance(body, dict) else None
            vectors = result.get("points_count") if isinstance(result, dict) else None
            return {
                "status": ServiceStatus.HEALTHY.value,
                "configured": True,
                "latency_ms": _latency_ms(started_at),
                "vector_count": int(vectors) if isinstance(vectors, int) else None,
            }
        except asyncio.CancelledError:
            raise
        except Exception:
            return {
                "status": ServiceStatus.DEGRADED.value,
                "configured": True,
                "latency_ms": _latency_ms(started_at),
            }

    async def _worker(self) -> dict[str, Any]:
        started_at = time.perf_counter()
        try:
            encoded = await asyncio.wait_for(
                self._runtime.valkey.get("driftguard:worker:heartbeat"),
                timeout=self._runtime.settings.dependency_timeout_seconds,
            )
            if not encoded:
                return {
                    "status": ServiceStatus.DEGRADED.value,
                    "latency_ms": _latency_ms(started_at),
                    "active": False,
                }
            heartbeat = json.loads(encoded)
            if not isinstance(heartbeat, dict):
                raise ValueError("worker heartbeat is not an object")
            return {
                "status": ServiceStatus.HEALTHY.value,
                "latency_ms": _latency_ms(started_at),
                "active": True,
                "worker_id": heartbeat.get("worker_id"),
                "worker_status": heartbeat.get("status"),
                "active_threads": heartbeat.get("active_threads"),
                "current_run_id": heartbeat.get("current_run_id"),
                "last_completed_at": heartbeat.get("last_completed_at"),
                "timestamp": heartbeat.get("timestamp"),
            }
        except asyncio.CancelledError:
            raise
        except Exception:
            return {
                "status": ServiceStatus.DEGRADED.value,
                "latency_ms": _latency_ms(started_at),
                "active": False,
            }

    async def snapshot(self) -> PulseResponse:
        postgres, valkey, qdrant, worker = await asyncio.gather(
            self._postgres(),
            self._valkey(),
            self._qdrant(),
            self._worker(),
        )
        services = {
            "postgres": postgres,
            "valkey": valkey,
            "qdrant": qdrant,
            "worker": worker,
        }
        overall = (
            ServiceStatus.HEALTHY
            if all(item["status"] == ServiceStatus.HEALTHY.value for item in services.values())
            else ServiceStatus.DEGRADED
        )
        return PulseResponse(
            timestamp=datetime.now(timezone.utc),  # noqa: UP017 -- Python 3.10 test support
            status=overall,
            services=services,
        )


def get_infrastructure_probe(request: Request) -> InfrastructureProbe:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise RuntimeError("application runtime is not initialized")
    return InfrastructureProbe(runtime)


@router.get("/pulse", response_model=PulseResponse)
@router.get("/health", response_model=PulseResponse, include_in_schema=False)
async def infrastructure_pulse(
    project: ProjectDependency,
    probe: Annotated[InfrastructureProbe, Depends(get_infrastructure_probe)],
) -> PulseResponse:
    del project
    return await probe.snapshot()
