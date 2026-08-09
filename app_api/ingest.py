"""Authenticated telemetry ingestion with an ACID outbox boundary."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app_api.auth import ApiKeyHashDependency, ProjectDependency
from app_api.database import get_session
from app_api.db_schema import projects, telemetry_outbox, telemetry_runs
from app_api.outbox import OutboxDispatcher
from app_api.rate_limit import (
    ProjectRateLimiter,
    RateLimiterUnavailable,
    RateLimitExceeded,
    get_rate_limiter,
)
from app_api.schemas import IngestResponse, TelemetryPayload

logger = logging.getLogger("driftguard.ingest")
router = APIRouter(prefix="/api/v1", tags=["ingestion"])
EVENT_TYPE = "TELEMETRY_INGESTED"


@dataclass(frozen=True, slots=True)
class PersistedTelemetry:
    run_id: UUID
    outbox_id: UUID


class InvalidApiKeyError(LookupError):
    pass


async def persist_telemetry(
    session: AsyncSession,
    *,
    project_id: UUID,
    api_key_hash: str,
    payload: TelemetryPayload,
) -> PersistedTelemetry:
    """Insert a run and its queue event in one database transaction."""

    run_id = uuid4()
    outbox_id = uuid4()
    async with session.begin():
        project_result = await session.execute(
            select(projects.c.id).where(
                projects.c.id == project_id,
                projects.c.api_key_hash == api_key_hash,
            )
        )
        authenticated_project_id = project_result.scalar_one_or_none()
        if authenticated_project_id is None:
            raise InvalidApiKeyError("API key did not resolve to a project")

        queue_payload = {
            "event_id": str(outbox_id),
            "run_id": str(run_id),
        }
        await session.execute(
            insert(telemetry_runs).values(
                id=run_id,
                project_id=project_id,
                session_id=payload.session_id,
                prompt_text=payload.prompt_text,
                output_text=payload.output_text,
                raw_metadata=payload.metadata,
                status="queued",
            )
        )
        await session.execute(
            insert(telemetry_outbox).values(
                id=outbox_id,
                run_id=run_id,
                event_type=EVENT_TYPE,
                payload=queue_payload,
                status="PENDING",
                retry_count=0,
            )
        )

    return PersistedTelemetry(run_id=run_id, outbox_id=outbox_id)


def get_dispatcher(request: Request) -> OutboxDispatcher:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise RuntimeError("application runtime is not initialized")
    return runtime.dispatcher


@router.post(
    "/logs",
    response_model=IngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_telemetry(
    payload: TelemetryPayload,
    background_tasks: BackgroundTasks,
    api_key_hash: ApiKeyHashDependency,
    project: ProjectDependency,
    session: Annotated[AsyncSession, Depends(get_session)],
    dispatcher: Annotated[OutboxDispatcher, Depends(get_dispatcher)],
    rate_limiter: Annotated[ProjectRateLimiter, Depends(get_rate_limiter)],
) -> IngestResponse:
    try:
        # Authentication has already resolved a real project, so this cannot
        # create unbounded Valkey keys from attacker-supplied random tokens.
        # It runs before the write transaction to avoid holding a DB pool slot
        # during a transient Valkey timeout.
        await rate_limiter.check(project.id)
    except RateLimitExceeded as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="telemetry rate limit exceeded",
            headers={"Retry-After": str(exc.retry_after_seconds)},
        ) from None
    except RateLimiterUnavailable:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="telemetry admission temporarily unavailable",
        ) from None

    try:
        persisted = await persist_telemetry(
            session,
            project_id=project.id,
            api_key_hash=api_key_hash,
            payload=payload,
        )
    except InvalidApiKeyError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        ) from None
    except SQLAlchemyError as exc:
        logger.error("telemetry transaction failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="telemetry storage temporarily unavailable",
        ) from None

    # Starlette executes this only after the response has been sent. The
    # PENDING database row remains authoritative if the process exits first.
    background_tasks.add_task(dispatcher.dispatch_event, persisted.outbox_id)
    return IngestResponse(run_id=persisted.run_id)
