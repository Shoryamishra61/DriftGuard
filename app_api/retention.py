"""Project-scoped legal holds for the automated retention lifecycle."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app_api.auth import ProjectDependency
from app_api.database import get_session
from app_api.db_schema import legal_holds, retention_vector_outbox, telemetry_runs
from app_api.security import require_admin_token

UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017

router = APIRouter(
    prefix="/api/v1/retention",
    tags=["retention"],
    dependencies=[Depends(require_admin_token)],
)
RETENTION_ADVISORY_LOCK_KEY = 42070
LOAD_TEST_SOURCE = "load-acceptance"
LOAD_TEST_CLEANUP_BATCH_SIZE = 10_000


async def _serialize_with_retention(session: AsyncSession) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": RETENTION_ADVISORY_LOCK_KEY},
    )


class LegalHoldCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    starts_at: datetime
    ends_at: datetime | None = None
    reason: str = Field(min_length=3, max_length=500)

    @field_validator("starts_at", "ends_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("legal hold timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_range(self) -> LegalHoldCreate:
        if self.ends_at is not None and self.ends_at < self.starts_at:
            raise ValueError("ends_at must not precede starts_at")
        return self


class LegalHoldItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    starts_at: datetime
    ends_at: datetime | None
    reason: str
    created_at: datetime
    released_at: datetime | None


class LoadTestCleanupResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    removed_runs: int
    removed_queue_items: int
    has_more: bool


@router.post("/legal-holds", response_model=LegalHoldItem, status_code=status.HTTP_201_CREATED)
async def create_legal_hold(
    payload: LegalHoldCreate,
    project: ProjectDependency,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> LegalHoldItem:
    try:
        async with session.begin():
            await _serialize_with_retention(session)
            result = await session.execute(
                insert(legal_holds)
                .values(project_id=project.id, **payload.model_dump())
                .returning(
                    legal_holds.c.id,
                    legal_holds.c.starts_at,
                    legal_holds.c.ends_at,
                    legal_holds.c.reason,
                    legal_holds.c.created_at,
                    legal_holds.c.released_at,
                )
            )
            row = result.mappings().one()
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="retention storage temporarily unavailable",
        ) from None
    return LegalHoldItem.model_validate(dict(row))


@router.get("/legal-holds", response_model=list[LegalHoldItem])
async def list_legal_holds(
    project: ProjectDependency,
    session: Annotated[AsyncSession, Depends(get_session)],
    active_only: Annotated[bool, Query()] = True,
) -> list[LegalHoldItem]:
    statement = (
        select(
            legal_holds.c.id,
            legal_holds.c.starts_at,
            legal_holds.c.ends_at,
            legal_holds.c.reason,
            legal_holds.c.created_at,
            legal_holds.c.released_at,
        )
        .where(legal_holds.c.project_id == project.id)
        .order_by(legal_holds.c.created_at.desc(), legal_holds.c.id)
    )
    if active_only:
        statement = statement.where(legal_holds.c.released_at.is_(None))
    try:
        result = await session.execute(statement)
        rows = result.mappings().all()
        await session.rollback()
    except SQLAlchemyError:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="retention storage temporarily unavailable",
        ) from None
    return [LegalHoldItem.model_validate(dict(row)) for row in rows]


@router.delete("/legal-holds/{hold_id}", status_code=status.HTTP_204_NO_CONTENT)
async def release_legal_hold(
    hold_id: UUID,
    project: ProjectDependency,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> Response:
    try:
        async with session.begin():
            await _serialize_with_retention(session)
            result = await session.execute(
                update(legal_holds)
                .where(
                    legal_holds.c.id == hold_id,
                    legal_holds.c.project_id == project.id,
                    legal_holds.c.released_at.is_(None),
                )
                .values(released_at=datetime.now(UTC))
                .returning(legal_holds.c.id)
            )
            released_id = result.scalar_one_or_none()
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="retention storage temporarily unavailable",
        ) from None
    if released_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="legal hold not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete("/load-test-data", response_model=LoadTestCleanupResponse)
async def cleanup_load_test_data(
    request: Request,
    project: ProjectDependency,
    session: Annotated[AsyncSession, Depends(get_session)],
    confirm_workers_stopped: Annotated[Literal["yes"], Query()],
) -> LoadTestCleanupResponse:
    """Remove only explicitly labeled acceptance traffic in bounded, recoverable batches."""

    del confirm_workers_stopped
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="application runtime is not initialized",
        )
    lock_key = f"driftguard:maintenance:load-cleanup:{project.id}"
    lock_token = str(uuid4())
    if not await runtime.valkey.set(lock_key, lock_token, nx=True, ex=300):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="cleanup already running")

    try:
        async with session.begin():
            await _serialize_with_retention(session)
            result = await session.execute(
                select(telemetry_runs.c.id)
                .where(
                    telemetry_runs.c.project_id == project.id,
                    telemetry_runs.c.raw_metadata["source"].astext == LOAD_TEST_SOURCE,
                )
                .order_by(telemetry_runs.c.ingested_at, telemetry_runs.c.id)
                .limit(LOAD_TEST_CLEANUP_BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )
            run_ids = list(result.scalars())
            if not run_ids:
                return LoadTestCleanupResponse(
                    removed_runs=0,
                    removed_queue_items=0,
                    has_more=False,
                )

            removed_queue_items = int(
                await runtime.valkey.eval(
                    """
                    local selected = {}
                    for index = 1, #ARGV do
                        selected[ARGV[index]] = true
                    end
                    local items = redis.call('LRANGE', KEYS[1], 0, -1)
                    local retained = {}
                    local removed = 0
                    for _, item in ipairs(items) do
                        local ok, payload = pcall(cjson.decode, item)
                        if ok and payload['run_id'] and selected[tostring(payload['run_id'])] then
                            removed = removed + 1
                        else
                            table.insert(retained, item)
                        end
                    end
                    redis.call('DEL', KEYS[1])
                    for first = 1, #retained, 1000 do
                        redis.call(
                            'RPUSH',
                            KEYS[1],
                            unpack(retained, first, math.min(first + 999, #retained))
                        )
                    end
                    return removed
                    """,
                    1,
                    runtime.settings.queue_name,
                    *(str(run_id) for run_id in run_ids),
                )
            )

            await session.execute(
                postgresql_insert(retention_vector_outbox)
                .values([{"run_id": run_id} for run_id in run_ids])
                .on_conflict_do_nothing(index_elements=[retention_vector_outbox.c.run_id])
            )
            await session.execute(delete(telemetry_runs).where(telemetry_runs.c.id.in_(run_ids)))

        return LoadTestCleanupResponse(
            removed_runs=len(run_ids),
            removed_queue_items=removed_queue_items,
            has_more=len(run_ids) == LOAD_TEST_CLEANUP_BATCH_SIZE,
        )
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="load-test cleanup temporarily unavailable",
        ) from None
    finally:
        await runtime.valkey.eval(
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """,
            1,
            lock_key,
            lock_token,
        )
