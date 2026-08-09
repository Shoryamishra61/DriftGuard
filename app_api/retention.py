"""Project-scoped legal holds for the automated retention lifecycle."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import insert, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app_api.auth import ProjectDependency
from app_api.database import get_session
from app_api.db_schema import legal_holds
from app_api.security import require_admin_token

UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017

router = APIRouter(
    prefix="/api/v1/retention",
    tags=["retention"],
    dependencies=[Depends(require_admin_token)],
)
RETENTION_ADVISORY_LOCK_KEY = 42070


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
