"""API-key authentication and tenant resolution."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app_api.database import get_session
from app_api.db_schema import projects

logger = logging.getLogger("driftguard.auth")
MAX_API_KEY_BYTES = 512
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


@dataclass(frozen=True, slots=True)
class AuthenticatedProject:
    id: UUID


def hash_api_key(raw_api_key: str) -> str:
    return hashlib.sha256(raw_api_key.encode("utf-8")).hexdigest()


def api_key_hash_from_header(
    x_api_key: Annotated[str | None, Security(api_key_header)] = None,
) -> str:
    if not x_api_key or len(x_api_key.encode("utf-8")) > MAX_API_KEY_BYTES:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return hash_api_key(x_api_key)


async def resolve_project(
    api_key_hash: Annotated[str, Depends(api_key_hash_from_header)],
    session: Annotated[AsyncSession, Depends(get_session, use_cache=False)],
) -> AuthenticatedProject:

    try:
        result = await session.execute(
            select(projects.c.id).where(projects.c.api_key_hash == api_key_hash)
        )
        project_id = result.scalar_one_or_none()
        # The read-only authentication transaction is intentionally ended
        # before rate limiting or endpoint work so it never holds a pool slot
        # across network I/O. Mutating paths revalidate tenant ownership in
        # their own write transaction.
        await session.rollback()
    except SQLAlchemyError as exc:
        await session.rollback()
        logger.error("project authentication query failed (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="authentication service unavailable",
        ) from None

    if project_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return AuthenticatedProject(id=project_id)


ProjectDependency = Annotated[AuthenticatedProject, Depends(resolve_project)]
ApiKeyHashDependency = Annotated[str, Depends(api_key_hash_from_header)]
