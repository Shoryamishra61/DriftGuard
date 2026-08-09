"""Credential-pair readiness check for the server-side dashboard proxy."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from app_api.auth import ProjectDependency
from app_api.security import require_admin_token

router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])


@router.get("/session", status_code=status.HTTP_204_NO_CONTENT)
async def validate_dashboard_session(
    project: ProjectDependency,
    admin_access: Annotated[None, Depends(require_admin_token)],
) -> Response:
    del project, admin_access
    return Response(status_code=status.HTTP_204_NO_CONTENT)
