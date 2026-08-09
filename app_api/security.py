"""Administrative authorization for raw telemetry and routing controls."""

from __future__ import annotations

import hashlib
import secrets
from typing import Annotated

from fastapi import Header, HTTPException, Request, status

ADMIN_HEADER_NAME = "X-DriftGuard-Admin-Token"


def require_admin_token(
    request: Request,
    provided_token: Annotated[str | None, Header(alias=ADMIN_HEADER_NAME)] = None,
) -> None:
    runtime = getattr(request.app.state, "runtime", None)
    settings = runtime.settings if runtime is not None else request.app.state.settings
    configured = settings.admin_token
    if configured is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="administrative access is not configured",
        )

    candidate_digest = hashlib.sha256((provided_token or "").encode("utf-8")).digest()
    expected_digest = hashlib.sha256(
        configured.get_secret_value().encode("utf-8")
    ).digest()
    if not secrets.compare_digest(candidate_digest, expected_digest):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="invalid admin token",
        )
