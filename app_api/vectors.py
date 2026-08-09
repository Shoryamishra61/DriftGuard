"""Tenant-isolated two-dimensional views of Qdrant embeddings."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app_api.auth import ProjectDependency
from app_api.qdrant import (
    VectorProjectionReader,
    VectorProjectionUnavailable,
    get_vector_projection_reader,
)
from app_api.schemas import VectorProjectionPoint, VectorProjectionResponse
from app_api.security import require_admin_token

router = APIRouter(
    prefix="/api/v1/vectors",
    tags=["dashboard"],
    dependencies=[Depends(require_admin_token)],
)


@router.get("/projection", response_model=VectorProjectionResponse)
async def get_vector_projection(
    project: ProjectDependency,
    reader: Annotated[
        VectorProjectionReader,
        Depends(get_vector_projection_reader),
    ],
    limit: Annotated[int, Query(ge=1, le=500)] = 250,
) -> VectorProjectionResponse:
    try:
        page = await reader.fetch(project.id, limit)
    except VectorProjectionUnavailable:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="vector projection temporarily unavailable",
        ) from None

    points = [
        VectorProjectionPoint.model_validate(point, from_attributes=True)
        for point in page.points
    ]
    return VectorProjectionResponse(
        points=points,
        count=len(points),
        limit=limit,
        has_more=page.has_more,
    )
