"""Application runtime resources and lifecycle-safe dependency probes."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from app_api.config import Settings
from app_api.database import SessionFactory


@dataclass(slots=True)
class RuntimeResources:
    settings: Settings
    engine: AsyncEngine
    session_factory: SessionFactory
    valkey: Redis
    http_client: httpx.AsyncClient
    dispatcher: object
    ready: bool = False

