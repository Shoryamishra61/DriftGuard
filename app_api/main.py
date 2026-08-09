"""DriftGuard FastAPI application factory and production lifecycle."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app_api import __version__
from app_api.analytics import router as analytics_router
from app_api.body_limit import RequestBodyLimitMiddleware
from app_api.config import ConfigurationError, Settings
from app_api.dashboard_session import router as dashboard_session_router
from app_api.database import (
    create_engine,
    create_session_factory,
    ping_database,
)
from app_api.diagnostics import router as diagnostics_router
from app_api.health import router as health_router
from app_api.ingest import router as ingest_router
from app_api.outbox import OutboxDispatcher
from app_api.qdrant import ping_qdrant, qdrant_auth_headers
from app_api.runtime import RuntimeResources
from app_api.valkey import create_valkey_client, ping_valkey
from app_api.vectors import router as vectors_router
from common_utils.retry import retry_async

logger = logging.getLogger("driftguard.api")


def _lifespan(settings_override: Settings | None):
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        settings = settings_override or Settings()
        if settings.admin_token is None:
            raise ConfigurationError("DRIFTGUARD_ADMIN_TOKEN is required")
        # Missing vector-store credentials are configuration errors rather
        # than transient outages and must fail before allocating clients.
        qdrant_auth_headers(settings)
        engine = create_engine(settings)
        session_factory = create_session_factory(engine)
        valkey = create_valkey_client(settings)
        http_client = httpx.AsyncClient(
            timeout=settings.dependency_timeout_seconds,
            trust_env=False,
        )
        dispatcher = OutboxDispatcher(session_factory, valkey, settings)
        runtime = RuntimeResources(
            settings=settings,
            engine=engine,
            session_factory=session_factory,
            valkey=valkey,
            http_client=http_client,
            dispatcher=dispatcher,
        )
        app.state.runtime = runtime
        poller_task: asyncio.Task[None] | None = None

        try:
            await asyncio.gather(
                retry_async(
                    lambda: ping_database(engine),
                    operation_name="PostgreSQL",
                    backoff_seconds=settings.startup_backoff_schedule,
                ),
                retry_async(
                    lambda: ping_valkey(valkey),
                    operation_name="Valkey",
                    backoff_seconds=settings.startup_backoff_schedule,
                ),
                retry_async(
                    lambda: ping_qdrant(http_client, settings),
                    operation_name="Qdrant",
                    backoff_seconds=settings.startup_backoff_schedule,
                ),
            )
            runtime.ready = True
            poller_task = asyncio.create_task(
                dispatcher.run(),
                name="driftguard-outbox-poller",
            )
            yield
        finally:
            runtime.ready = False
            dispatcher.stop()
            if poller_task is not None:
                try:
                    await asyncio.wait_for(poller_task, timeout=5)
                except TimeoutError:
                    poller_task.cancel()
                    await asyncio.gather(poller_task, return_exceptions=True)
            await http_client.aclose()
            await valkey.aclose()
            await engine.dispose()

    return lifespan


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or Settings()
    application = FastAPI(
        title="DriftGuard API",
        version=__version__,
        lifespan=_lifespan(resolved_settings),
    )
    application.state.settings = resolved_settings
    application.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=resolved_settings.max_request_bytes,
    )

    @application.exception_handler(RequestValidationError)
    async def sanitized_validation_error(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        del request
        errors = [
            {
                "field": ".".join(str(part) for part in error.get("loc", ())),
                "code": error.get("type", "validation_error"),
            }
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={"detail": "request validation failed", "errors": errors},
        )

    application.include_router(health_router)
    application.include_router(ingest_router)
    application.include_router(analytics_router)
    application.include_router(diagnostics_router)
    application.include_router(dashboard_session_router)
    application.include_router(vectors_router)
    return application


app = create_app()
