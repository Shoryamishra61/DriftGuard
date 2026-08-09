"""Atomic, tenant-scoped Valkey rate limiting for telemetry ingestion."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from uuid import UUID

from fastapi import Request

from app_api.config import Settings

logger = logging.getLogger("driftguard.rate_limit")

RATE_LIMIT_SCRIPT = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('TTL', KEYS[1])
if ttl < 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
    ttl = tonumber(ARGV[1])
end
return {current, ttl}
""".strip()


@dataclass(frozen=True, slots=True)
class RateLimitExceeded(Exception):
    retry_after_seconds: int


class RateLimiterUnavailable(RuntimeError):
    pass


class ProjectRateLimiter:
    def __init__(self, valkey, settings: Settings):
        self._valkey = valkey
        self._settings = settings

    async def check(self, project_id: UUID) -> None:
        """Increment one trusted-project counter atomically in Valkey."""

        counter_key = f"driftguard:rate-limit:ingest:{project_id}"
        try:
            result = await asyncio.wait_for(
                self._valkey.eval(
                    RATE_LIMIT_SCRIPT,
                    1,
                    counter_key,
                    self._settings.ingest_rate_limit_window_seconds,
                ),
                timeout=self._settings.dependency_timeout_seconds,
            )
            current, ttl = int(result[0]), int(result[1])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("rate limiter unavailable (%s)", type(exc).__name__)
            if self._settings.rate_limit_fail_open:
                return
            raise RateLimiterUnavailable from None

        if current > self._settings.ingest_rate_limit_requests:
            raise RateLimitExceeded(retry_after_seconds=max(1, ttl))


def get_rate_limiter(request: Request) -> ProjectRateLimiter:
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:
        raise RuntimeError("application runtime is not initialized")
    return ProjectRateLimiter(runtime.valkey, runtime.settings)
