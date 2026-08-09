"""Bounded asynchronous retries for cold-start dependency connections."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

T = TypeVar("T")
DEFAULT_BACKOFF_SECONDS: tuple[float, ...] = (2.0, 4.0, 8.0, 16.0, 32.0)


async def retry_async(  # noqa: UP047 -- Python 3.10 test support
    operation: Callable[[], Awaitable[T]],
    *,
    operation_name: str,
    backoff_seconds: Sequence[float] = DEFAULT_BACKOFF_SECONDS,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    logger: logging.Logger | None = None,
) -> T:
    """Run immediately, then retry once after each configured delay.

    Error messages are deliberately omitted from logs because connection-driver
    exceptions can contain credential-bearing DSNs. Cancellation is never
    swallowed, even when ``Exception`` is configured as retryable.
    """

    if not operation_name.strip():
        raise ValueError("operation_name must not be empty")
    if any(delay < 0 for delay in backoff_seconds):
        raise ValueError("backoff delays must be non-negative")

    log = logger or logging.getLogger("driftguard.retry")
    total_attempts = len(backoff_seconds) + 1

    for attempt_number in range(1, total_attempts + 1):
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except retry_exceptions as exc:
            if attempt_number == total_attempts:
                log.error(
                    "%s unavailable after %d attempts (%s)",
                    operation_name,
                    total_attempts,
                    type(exc).__name__,
                )
                raise

            delay = float(backoff_seconds[attempt_number - 1])
            log.warning(
                "%s unavailable; retry %d/%d in %.0fs (%s)",
                operation_name,
                attempt_number,
                len(backoff_seconds),
                delay,
                type(exc).__name__,
            )
            await sleeper(delay)

    raise RuntimeError("retry loop exited without returning or raising")
