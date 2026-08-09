"""Bounded startup retry support for private Zerops dependencies."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

LOGGER = logging.getLogger("driftguard.worker.startup")
STARTUP_RETRY_DELAYS: tuple[float, ...] = (2.0, 4.0, 8.0, 16.0, 32.0)
T = TypeVar("T")


class StartupDependencyError(RuntimeError):
    """Raised after a dependency fails its initial attempt and five retries."""


async def retry_startup(  # noqa: UP047 - Zerops local verification supports Python 3.10
    dependency_name: str,
    operation: Callable[[], Awaitable[T]],
    *,
    delays: Sequence[float] = STARTUP_RETRY_DELAYS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run ``operation`` immediately, then retry after every supplied delay."""

    for retry_index in range(len(delays) + 1):
        try:
            result = await operation()
            LOGGER.info(
                "%s connection ready after %d attempt(s)",
                dependency_name,
                retry_index + 1,
            )
            return result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if retry_index == len(delays):
                raise StartupDependencyError(
                    f"{dependency_name} unavailable after {retry_index + 1} attempts"
                ) from exc
            delay = float(delays[retry_index])
            LOGGER.warning(
                "%s connection attempt %d failed (%s); retrying in %.0fs",
                dependency_name,
                retry_index + 1,
                type(exc).__name__,
                delay,
            )
            await sleep(delay)

    raise AssertionError("startup retry loop exhausted unexpectedly")
