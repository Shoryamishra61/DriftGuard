"""Concurrency-safe async circuit breaker for Qdrant runtime operations."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """Raised without touching Qdrant while the recovery window is closed."""


@dataclass(frozen=True, slots=True)
class _Admission:
    epoch: int
    half_open_probe: bool


class AsyncCircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int,
        reset_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 1 <= failure_threshold <= 100:
            raise ValueError("circuit failure threshold must be between 1 and 100")
        if not 0.1 <= reset_seconds <= 3600:
            raise ValueError("circuit reset must be between 0.1 and 3600 seconds")
        self.failure_threshold = failure_threshold
        self.reset_seconds = reset_seconds
        self._monotonic = monotonic
        self._lock = asyncio.Lock()
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._half_open_probe_active = False
        self._epoch = 0

    @property
    def state(self) -> CircuitState:
        return self._state

    async def call(self, operation: Callable[[], Awaitable[T]]) -> T:
        admission = await self._before_call()
        try:
            result = await operation()
        except asyncio.CancelledError:
            await self._cancel_call(admission)
            raise
        except Exception as exc:
            opened = await self._record_failure(admission)
            if opened:
                raise CircuitOpenError("Qdrant circuit opened after a failure") from exc
            raise
        await self._record_success(admission)
        return result

    async def _before_call(self) -> _Admission:
        async with self._lock:
            if self._state is CircuitState.OPEN:
                elapsed = self._monotonic() - self._opened_at
                if elapsed < self.reset_seconds:
                    raise CircuitOpenError("Qdrant circuit is open")
                self._state = CircuitState.HALF_OPEN
                self._half_open_probe_active = False
                self._epoch += 1

            if self._state is CircuitState.HALF_OPEN:
                if self._half_open_probe_active:
                    raise CircuitOpenError("Qdrant half-open probe is already running")
                self._half_open_probe_active = True
                return _Admission(self._epoch, True)
            return _Admission(self._epoch, False)

    async def _record_success(self, admission: _Admission) -> None:
        async with self._lock:
            if admission.epoch != self._epoch:
                return
            if admission.half_open_probe and self._state is CircuitState.HALF_OPEN:
                self._state = CircuitState.CLOSED
                self._half_open_probe_active = False
                self._epoch += 1
                self._consecutive_failures = 0
            elif not admission.half_open_probe and self._state is CircuitState.CLOSED:
                self._consecutive_failures = 0

    async def _record_failure(self, admission: _Admission) -> bool:
        async with self._lock:
            if admission.epoch != self._epoch:
                return False
            if admission.half_open_probe and self._state is CircuitState.HALF_OPEN:
                self._trip()
                return True
            if self._state is not CircuitState.CLOSED:
                return False
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.failure_threshold:
                self._trip()
                return True
            return False

    async def _cancel_call(self, admission: _Admission) -> None:
        if not admission.half_open_probe:
            return
        async with self._lock:
            if admission.epoch != self._epoch:
                return
            self._state = CircuitState.OPEN
            self._opened_at = self._monotonic()
            self._half_open_probe_active = False
            self._epoch += 1

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._monotonic()
        self._half_open_probe_active = False
        self._epoch += 1
