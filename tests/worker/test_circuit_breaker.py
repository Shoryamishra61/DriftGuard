import asyncio

import pytest

from app_worker.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitOpenError,
    CircuitState,
)


@pytest.mark.asyncio
async def test_circuit_trips_and_fast_fails_without_calling_dependency() -> None:
    now = [100.0]
    breaker = AsyncCircuitBreaker(
        failure_threshold=2,
        reset_seconds=30,
        monotonic=lambda: now[0],
    )
    calls = 0

    async def failing():
        nonlocal calls
        calls += 1
        raise ConnectionError("Qdrant offline")

    with pytest.raises(ConnectionError):
        await breaker.call(failing)
    with pytest.raises(CircuitOpenError):
        await breaker.call(failing)
    with pytest.raises(CircuitOpenError):
        await breaker.call(failing)

    assert calls == 2
    assert breaker.state is CircuitState.OPEN


@pytest.mark.asyncio
async def test_half_open_allows_one_probe_and_closes_after_recovery() -> None:
    now = [100.0]
    breaker = AsyncCircuitBreaker(
        failure_threshold=1,
        reset_seconds=10,
        monotonic=lambda: now[0],
    )

    async def failing():
        raise ConnectionError("offline")

    with pytest.raises(CircuitOpenError):
        await breaker.call(failing)
    now[0] = 111.0
    probe_started = asyncio.Event()
    release_probe = asyncio.Event()

    async def probe():
        probe_started.set()
        await release_probe.wait()
        return "healthy"

    probe_task = asyncio.create_task(breaker.call(probe))
    await probe_started.wait()
    with pytest.raises(CircuitOpenError):
        await breaker.call(probe)
    release_probe.set()

    assert await probe_task == "healthy"
    assert breaker.state is CircuitState.CLOSED
    assert await breaker.call(lambda: asyncio.sleep(0, result="ok")) == "ok"


@pytest.mark.asyncio
async def test_failed_half_open_probe_reopens_for_full_reset_window() -> None:
    now = [0.0]
    breaker = AsyncCircuitBreaker(
        failure_threshold=1,
        reset_seconds=5,
        monotonic=lambda: now[0],
    )

    async def failing():
        raise OSError("still offline")

    with pytest.raises(CircuitOpenError):
        await breaker.call(failing)
    now[0] = 6.0
    with pytest.raises(CircuitOpenError):
        await breaker.call(failing)
    now[0] = 10.0
    with pytest.raises(CircuitOpenError):
        await breaker.call(failing)
    now[0] = 11.1
    assert await breaker.call(lambda: asyncio.sleep(0, result=42)) == 42


@pytest.mark.asyncio
async def test_stale_closed_completion_cannot_close_new_half_open_epoch() -> None:
    now = [0.0]
    breaker = AsyncCircuitBreaker(
        failure_threshold=1,
        reset_seconds=5,
        monotonic=lambda: now[0],
    )
    stale_started = asyncio.Event()
    release_stale = asyncio.Event()

    async def stale_success():
        stale_started.set()
        await release_stale.wait()
        return "stale"

    stale_task = asyncio.create_task(breaker.call(stale_success))
    await stale_started.wait()

    async def trip():
        raise ConnectionError("offline")

    with pytest.raises(CircuitOpenError):
        await breaker.call(trip)
    now[0] = 6.0
    probe_started = asyncio.Event()
    release_probe = asyncio.Event()

    async def recovery_probe():
        probe_started.set()
        await release_probe.wait()
        return "recovered"

    probe_task = asyncio.create_task(breaker.call(recovery_probe))
    await probe_started.wait()
    release_stale.set()

    assert await stale_task == "stale"
    assert breaker.state is CircuitState.HALF_OPEN
    with pytest.raises(CircuitOpenError):
        await breaker.call(recovery_probe)

    release_probe.set()
    assert await probe_task == "recovered"
    assert breaker.state is CircuitState.CLOSED
