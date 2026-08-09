import pytest

from app_worker.retry import StartupDependencyError, retry_startup


@pytest.mark.asyncio
async def test_startup_dependency_uses_all_five_exponential_retry_delays() -> None:
    attempts = 0
    sleeps = []

    async def operation():
        nonlocal attempts
        attempts += 1
        raise ConnectionError("offline")

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    with pytest.raises(StartupDependencyError) as failure:
        await retry_startup("Qdrant", operation, sleep=record_sleep)

    assert attempts == 6
    assert sleeps == [2.0, 4.0, 8.0, 16.0, 32.0]
    assert "Qdrant unavailable" in str(failure.value)


@pytest.mark.asyncio
async def test_startup_retry_returns_after_dependency_recovers() -> None:
    attempts = 0
    sleeps = []

    async def operation():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("warming")
        return "ready"

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    assert await retry_startup("PostgreSQL", operation, sleep=record_sleep) == "ready"
    assert attempts == 3
    assert sleeps == [2.0, 4.0]
