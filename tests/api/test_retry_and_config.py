from __future__ import annotations

import pytest
from pydantic import ValidationError

from app_api.config import Settings
from common_utils.retry import retry_async


@pytest.mark.asyncio
async def test_startup_retry_uses_all_five_canonical_delays() -> None:
    calls = 0
    sleeps: list[float] = []

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls <= 5:
            raise ConnectionError("transient")
        return "ready"

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)

    result = await retry_async(
        operation,
        operation_name="dependency",
        sleeper=sleeper,
    )

    assert result == "ready"
    assert calls == 6
    assert sleeps == [2.0, 4.0, 8.0, 16.0, 32.0]


@pytest.mark.asyncio
async def test_startup_retry_reraises_after_final_attempt() -> None:
    calls = 0
    sleeps: list[float] = []

    async def operation() -> None:
        nonlocal calls
        calls += 1
        raise OSError("still unavailable")

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)

    with pytest.raises(OSError):
        await retry_async(
            operation,
            operation_name="dependency",
            sleeper=sleeper,
        )

    assert calls == 6
    assert sleeps == [2.0, 4.0, 8.0, 16.0, 32.0]


def test_manifest_environment_aliases_drive_runtime_configuration(monkeypatch) -> None:
    monkeypatch.setenv("DRIFT_QUEUE_NAME", "custom_queue")
    monkeypatch.setenv("QDRANT_COLLECTION", "custom_collection")
    monkeypatch.setenv("STARTUP_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("STARTUP_BACKOFF_SECONDS", "2")

    settings = Settings()

    assert settings.queue_name == "custom_queue"
    assert settings.qdrant_collection == "custom_collection"
    assert settings.startup_backoff_schedule == (2.0, 4.0, 8.0, 16.0, 32.0)


def test_internal_qdrant_url_rejects_https() -> None:
    with pytest.raises(ValidationError):
        Settings(qdrant_url="https://qdrant:6333")


def test_database_url_is_normalized_to_asyncpg_without_exposing_password() -> None:
    settings = Settings(database_url="postgresql://user:secret@db:5432/driftguard")
    assert settings.database_dsn.drivername == "postgresql+asyncpg"
    assert "secret" not in str(settings.database_dsn)


def test_webhook_allowlist_uses_worker_compatible_csv_alias(monkeypatch) -> None:
    monkeypatch.setenv(
        "WEBHOOK_ALLOWED_HOSTS",
        " Hooks.Example.com.,api.example.com,hooks.example.com ",
    )

    settings = Settings()

    assert settings.webhook_allowed_hosts == (
        "hooks.example.com",
        "api.example.com",
    )


@pytest.mark.parametrize(
    "hosts",
    [
        "http://example.com",
        "localhost",
        "127.0.0.1",
        "224.0.0.1",
        "ff02::1",
        "*.example.com",
    ],
)
def test_webhook_allowlist_rejects_non_bare_or_private_hosts(hosts: str) -> None:
    with pytest.raises(ValidationError):
        Settings(webhook_allowed_hosts_csv=hosts)
