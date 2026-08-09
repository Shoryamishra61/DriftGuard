from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app_worker.config import WorkerConfig
from app_worker.retention import RetentionManager

UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017


def retention_config() -> WorkerConfig:
    return WorkerConfig(
        database_url="postgresql://worker:secret@db:5432/driftguard",
        valkey_host="cache",
        valkey_port=6379,
        valkey_password="secret",  # noqa: S106
        qdrant_host="qdrant",
        qdrant_port=6333,
        qdrant_api_key="secret",
        retention_enabled=True,
        retention_raw_text_days=30,
        retention_telemetry_days=90,
        retention_outbox_days=7,
        retention_batch_size=100,
        retention_max_batches=3,
    )


class FakeRepository:
    def __init__(self, *, lock=True):
        self.lock = lock
        self.connection = object()
        self.calls = []
        self.pending = [[uuid4(), uuid4()], []]
        self.failed = []
        self.completed = []

    @asynccontextmanager
    async def retention_lock(self):
        yield self.connection if self.lock else None

    async def redact_expired_telemetry(self, connection, **kwargs):
        self.calls.append(("redact", connection, kwargs))
        return 3

    async def purge_dispatched_outbox(self, connection, **kwargs):
        self.calls.append(("outbox", connection, kwargs))
        return 2

    async def expire_telemetry_runs(self, connection, **kwargs):
        self.calls.append(("expire", connection, kwargs))
        return 2

    async def pending_vector_deletions(self, connection, **kwargs):
        self.calls.append(("pending", connection, kwargs))
        return self.pending.pop(0)

    async def complete_vector_deletions(self, connection, run_ids):
        self.completed.extend(run_ids)

    async def fail_vector_deletions(self, connection, run_ids, error):
        self.failed.append((list(run_ids), error))

    async def purge_completed_vector_deletions(self, connection, **kwargs):
        self.calls.append(("receipts", connection, kwargs))
        return 1


class FakeVectorStore:
    def __init__(self, *, failure=None):
        self.failure = failure
        self.deleted = []

    async def delete_evaluations(self, run_ids):
        if self.failure is not None:
            raise self.failure
        self.deleted.extend(run_ids)


@pytest.mark.asyncio
async def test_retention_is_bounded_hold_aware_and_cross_store_durable() -> None:
    repository = FakeRepository()
    vector_store = FakeVectorStore()
    manager = RetentionManager(
        config=retention_config(),
        repository=repository,
        vector_store=vector_store,
        utc_now=lambda: datetime(2026, 8, 9, tzinfo=UTC),
    )

    result = await manager.run_once()

    assert result.lock_acquired is True
    assert result.redacted_runs == 3
    assert result.purged_outbox_events == 2
    assert result.expired_runs == 2
    assert result.deleted_vectors == 2
    assert result.purged_vector_receipts == 1
    assert vector_store.deleted == repository.completed
    assert repository.failed == []
    assert all(call[2]["batch_size"] == 100 for call in repository.calls)


@pytest.mark.asyncio
async def test_retention_noops_when_another_worker_owns_the_lock() -> None:
    repository = FakeRepository(lock=False)
    manager = RetentionManager(
        config=retention_config(),
        repository=repository,
        vector_store=FakeVectorStore(),
    )

    result = await manager.run_once()

    assert result.lock_acquired is False
    assert repository.calls == []


@pytest.mark.asyncio
async def test_qdrant_failure_preserves_pending_deletion_with_backoff() -> None:
    repository = FakeRepository()
    vector_store = FakeVectorStore(failure=TimeoutError("qdrant unavailable"))
    manager = RetentionManager(
        config=retention_config(),
        repository=repository,
        vector_store=vector_store,
    )

    with pytest.raises(TimeoutError, match="qdrant unavailable"):
        await manager.run_once()

    assert len(repository.failed) == 1
    assert repository.failed[0][1] == "TimeoutError"
    assert repository.completed == []
