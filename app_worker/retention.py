"""Bounded, legal-hold-aware retention across PostgreSQL and Qdrant."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import WorkerConfig

UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017


@dataclass(frozen=True, slots=True)
class RetentionResult:
    redacted_runs: int = 0
    purged_outbox_events: int = 0
    expired_runs: int = 0
    deleted_vectors: int = 0
    purged_vector_receipts: int = 0
    lock_acquired: bool = False


class RetentionManager:
    """Apply data minimization in bounded batches under one cluster-wide lock."""

    def __init__(
        self,
        *,
        config: WorkerConfig,
        repository: Any,
        vector_store: Any,
        utc_now: Any | None = None,
    ) -> None:
        self._config = config
        self._repository = repository
        self._vector_store = vector_store
        self._utc_now = utc_now or (lambda: datetime.now(UTC))

    async def run_once(self) -> RetentionResult:
        now = self._utc_now()
        raw_cutoff = now - timedelta(days=self._config.retention_raw_text_days)
        telemetry_cutoff = now - timedelta(days=self._config.retention_telemetry_days)
        outbox_cutoff = now - timedelta(days=self._config.retention_outbox_days)
        totals = {
            "redacted_runs": 0,
            "purged_outbox_events": 0,
            "expired_runs": 0,
            "deleted_vectors": 0,
            "purged_vector_receipts": 0,
        }

        async with self._repository.retention_lock() as connection:
            if connection is None:
                return RetentionResult()

            totals["redacted_runs"] = await self._drain_bounded(
                lambda: self._repository.redact_expired_telemetry(
                    connection,
                    cutoff=raw_cutoff,
                    batch_size=self._config.retention_batch_size,
                )
            )
            totals["purged_outbox_events"] = await self._drain_bounded(
                lambda: self._repository.purge_dispatched_outbox(
                    connection,
                    cutoff=outbox_cutoff,
                    batch_size=self._config.retention_batch_size,
                )
            )
            totals["expired_runs"] = await self._drain_bounded(
                lambda: self._repository.expire_telemetry_runs(
                    connection,
                    cutoff=telemetry_cutoff,
                    batch_size=self._config.retention_batch_size,
                )
            )

            for _batch in range(self._config.retention_max_batches):
                run_ids = await self._repository.pending_vector_deletions(
                    connection,
                    batch_size=self._config.retention_batch_size,
                )
                if not run_ids:
                    break
                try:
                    await self._vector_store.delete_evaluations(run_ids)
                except Exception as exc:
                    await self._repository.fail_vector_deletions(
                        connection,
                        run_ids,
                        type(exc).__name__,
                    )
                    raise
                await self._repository.complete_vector_deletions(connection, run_ids)
                totals["deleted_vectors"] += len(run_ids)
                if len(run_ids) < self._config.retention_batch_size:
                    break

            totals["purged_vector_receipts"] = await self._drain_bounded(
                lambda: self._repository.purge_completed_vector_deletions(
                    connection,
                    cutoff=outbox_cutoff,
                    batch_size=self._config.retention_batch_size,
                )
            )

        return RetentionResult(**totals, lock_acquired=True)

    async def _drain_bounded(self, operation: Any) -> int:
        total = 0
        for _batch in range(self._config.retention_max_batches):
            affected = int(await operation())
            total += affected
            if affected < self._config.retention_batch_size:
                break
        return total
