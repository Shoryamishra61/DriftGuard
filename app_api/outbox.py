"""Reliable, idempotent PostgreSQL-outbox to Valkey dispatcher."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import timedelta

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - production runs Python 3.12
    from enum import Enum

    class StrEnum(str, Enum):  # noqa: UP042 -- Python 3.10 test support
        pass
from typing import Any
from uuid import UUID

from sqlalchemy import and_, exists, func, or_, select, update

from app_api.config import Settings
from app_api.database import SessionFactory
from app_api.db_schema import evaluations, telemetry_outbox, telemetry_runs

logger = logging.getLogger("driftguard.outbox")

IDEMPOTENT_LPUSH_SCRIPT = """
local inserted = redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[2])
if inserted then
    redis.call('LPUSH', KEYS[2], ARGV[1])
    return 1
end
return 0
""".strip()


class DispatchOutcome(StrEnum):
    DISPATCHED = "dispatched"
    DEFERRED = "deferred"
    NOT_FOUND = "not_found"


class OutboxDispatcher:
    """Publish outbox records with DB locking and Valkey-side deduplication.

    The Lua operation atomically records an event marker and performs ``LPUSH``.
    If the API crashes after Valkey accepts the task but before PostgreSQL marks
    it dispatched, the next attempt sees the marker and safely completes the DB
    transition without pushing a duplicate queue item.
    """

    def __init__(self, session_factory: SessionFactory, valkey: Any, settings: Settings):
        self._session_factory = session_factory
        self._valkey = valkey
        self._settings = settings
        self._stop_event = asyncio.Event()

    async def _publish_once(
        self,
        event_id: UUID,
        delivery_generation: int,
        payload: dict[str, Any],
    ) -> bool:
        serialized_payload = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        marker_key = (
            f"driftguard:outbox:{event_id}:delivery:{delivery_generation}:published"
        )
        result = await self._valkey.eval(
            IDEMPOTENT_LPUSH_SCRIPT,
            2,
            marker_key,
            self._settings.queue_name,
            serialized_payload,
            self._settings.outbox_dedupe_ttl_seconds,
        )
        return bool(result)

    def _retry_delay(self, completed_attempts: int) -> int:
        exponent = max(0, completed_attempts - 1)
        return min(
            self._settings.outbox_retry_base_seconds * (2**exponent),
            self._settings.outbox_retry_max_seconds,
        )

    def _recoverable_statement(self, event_id: UUID | None = None):
        pending_due = and_(
            telemetry_outbox.c.status == "PENDING",
            telemetry_outbox.c.next_attempt_at <= func.now(),
        )
        stale_dispatch = and_(
            telemetry_outbox.c.status == "DISPATCHED",
            telemetry_outbox.c.dispatch_time.is_not(None),
            telemetry_outbox.c.dispatch_time
            <= func.now()
            - timedelta(seconds=self._settings.outbox_dispatch_lease_seconds),
            telemetry_runs.c.status.in_(("queued", "processing")),
            ~exists(
                select(evaluations.c.id).where(
                    evaluations.c.run_id == telemetry_outbox.c.run_id
                )
            ),
        )
        statement = (
            select(
                telemetry_outbox.c.id,
                telemetry_outbox.c.payload,
                telemetry_outbox.c.retry_count,
                telemetry_outbox.c.status,
            )
            .select_from(
                telemetry_outbox.join(
                    telemetry_runs,
                    telemetry_outbox.c.run_id == telemetry_runs.c.id,
                )
            )
            .where(or_(pending_due, stale_dispatch))
            .order_by(telemetry_outbox.c.next_attempt_at, telemetry_outbox.c.created_at)
            .limit(1)
            .with_for_update(of=telemetry_outbox, skip_locked=True)
        )
        if event_id is not None:
            statement = statement.where(telemetry_outbox.c.id == event_id)
        return statement

    async def _dispatch_locked(self, event_id: UUID | None) -> DispatchOutcome:
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(self._recoverable_statement(event_id))
                row = result.mappings().first()
                if row is None:
                    return DispatchOutcome.NOT_FOUND

                claimed_event_id = row["id"]
                is_stale_redelivery = row["status"] == "DISPATCHED"
                delivery_generation = int(row["retry_count"]) + int(is_stale_redelivery)
                try:
                    await asyncio.wait_for(
                        self._publish_once(
                            claimed_event_id,
                            delivery_generation,
                            row["payload"],
                        ),
                        timeout=self._settings.dependency_timeout_seconds,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    completed_attempts = delivery_generation + 1
                    is_terminal = completed_attempts >= self._settings.outbox_max_attempts
                    await session.execute(
                        update(telemetry_outbox)
                        .where(telemetry_outbox.c.id == claimed_event_id)
                        .values(
                            status="FAILED" if is_terminal else "PENDING",
                            retry_count=completed_attempts,
                            next_attempt_at=func.now()
                            + timedelta(seconds=self._retry_delay(completed_attempts)),
                            last_error=f"queue_publish:{type(exc).__name__}",
                        )
                    )
                    logger.warning(
                        "outbox event %s dispatch deferred after attempt %d (%s)",
                        claimed_event_id,
                        completed_attempts,
                        type(exc).__name__,
                    )
                    return DispatchOutcome.DEFERRED

                await session.execute(
                    update(telemetry_outbox)
                    .where(telemetry_outbox.c.id == claimed_event_id)
                    .values(
                        status="DISPATCHED",
                        retry_count=delivery_generation,
                        dispatch_time=func.now(),
                        last_error=None,
                    )
                )
                return DispatchOutcome.DISPATCHED

    async def dispatch_event(self, event_id: UUID) -> DispatchOutcome:
        """Best-effort immediate dispatch used after an ingest commit."""

        try:
            return await self._dispatch_locked(event_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "immediate outbox dispatch failed for event %s (%s)",
                event_id,
                type(exc).__name__,
            )
            return DispatchOutcome.DEFERRED

    async def dispatch_pending_once(self) -> DispatchOutcome:
        return await self._dispatch_locked(None)

    async def run(self) -> None:
        """Recover due PENDING and stale, unevaluated DISPATCHED events."""

        while not self._stop_event.is_set():
            processed = 0
            try:
                for _ in range(self._settings.outbox_batch_size):
                    outcome = await self.dispatch_pending_once()
                    if outcome is DispatchOutcome.NOT_FOUND:
                        break
                    processed += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("outbox polling cycle failed (%s)", type(exc).__name__)

            if processed == 0:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._settings.outbox_poll_interval_seconds,
                    )
                except TimeoutError:
                    pass

    def stop(self) -> None:
        self._stop_event.set()
