"""PostgreSQL persistence adapter for worker-owned state transitions."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4, uuid5

import asyncpg

from .domain import (
    ActionType,
    Alert,
    AlertRule,
    AlertStatus,
    DeliveryBatch,
    DeliveryItem,
    DigestDelivery,
    Evaluation,
    RouteStatus,
    TelemetryRun,
)

ALERT_ID_NAMESPACE = UUID("b0ca6a91-45e0-5bca-8a4b-c35fb030ba29")


class PostgresRepository:
    """Implements idempotent evaluation and alert persistence with asyncpg."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @classmethod
    async def connect(cls, database_url: str, *, max_size: int = 5) -> PostgresRepository:
        pool = await asyncpg.create_pool(
            dsn=database_url,
            min_size=1,
            max_size=max_size,
            command_timeout=10.0,
            timeout=5.0,
        )
        repository = cls(pool)
        try:
            await repository.ping()
        except Exception:
            await pool.close()
            raise
        return repository

    async def ping(self) -> None:
        async with self._pool.acquire() as connection:
            schema = await connection.fetchrow(
                """
                SELECT to_regclass('public.telemetry_runs') AS telemetry_runs,
                       to_regclass('public.evaluations') AS evaluations,
                       to_regclass('public.alert_rules') AS alert_rules,
                       to_regclass('public.alerts') AS alerts,
                       to_regclass('public.idx_alerts_delivery_lease_token')
                           AS delivery_lease_index,
                       to_regclass('public.legal_holds') AS legal_holds,
                       to_regclass('public.retention_vector_outbox') AS retention_vector_outbox,
                       to_regclass('public.idx_retention_vector_outbox_pending')
                           AS retention_vector_index,
                       (
                           SELECT COUNT(*) = 7
                           FROM information_schema.columns
                           WHERE table_schema = 'public'
                             AND table_name = 'alerts'
                             AND column_name IN (
                                 'route_status', 'route_due_at',
                                 'delivery_lease_until', 'delivery_attempts',
                                 'delivery_lease_token', 'last_delivery_error',
                                 'notified_at'
                             )
                       ) AS delivery_schema_ready,
                       EXISTS (
                           SELECT 1
                           FROM information_schema.columns
                           WHERE table_schema = 'public'
                             AND table_name = 'projects'
                             AND column_name = 'active_baseline_set'
                       ) AS baseline_schema_ready
                """
            )
            table_names = (
                "telemetry_runs",
                "evaluations",
                "alert_rules",
                "alerts",
                "legal_holds",
                "retention_vector_outbox",
                "retention_vector_index",
            )
            if (
                schema is None
                or any(schema[name] is None for name in table_names)
                or schema["delivery_lease_index"] is None
                or not schema["delivery_schema_ready"]
                or not schema["baseline_schema_ready"]
            ):
                raise RuntimeError("canonical worker tables are not migrated yet")

    async def close(self) -> None:
        await self._pool.close()

    @asynccontextmanager
    async def retention_lock(self) -> AsyncIterator[asyncpg.Connection | None]:
        """Serialize cross-store retention and legal-hold changes across workers."""

        lock_key = 42070
        async with self._pool.acquire() as connection:
            acquired = bool(
                await connection.fetchval(
                    "SELECT pg_try_advisory_lock($1::bigint)",
                    lock_key,
                )
            )
            try:
                yield connection if acquired else None
            finally:
                if acquired:
                    try:
                        unlocked = await connection.fetchval(
                            "SELECT pg_advisory_unlock($1::bigint)",
                            lock_key,
                        )
                        if not unlocked:
                            raise RuntimeError("PostgreSQL retention advisory lock was not owned")
                    except BaseException:
                        connection.terminate()
                        raise

    @staticmethod
    async def redact_expired_telemetry(
        connection: asyncpg.Connection,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        rows = await connection.fetch(
            """
            WITH candidates AS (
                SELECT run.id
                FROM telemetry_runs AS run
                WHERE run.ingested_at < $1
                  AND run.status IN ('completed', 'failed')
                  AND run.prompt_text <> '[retention-redacted]'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM legal_holds AS hold
                      WHERE hold.project_id = run.project_id
                        AND hold.released_at IS NULL
                        AND hold.starts_at <= run.ingested_at
                        AND (hold.ends_at IS NULL OR hold.ends_at >= run.ingested_at)
                  )
                ORDER BY run.ingested_at, run.id
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE telemetry_runs AS run
            SET prompt_text = '[retention-redacted]',
                output_text = '[retention-redacted]',
                raw_metadata = jsonb_build_object(
                    'retention_redacted', TRUE,
                    'redacted_at', NOW()
                )
            FROM candidates
            WHERE run.id = candidates.id
            RETURNING run.id
            """,
            cutoff,
            batch_size,
        )
        return len(rows)

    @staticmethod
    async def purge_dispatched_outbox(
        connection: asyncpg.Connection,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        rows = await connection.fetch(
            """
            WITH candidates AS (
                SELECT event.id
                FROM telemetry_outbox AS event
                JOIN telemetry_runs AS run ON run.id = event.run_id
                WHERE event.status = 'DISPATCHED'
                  AND event.dispatch_time < $1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM legal_holds AS hold
                      WHERE hold.project_id = run.project_id
                        AND hold.released_at IS NULL
                        AND hold.starts_at <= run.ingested_at
                        AND (hold.ends_at IS NULL OR hold.ends_at >= run.ingested_at)
                  )
                ORDER BY event.dispatch_time, event.id
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM telemetry_outbox AS event
            USING candidates
            WHERE event.id = candidates.id
            RETURNING event.id
            """,
            cutoff,
            batch_size,
        )
        return len(rows)

    @staticmethod
    async def expire_telemetry_runs(
        connection: asyncpg.Connection,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        async with connection.transaction():
            rows = await connection.fetch(
                """
                SELECT run.id
                FROM telemetry_runs AS run
                WHERE run.ingested_at < $1
                  AND run.status IN ('completed', 'failed')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM legal_holds AS hold
                      WHERE hold.project_id = run.project_id
                        AND hold.released_at IS NULL
                        AND hold.starts_at <= run.ingested_at
                        AND (hold.ends_at IS NULL OR hold.ends_at >= run.ingested_at)
                  )
                ORDER BY run.ingested_at, run.id
                LIMIT $2
                FOR UPDATE SKIP LOCKED
                """,
                cutoff,
                batch_size,
            )
            run_ids = [row["id"] for row in rows]
            if not run_ids:
                return 0
            await connection.execute(
                """
                INSERT INTO retention_vector_outbox (run_id)
                SELECT candidate_id
                FROM unnest($1::uuid[]) AS candidate(candidate_id)
                ON CONFLICT (run_id) DO NOTHING
                """,
                run_ids,
            )
            await connection.execute(
                "DELETE FROM telemetry_runs WHERE id = ANY($1::uuid[])",
                run_ids,
            )
            return len(run_ids)

    @staticmethod
    async def pending_vector_deletions(
        connection: asyncpg.Connection,
        *,
        batch_size: int,
    ) -> list[UUID]:
        rows = await connection.fetch(
            """
            SELECT run_id
            FROM retention_vector_outbox
            WHERE status = 'PENDING'
              AND next_attempt_at <= NOW()
            ORDER BY next_attempt_at, run_id
            LIMIT $1
            """,
            batch_size,
        )
        return [row["run_id"] for row in rows]

    @staticmethod
    async def complete_vector_deletions(
        connection: asyncpg.Connection,
        run_ids: Sequence[UUID],
    ) -> None:
        if not run_ids:
            return
        await connection.execute(
            """
            UPDATE retention_vector_outbox
            SET status = 'COMPLETED',
                completed_at = NOW(),
                last_error = NULL
            WHERE run_id = ANY($1::uuid[])
              AND status = 'PENDING'
            """,
            list(run_ids),
        )

    @staticmethod
    async def fail_vector_deletions(
        connection: asyncpg.Connection,
        run_ids: Sequence[UUID],
        error: str,
    ) -> None:
        if not run_ids:
            return
        await connection.execute(
            """
            UPDATE retention_vector_outbox
            SET attempts = attempts + 1,
                next_attempt_at = NOW() + (
                    LEAST(3600, POWER(2, LEAST(attempts + 1, 10))::integer)
                    * INTERVAL '1 second'
                ),
                last_error = LEFT($2, 1000)
            WHERE run_id = ANY($1::uuid[])
              AND status = 'PENDING'
            """,
            list(run_ids),
            error,
        )

    @staticmethod
    async def purge_completed_vector_deletions(
        connection: asyncpg.Connection,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        rows = await connection.fetch(
            """
            WITH candidates AS (
                SELECT run_id
                FROM retention_vector_outbox
                WHERE status = 'COMPLETED'
                  AND completed_at < $1
                ORDER BY completed_at, run_id
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM retention_vector_outbox AS item
            USING candidates
            WHERE item.run_id = candidates.run_id
            RETURNING item.run_id
            """,
            cutoff,
            batch_size,
        )
        return len(rows)

    @asynccontextmanager
    async def run_processing_lock(self, run_id: UUID) -> AsyncIterator[bool]:
        """Hold a non-transactional, session-scoped lock for one run evaluation."""

        lock_key = self._run_lock_key(run_id)
        async with self._pool.acquire() as connection:
            acquired = bool(
                await connection.fetchval(
                    "SELECT pg_try_advisory_lock($1::bigint)",
                    lock_key,
                )
            )
            try:
                yield acquired
            finally:
                if acquired:
                    try:
                        unlocked = await connection.fetchval(
                            "SELECT pg_advisory_unlock($1::bigint)",
                            lock_key,
                        )
                        if not unlocked:
                            raise RuntimeError("PostgreSQL run advisory lock was not owned")
                    except BaseException:
                        # Never return a possibly locked session to the pool. Closing the
                        # PostgreSQL session also releases its advisory locks on crashes or
                        # cancellation during cleanup.
                        connection.terminate()
                        raise

    @staticmethod
    def _run_lock_key(run_id: UUID) -> int:
        digest = hashlib.sha256(b"driftguard:run:" + run_id.bytes).digest()
        return int.from_bytes(digest[:8], byteorder="big", signed=True)

    async def claim_run(self, run_id: UUID) -> tuple[TelemetryRun | None, Evaluation | None]:
        """Lock a run briefly, set it processing, and expose an existing result."""

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT tr.id, tr.project_id, tr.prompt_text, tr.output_text,
                           tr.ingested_at, p.active_baseline_set
                    FROM telemetry_runs AS tr
                    JOIN projects AS p ON p.id = tr.project_id
                    WHERE tr.id = $1
                    FOR UPDATE
                    """,
                    run_id,
                )
                if row is None:
                    return None, None

                run = TelemetryRun(
                    id=row["id"],
                    project_id=row["project_id"],
                    output_text=row["output_text"],
                    ingested_at=row["ingested_at"],
                    active_baseline_set=row["active_baseline_set"],
                    prompt_text=row["prompt_text"],
                )
                evaluation_row = await connection.fetchrow(
                    """
                    SELECT id, run_id, drift_distance, matched_baseline_id,
                           evaluation_latency_ms, is_anomaly
                    FROM evaluations
                    WHERE run_id = $1
                    """,
                    run_id,
                )
                if evaluation_row is not None:
                    await connection.execute(
                        "UPDATE telemetry_runs SET status = 'completed' WHERE id = $1",
                        run_id,
                    )
                    return run, self._evaluation(evaluation_row)

                await connection.execute(
                    "UPDATE telemetry_runs SET status = 'processing' WHERE id = $1",
                    run_id,
                )
                return run, None

    async def project_exists(self, project_id: UUID) -> bool:
        """Verify tenant authority before accepting project-scoped baseline data."""

        async with self._pool.acquire() as connection:
            return bool(
                await connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM projects WHERE id = $1)",
                    project_id,
                )
            )

    async def project_id_by_name(self, project_name: str) -> UUID | None:
        """Resolve an exact tenant name for controlled deployment bootstrap."""

        async with self._pool.acquire() as connection:
            return await connection.fetchval(
                "SELECT id FROM projects WHERE name = $1",
                project_name,
            )

    async def active_baseline_set(self, project_id: UUID) -> str | None:
        async with self._pool.acquire() as connection:
            return await connection.fetchval(
                "SELECT active_baseline_set FROM projects WHERE id = $1",
                project_id,
            )

    async def activate_baseline_set(self, project_id: UUID, baseline_set: str) -> None:
        """Atomically switch a project only after its complete vector set is durable."""

        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE projects
                SET active_baseline_set = $2
                WHERE id = $1
                """,
                project_id,
                baseline_set,
            )
        if result != "UPDATE 1":
            raise RuntimeError("project disappeared before baseline activation")

    async def persist_evaluation_and_alerts(
        self,
        *,
        run: TelemetryRun,
        drift_distance: float | None,
        matched_baseline_id: UUID | None,
        evaluation_latency_ms: int,
    ) -> tuple[Evaluation, bool, list[Alert]]:
        """Atomically insert one evaluation, its durable alerts, and completion state."""

        candidate_id = uuid4()
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                rule_rows = await connection.fetch(
                    """
                    SELECT id, project_id, rule_name, threshold, action_type,
                           notification_target
                    FROM alert_rules
                    WHERE project_id = $1 AND is_active = TRUE
                    ORDER BY threshold DESC, id ASC
                    FOR SHARE
                    """,
                    run.project_id,
                )
                current_rules = [self._rule(row) for row in rule_rows]
                is_anomaly = drift_distance is not None and any(
                    drift_distance > rule.threshold for rule in current_rules
                )
                inserted = await connection.fetchrow(
                    """
                    INSERT INTO evaluations (
                        id, run_id, drift_distance, matched_baseline_id,
                        evaluation_latency_ms, is_anomaly
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (run_id) DO NOTHING
                    RETURNING id, run_id, drift_distance, matched_baseline_id,
                              evaluation_latency_ms, is_anomaly
                    """,
                    candidate_id,
                    run.id,
                    drift_distance,
                    matched_baseline_id,
                    evaluation_latency_ms,
                    is_anomaly,
                )
                created = inserted is not None
                evaluation_row = inserted
                if evaluation_row is None:
                    evaluation_row = await connection.fetchrow(
                        """
                        SELECT id, run_id, drift_distance, matched_baseline_id,
                               evaluation_latency_ms, is_anomaly
                        FROM evaluations
                        WHERE run_id = $1
                        FOR UPDATE
                        """,
                        run.id,
                    )
                if evaluation_row is None:
                    raise RuntimeError("evaluation upsert did not return a persisted row")

                evaluation = self._evaluation(evaluation_row)
                alerts: list[Alert] = []
                if created and evaluation.drift_distance is not None:
                    for rule in current_rules:
                        if evaluation.drift_distance <= rule.threshold:
                            continue
                        alerts.append(await self._persist_alert(connection, evaluation, rule))

                await connection.execute(
                    "UPDATE telemetry_runs SET status = 'completed' WHERE id = $1",
                    run.id,
                )
                return evaluation, created, alerts

    async def claim_due_deliveries(
        self,
        *,
        limit: int,
        lease_seconds: int,
    ) -> DeliveryBatch:
        """Atomically lease due outbound routes across horizontally scaled workers."""

        digest_lease_token = uuid4()
        regular_lease_token = uuid4()
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                regular_rows = await connection.fetch(
                    """
                    WITH digest_group AS MATERIALIZED (
                        SELECT a.rule_id,
                               (e.evaluated_at AT TIME ZONE 'UTC')::date AS digest_day
                        FROM alerts AS a
                        JOIN alert_rules AS r ON r.id = a.rule_id
                        JOIN evaluations AS e ON e.id = a.evaluation_id
                        WHERE a.route_status = 'PENDING'
                          AND a.alert_status = 'TRIGGERED'
                          AND a.route_due_at <= NOW()
                          AND r.is_active = TRUE
                          AND r.action_type = 'DIGEST'
                          AND (
                              a.delivery_lease_until IS NULL
                              OR a.delivery_lease_until < NOW()
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM alerts AS leased
                              JOIN evaluations AS leased_evaluation
                                ON leased_evaluation.id = leased.evaluation_id
                              WHERE leased.rule_id = a.rule_id
                                AND leased.route_status = 'PENDING'
                                AND leased.delivery_lease_until >= NOW()
                                AND (
                                    leased_evaluation.evaluated_at
                                    AT TIME ZONE 'UTC'
                                )::date =
                                    (e.evaluated_at AT TIME ZONE 'UTC')::date
                          )
                        GROUP BY a.rule_id,
                                 (e.evaluated_at AT TIME ZONE 'UTC')::date
                        ORDER BY MIN(a.route_due_at) ASC, a.rule_id ASC,
                                 (e.evaluated_at AT TIME ZONE 'UTC')::date ASC
                        LIMIT 1
                    ), locked_digest_group AS MATERIALIZED (
                        SELECT dg.rule_id, dg.digest_day
                        FROM digest_group AS dg
                        WHERE pg_try_advisory_xact_lock(
                            hashtextextended(
                                'driftguard:digest:' || dg.rule_id::text || ':' ||
                                dg.digest_day::text,
                                0
                            )
                        )
                    ), digest_candidates AS (
                        SELECT a.id
                        FROM alerts AS a
                        JOIN alert_rules AS r ON r.id = a.rule_id
                        JOIN evaluations AS e ON e.id = a.evaluation_id
                        JOIN locked_digest_group AS dg
                         ON dg.rule_id = a.rule_id
                         AND dg.digest_day =
                             (e.evaluated_at AT TIME ZONE 'UTC')::date
                        WHERE a.route_status = 'PENDING'
                          AND a.alert_status = 'TRIGGERED'
                          AND a.route_due_at <= NOW()
                          AND r.is_active = TRUE
                          AND r.action_type = 'DIGEST'
                          AND (
                              a.delivery_lease_until IS NULL
                              OR a.delivery_lease_until < NOW()
                        )
                        ORDER BY a.id ASC
                        FOR UPDATE OF a SKIP LOCKED
                    ), claimed_digest AS (
                        UPDATE alerts AS a
                        SET delivery_lease_until = NOW() + ($2 * INTERVAL '1 second'),
                            delivery_lease_token = $3
                        FROM digest_candidates AS c
                        WHERE a.id = c.id
                        RETURNING a.id
                    ), regular_candidates AS (
                        SELECT a.id
                        FROM alerts AS a
                        JOIN alert_rules AS r ON r.id = a.rule_id
                        WHERE a.route_status = 'PENDING'
                          AND a.alert_status = 'TRIGGERED'
                          AND a.route_due_at <= NOW()
                          AND r.is_active = TRUE
                          AND r.action_type <> 'DIGEST'
                          AND (
                              a.delivery_lease_until IS NULL
                              OR a.delivery_lease_until < NOW()
                          )
                        ORDER BY a.route_due_at ASC, a.id ASC
                        LIMIT $1
                        FOR UPDATE OF a SKIP LOCKED
                    ), claimed_regular AS (
                        UPDATE alerts AS a
                        SET delivery_lease_until = NOW() + ($2 * INTERVAL '1 second'),
                            delivery_lease_token = $4
                        FROM regular_candidates AS c
                        WHERE a.id = c.id
                        RETURNING a.id, a.evaluation_id, a.rule_id,
                                  a.alert_status, a.route_status,
                                  a.delivery_attempts, a.delivery_lease_token
                    )
                    SELECT c.id AS alert_id, c.evaluation_id, c.alert_status,
                           c.route_status, c.delivery_attempts,
                           c.delivery_lease_token,
                           r.id AS id, r.project_id, r.rule_name,
                           r.threshold, r.action_type, r.notification_target,
                           e.run_id, e.drift_distance, e.matched_baseline_id,
                           e.evaluation_latency_ms, e.is_anomaly,
                           tr.prompt_text, tr.output_text, tr.ingested_at
                    FROM claimed_regular AS c
                    JOIN alert_rules AS r ON r.id = c.rule_id
                    JOIN evaluations AS e ON e.id = c.evaluation_id
                    JOIN telemetry_runs AS tr ON tr.id = e.run_id
                    ORDER BY c.id ASC
                    """,
                    limit,
                    lease_seconds,
                    digest_lease_token,
                    regular_lease_token,
                )
                digest_summary = await connection.fetchrow(
                    """
                    SELECT COUNT(*)::bigint AS total_count,
                           r.id AS id, r.project_id, r.rule_name,
                           r.threshold, r.action_type, r.notification_target,
                           (MIN(e.evaluated_at) AT TIME ZONE 'UTC')::date
                               AS digest_day
                    FROM alerts AS a
                    JOIN alert_rules AS r ON r.id = a.rule_id
                    JOIN evaluations AS e ON e.id = a.evaluation_id
                    WHERE a.delivery_lease_token = $1
                    GROUP BY r.id, r.project_id, r.rule_name, r.threshold,
                             r.action_type, r.notification_target
                    """,
                    digest_lease_token,
                )
                digest = None
                if digest_summary is not None:
                    evidence_rows = await connection.fetch(
                        """
                        SELECT a.id AS alert_id, a.evaluation_id, a.alert_status,
                               a.route_status, a.delivery_attempts,
                               a.delivery_lease_token,
                               r.id AS id, r.project_id, r.rule_name,
                               r.threshold, r.action_type, r.notification_target,
                               e.run_id, e.drift_distance, e.matched_baseline_id,
                               e.evaluation_latency_ms, e.is_anomaly,
                               tr.prompt_text, tr.output_text, tr.ingested_at
                        FROM alerts AS a
                        JOIN alert_rules AS r ON r.id = a.rule_id
                        JOIN evaluations AS e ON e.id = a.evaluation_id
                        JOIN telemetry_runs AS tr ON tr.id = e.run_id
                        WHERE a.delivery_lease_token = $1
                        ORDER BY e.drift_distance DESC NULLS LAST, a.id ASC
                        LIMIT 20
                        """,
                        digest_lease_token,
                    )
                    digest = DigestDelivery(
                        lease_token=digest_lease_token,
                        project_id=digest_summary["project_id"],
                        rule=self._rule(digest_summary),
                        digest_day=digest_summary["digest_day"],
                        total_count=int(digest_summary["total_count"]),
                        evidence=tuple(self._delivery_item(row) for row in evidence_rows),
                    )
        return DeliveryBatch(
            items=tuple(self._delivery_item(row) for row in regular_rows),
            digest=digest,
        )

    async def start_delivery_attempt(
        self,
        alert_ids: Sequence[UUID],
        lease_token: UUID,
    ) -> bool:
        """Count an outbound attempt only after this worker owns its route lock."""

        if not alert_ids:
            return False
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                UPDATE alerts AS a
                SET delivery_attempts = delivery_attempts + 1
                FROM alert_rules AS r
                WHERE a.id = ANY($1::uuid[])
                  AND a.rule_id = r.id
                  AND a.route_status = 'PENDING'
                  AND a.alert_status = 'TRIGGERED'
                  AND a.delivery_lease_token = $2
                  AND a.delivery_lease_until >= NOW()
                  AND r.is_active = TRUE
                  AND r.action_type = 'NOTIFY'
                RETURNING a.id
                """,
                list(alert_ids),
                lease_token,
            )
        return len(rows) == len(set(alert_ids))

    async def release_delivery_claim(
        self,
        alert_ids: Sequence[UUID],
        *,
        lease_token: UUID,
        retry_delay_seconds: int = 1,
    ) -> None:
        """Release an unsent claim without pretending an outbound attempt occurred."""

        if not alert_ids:
            return
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE alerts
                SET delivery_lease_until = NULL,
                    delivery_lease_token = NULL,
                    route_due_at = GREATEST(
                        route_due_at,
                        NOW() + ($3 * INTERVAL '1 second')
                    )
                WHERE id = ANY($1::uuid[])
                  AND route_status = 'PENDING'
                  AND delivery_lease_token = $2
                """,
                list(alert_ids),
                lease_token,
                retry_delay_seconds,
            )

    async def start_digest_delivery_attempt(
        self,
        lease_token: UUID,
        *,
        expected_count: int,
    ) -> bool:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                UPDATE alerts AS a
                SET delivery_attempts = delivery_attempts + 1
                FROM alert_rules AS r
                WHERE a.delivery_lease_token = $1
                  AND a.rule_id = r.id
                  AND a.route_status = 'PENDING'
                  AND a.alert_status = 'TRIGGERED'
                  AND a.delivery_lease_until >= NOW()
                  AND r.is_active = TRUE
                  AND r.action_type = 'DIGEST'
                RETURNING a.id
                """,
                lease_token,
            )
        return len(rows) == expected_count

    async def release_digest_claim(
        self,
        lease_token: UUID,
        *,
        retry_delay_seconds: int = 1,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE alerts
                SET delivery_lease_until = NULL,
                    delivery_lease_token = NULL,
                    route_due_at = GREATEST(
                        route_due_at,
                        NOW() + ($2 * INTERVAL '1 second')
                    )
                WHERE delivery_lease_token = $1
                  AND route_status = 'PENDING'
                """,
                lease_token,
                retry_delay_seconds,
            )

    async def mark_digest_delivered(self, lease_token: UUID) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE alerts
                SET route_status = 'DELIVERED',
                    notified_at = NOW(),
                    delivery_lease_until = NULL,
                    delivery_lease_token = NULL,
                    last_delivery_error = NULL
                WHERE delivery_lease_token = $1
                  AND route_status = 'PENDING'
                """,
                lease_token,
            )

    async def record_digest_delivery_failure(
        self,
        lease_token: UUID,
        *,
        error_type: str,
        max_attempts: int,
    ) -> None:
        safe_error = error_type[:200]
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE alerts
                SET route_status = CASE
                        WHEN delivery_attempts >= $3 THEN 'FAILED'
                        ELSE 'PENDING'
                    END,
                    route_due_at = CASE
                        WHEN delivery_attempts >= $3 THEN route_due_at
                        ELSE NOW() + (
                            LEAST(300, POWER(2, LEAST(delivery_attempts, 8)))
                            * INTERVAL '1 second'
                        )
                    END,
                    delivery_lease_until = NULL,
                    delivery_lease_token = NULL,
                    last_delivery_error = $2,
                    notified_at = NULL
                WHERE delivery_lease_token = $1
                  AND route_status = 'PENDING'
                """,
                lease_token,
                safe_error,
                max_attempts,
            )

    async def mark_delivered(
        self,
        alert_ids: Sequence[UUID],
        lease_token: UUID,
    ) -> None:
        if not alert_ids:
            return
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE alerts
                SET route_status = 'DELIVERED',
                    notified_at = NOW(),
                    delivery_lease_until = NULL,
                    delivery_lease_token = NULL,
                    last_delivery_error = NULL
                WHERE id = ANY($1::uuid[])
                  AND route_status = 'PENDING'
                  AND delivery_lease_token = $2
                """,
                list(alert_ids),
                lease_token,
            )

    async def mark_suppressed(
        self,
        alert_ids: Sequence[UUID],
        lease_token: UUID,
    ) -> None:
        if not alert_ids:
            return
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE alerts
                SET alert_status = 'SNOOZED',
                    route_status = 'SUPPRESSED',
                    delivery_lease_until = NULL,
                    delivery_lease_token = NULL,
                    notified_at = NULL,
                    last_delivery_error = NULL
                WHERE id = ANY($1::uuid[])
                  AND route_status = 'PENDING'
                  AND delivery_lease_token = $2
                """,
                list(alert_ids),
                lease_token,
            )

    async def record_delivery_failure(
        self,
        alert_ids: Sequence[UUID],
        *,
        lease_token: UUID,
        error_type: str,
        max_attempts: int,
    ) -> None:
        if not alert_ids:
            return
        safe_error = error_type[:200]
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE alerts
                SET route_status = CASE
                        WHEN delivery_attempts >= $4 THEN 'FAILED'
                        ELSE 'PENDING'
                    END,
                    route_due_at = CASE
                        WHEN delivery_attempts >= $4 THEN route_due_at
                        ELSE NOW() + (
                            LEAST(300, POWER(2, LEAST(delivery_attempts, 8)))
                            * INTERVAL '1 second'
                        )
                    END,
                    delivery_lease_until = NULL,
                    delivery_lease_token = NULL,
                    last_delivery_error = $3,
                    notified_at = NULL
                WHERE id = ANY($1::uuid[])
                  AND route_status = 'PENDING'
                  AND delivery_lease_token = $2
                """,
                list(alert_ids),
                lease_token,
                safe_error,
                max_attempts,
            )

    async def mark_run_failed(self, run_id: UUID) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                "UPDATE telemetry_runs SET status = 'failed' WHERE id = $1",
                run_id,
            )

    async def mark_run_queued(self, run_id: UUID) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                "UPDATE telemetry_runs SET status = 'queued' WHERE id = $1",
                run_id,
            )

    async def _persist_alert(
        self,
        connection: asyncpg.Connection,
        evaluation: Evaluation,
        rule: AlertRule,
    ) -> Alert:
        alert_id = uuid5(ALERT_ID_NAMESPACE, f"{evaluation.run_id}:{rule.id}")
        initial_status = (
            AlertStatus.SNOOZED if rule.action_type is ActionType.MUTE else AlertStatus.TRIGGERED
        )
        route_status = (
            RouteStatus.SUPPRESSED if rule.action_type is ActionType.MUTE else RouteStatus.PENDING
        )
        inserted = await connection.fetchrow(
            """
            INSERT INTO alerts (
                id, evaluation_id, rule_id, alert_status, route_status,
                route_due_at, notified_at
            )
            VALUES (
                $1, $2, $3, $4, $5,
                CASE
                    WHEN $6 = 'NOTIFY' THEN NOW()
                    WHEN $6 = 'DIGEST'
                        THEN (
                            date_trunc('day', NOW() AT TIME ZONE 'UTC')
                            + INTERVAL '1 day'
                        ) AT TIME ZONE 'UTC'
                    ELSE NULL
                END,
                NULL
            )
            ON CONFLICT (id) DO NOTHING
            RETURNING alert_status, route_status, delivery_attempts
            """,
            alert_id,
            evaluation.id,
            rule.id,
            initial_status.value,
            route_status.value,
            rule.action_type.value,
        )
        created = inserted is not None
        if inserted is None:
            inserted = await connection.fetchrow(
                """
                SELECT alert_status, route_status, delivery_attempts
                FROM alerts
                WHERE id = $1
                """,
                alert_id,
            )
        if inserted is None:
            raise RuntimeError("alert upsert did not return a persisted row")
        return Alert(
            id=alert_id,
            evaluation_id=evaluation.id,
            rule=rule,
            status=AlertStatus(inserted["alert_status"].upper()),
            created=created,
            route_status=RouteStatus(inserted["route_status"].upper()),
            delivery_attempts=inserted["delivery_attempts"],
        )

    @staticmethod
    def _evaluation(row: Any) -> Evaluation:
        return Evaluation(
            id=row["id"],
            run_id=row["run_id"],
            drift_distance=row["drift_distance"],
            matched_baseline_id=row["matched_baseline_id"],
            evaluation_latency_ms=row["evaluation_latency_ms"],
            is_anomaly=row["is_anomaly"],
        )

    @staticmethod
    def _rule(row: Any) -> AlertRule:
        return AlertRule(
            id=row["id"],
            project_id=row["project_id"],
            rule_name=row["rule_name"],
            threshold=float(row["threshold"]),
            action_type=ActionType(row["action_type"].upper()),
            notification_target=row["notification_target"],
        )

    @classmethod
    def _delivery_item(cls, row: Any) -> DeliveryItem:
        rule = cls._rule(row)
        evaluation = Evaluation(
            id=row["evaluation_id"],
            run_id=row["run_id"],
            drift_distance=row["drift_distance"],
            matched_baseline_id=row["matched_baseline_id"],
            evaluation_latency_ms=row["evaluation_latency_ms"],
            is_anomaly=row["is_anomaly"],
        )
        alert = Alert(
            id=row["alert_id"],
            evaluation_id=evaluation.id,
            rule=rule,
            status=AlertStatus(row["alert_status"].upper()),
            created=False,
            route_status=RouteStatus(row["route_status"].upper()),
            delivery_attempts=row["delivery_attempts"],
            delivery_lease_token=row["delivery_lease_token"],
        )
        run = TelemetryRun(
            id=row["run_id"],
            project_id=row["project_id"],
            output_text=row["output_text"],
            ingested_at=row["ingested_at"],
            prompt_text=row["prompt_text"],
        )
        return DeliveryItem(alert=alert, evaluation=evaluation, run=run)
