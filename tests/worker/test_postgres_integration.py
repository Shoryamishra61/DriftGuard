"""Opt-in PostgreSQL coverage for worker delivery claims and lease fencing."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, time, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest

from app_worker.domain import UTC, ActionType, RouteStatus
from app_worker.repository import PostgresRepository


def _test_database_url() -> str:
    configured = os.getenv("DRIFTGUARD_TEST_DATABASE_URL", "").strip()
    if not configured:
        pytest.skip("set DRIFTGUARD_TEST_DATABASE_URL to a migrated PostgreSQL database")
    return configured.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _insert_alert(
    connection: asyncpg.Connection,
    *,
    project_id: UUID,
    rule_id: int,
    evaluated_at: datetime,
    distance: float,
    label: str,
) -> UUID:
    run_id = uuid4()
    evaluation_id = uuid4()
    alert_id = uuid4()
    await connection.execute(
        """
        INSERT INTO telemetry_runs (
            id, project_id, session_id, prompt_text, output_text,
            raw_metadata, status, ingested_at
        )
        VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, 'completed', $6)
        """,
        run_id,
        project_id,
        f"worker-integration-{label}",
        f"prompt-{label}",
        f"output-{label}",
        evaluated_at,
    )
    await connection.execute(
        """
        INSERT INTO evaluations (
            id, run_id, drift_distance, matched_baseline_id,
            evaluation_latency_ms, is_anomaly, evaluated_at
        )
        VALUES ($1, $2, $3, $4, 12, TRUE, $5)
        """,
        evaluation_id,
        run_id,
        distance,
        uuid4(),
        evaluated_at,
    )
    await connection.execute(
        """
        INSERT INTO alerts (
            id, evaluation_id, rule_id, alert_status, notified_at,
            route_status, route_due_at, delivery_attempts
        )
        VALUES (
            $1, $2, $3, 'TRIGGERED', NULL,
            'PENDING', NOW() - INTERVAL '1 minute', 0
        )
        """,
        alert_id,
        evaluation_id,
        rule_id,
    )
    return alert_id


@pytest.mark.asyncio
async def test_real_concurrent_claim_maps_whole_digest_and_fences_transitions() -> None:
    database_url = _test_database_url()
    bootstrap = await asyncpg.connect(database_url)
    repository: PostgresRepository | None = None
    project_id = uuid4()
    notify_alert_id: UUID | None = None
    digest_alert_ids: list[UUID] = []

    try:
        repository = await PostgresRepository.connect(database_url, max_size=4)
        await repository.ping()
        digest_day = datetime.now(UTC).date() - timedelta(days=1)
        digest_evaluated_at = datetime.combine(
            digest_day,
            time(hour=12),
            tzinfo=UTC,
        )

        async with bootstrap.transaction():
            await bootstrap.execute(
                """
                INSERT INTO projects (id, name, api_key_hash)
                VALUES ($1, $2, $3)
                """,
                project_id,
                f"worker-integration-{uuid4().hex}",
                "a" * 64,
            )
            notify_rule_id = await bootstrap.fetchval(
                """
                INSERT INTO alert_rules (
                    project_id, rule_name, threshold, action_type,
                    notification_target, is_active
                )
                VALUES ($1, 'immediate', 0.2, 'NOTIFY', $2, TRUE)
                RETURNING id
                """,
                project_id,
                "https://hooks.slack.com/services/T000/B000/secret",
            )
            digest_rule_id = await bootstrap.fetchval(
                """
                INSERT INTO alert_rules (
                    project_id, rule_name, threshold, action_type,
                    notification_target, is_active
                )
                VALUES ($1, 'daily', 0.2, 'DIGEST', $2, TRUE)
                RETURNING id
                """,
                project_id,
                "mailto:ops@example.com",
            )
            notify_alert_id = await _insert_alert(
                bootstrap,
                project_id=project_id,
                rule_id=notify_rule_id,
                evaluated_at=datetime.now(UTC),
                distance=0.8,
                label="notify",
            )
            for index in range(25):
                digest_alert_ids.append(
                    await _insert_alert(
                        bootstrap,
                        project_id=project_id,
                        rule_id=digest_rule_id,
                        evaluated_at=digest_evaluated_at,
                        distance=0.5 + (index / 100),
                        label=f"digest-{index}",
                    )
                )

        batches = await asyncio.gather(
            repository.claim_due_deliveries(limit=1, lease_seconds=60),
            repository.claim_due_deliveries(limit=1, lease_seconds=60),
        )

        assert sum(batch.claimed_count for batch in batches) == 26
        notify_items = [item for batch in batches for item in batch.items]
        digests = [batch.digest for batch in batches if batch.digest is not None]
        assert len(notify_items) == 1
        assert len(digests) == 1

        notify_item = notify_items[0]
        digest = digests[0]
        assert notify_item.alert.id == notify_alert_id
        assert notify_item.alert.rule.action_type is ActionType.NOTIFY
        assert notify_item.alert.route_status is RouteStatus.PENDING
        assert notify_item.alert.delivery_lease_token is not None
        assert notify_item.run.project_id == project_id
        assert notify_item.run.prompt_text == "prompt-notify"
        assert notify_item.run.output_text == "output-notify"
        assert notify_item.evaluation.drift_distance == pytest.approx(0.8)

        assert digest.project_id == project_id
        assert digest.rule.action_type is ActionType.DIGEST
        assert digest.digest_day == digest_day
        assert digest.total_count == 25
        assert len(digest.evidence) == 20
        assert all(item.alert.rule.id == digest.rule.id for item in digest.evidence)
        evidence_distances = [item.evaluation.drift_distance for item in digest.evidence]
        assert evidence_distances == sorted(evidence_distances, reverse=True)
        assert evidence_distances[0] == pytest.approx(0.74)
        assert evidence_distances[-1] == pytest.approx(0.55)
        assert digest.evidence[0].run.prompt_text == "prompt-digest-24"
        assert digest.evidence[0].run.output_text == "output-digest-24"

        digest_rows = await bootstrap.fetch(
            """
            SELECT id, delivery_lease_token, delivery_attempts, route_status
            FROM alerts
            WHERE id = ANY($1::uuid[])
            ORDER BY id
            """,
            digest_alert_ids,
        )
        assert len(digest_rows) == 25
        assert {row["delivery_lease_token"] for row in digest_rows} == {digest.lease_token}

        wrong_token = uuid4()
        assert not await repository.start_delivery_attempt(
            [notify_alert_id],
            wrong_token,
        )
        assert await repository.start_delivery_attempt(
            [notify_alert_id],
            notify_item.alert.delivery_lease_token,
        )
        await repository.mark_delivered([notify_alert_id], wrong_token)
        notify_before_commit = await bootstrap.fetchrow(
            """
            SELECT route_status, delivery_attempts, delivery_lease_token, notified_at
            FROM alerts WHERE id = $1
            """,
            notify_alert_id,
        )
        assert notify_before_commit["route_status"] == "PENDING"
        assert notify_before_commit["delivery_attempts"] == 1
        assert notify_before_commit["delivery_lease_token"] == (
            notify_item.alert.delivery_lease_token
        )
        assert notify_before_commit["notified_at"] is None
        await repository.mark_delivered(
            [notify_alert_id],
            notify_item.alert.delivery_lease_token,
        )

        assert not await repository.start_digest_delivery_attempt(
            wrong_token,
            expected_count=25,
        )
        assert await repository.start_digest_delivery_attempt(
            digest.lease_token,
            expected_count=25,
        )
        await repository.mark_digest_delivered(wrong_token)
        pending_digest_count = await bootstrap.fetchval(
            """
            SELECT COUNT(*) FROM alerts
            WHERE id = ANY($1::uuid[]) AND route_status = 'PENDING'
            """,
            digest_alert_ids,
        )
        assert pending_digest_count == 25
        await repository.mark_digest_delivered(digest.lease_token)

        final_rows = await bootstrap.fetch(
            """
            SELECT id, route_status, delivery_attempts,
                   delivery_lease_until, delivery_lease_token, notified_at
            FROM alerts
            WHERE id = $1 OR id = ANY($2::uuid[])
            """,
            notify_alert_id,
            digest_alert_ids,
        )
        assert len(final_rows) == 26
        assert {row["route_status"] for row in final_rows} == {"DELIVERED"}
        assert {row["delivery_attempts"] for row in final_rows} == {1}
        assert all(row["delivery_lease_until"] is None for row in final_rows)
        assert all(row["delivery_lease_token"] is None for row in final_rows)
        assert all(row["notified_at"] is not None for row in final_rows)
    finally:
        if repository is not None:
            await repository.close()
        try:
            await bootstrap.execute("DELETE FROM projects WHERE id = $1", project_id)
        finally:
            await bootstrap.close()


@pytest.mark.asyncio
async def test_real_session_run_lock_contends_and_releases_on_cancellation() -> None:
    database_url = _test_database_url()
    first = await PostgresRepository.connect(database_url, max_size=2)
    second = await PostgresRepository.connect(database_url, max_size=2)
    run_id = uuid4()
    owner_ready = asyncio.Event()
    hold_owner = asyncio.Event()

    async def own_lock() -> None:
        async with first.run_processing_lock(run_id) as acquired:
            assert acquired
            owner_ready.set()
            await hold_owner.wait()

    owner = asyncio.create_task(own_lock())
    try:
        await owner_ready.wait()
        async with second.run_processing_lock(run_id) as acquired:
            assert not acquired

        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner

        async with second.run_processing_lock(run_id) as acquired:
            assert acquired
            async with first.run_processing_lock(run_id) as stale_acquired:
                assert not stale_acquired
            async with first.run_processing_lock(run_id) as still_contended:
                assert not still_contended
    finally:
        if not owner.done():
            owner.cancel()
            await asyncio.gather(owner, return_exceptions=True)
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_real_retention_redacts_expires_and_honors_legal_holds() -> None:
    database_url = _test_database_url()
    connection = await asyncpg.connect(database_url)
    repository = await PostgresRepository.connect(database_url, max_size=2)
    project_id = uuid4()
    expired_run_id = uuid4()
    held_run_id = uuid4()
    redacted_run_id = uuid4()
    now = datetime.now(UTC)

    try:
        await connection.execute(
            "INSERT INTO projects (id, name, api_key_hash) VALUES ($1, $2, $3)",
            project_id,
            f"retention-integration-{uuid4().hex}",
            "b" * 64,
        )
        for run_id, age_days, label in (
            (expired_run_id, 120, "expired"),
            (held_run_id, 100, "held"),
            (redacted_run_id, 40, "redacted"),
        ):
            ingested_at = now - timedelta(days=age_days)
            await connection.execute(
                """
                INSERT INTO telemetry_runs (
                    id, project_id, session_id, prompt_text, output_text,
                    raw_metadata, status, ingested_at
                )
                VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, 'completed', $6)
                """,
                run_id,
                project_id,
                f"retention-{label}",
                f"prompt-{label}",
                f"output-{label}",
                ingested_at,
            )
            await connection.execute(
                """
                INSERT INTO telemetry_outbox (
                    id, run_id, event_type, payload, status,
                    retry_count, next_attempt_at, dispatch_time
                )
                VALUES (
                    $1, $2, 'TELEMETRY_INGESTED',
                    jsonb_build_object(
                        'event_id', ($1::uuid)::text,
                        'run_id', ($2::uuid)::text
                    ),
                    'DISPATCHED', 0, $3, $3
                )
                """,
                uuid4(),
                run_id,
                ingested_at,
            )
        await connection.execute(
            """
            INSERT INTO legal_holds (project_id, starts_at, ends_at, reason)
            VALUES ($1, $2, $3, 'integration preservation hold')
            """,
            project_id,
            now - timedelta(days=101),
            now - timedelta(days=99),
        )

        async with repository.retention_lock() as retention_connection:
            assert retention_connection is not None
            redacted = await repository.redact_expired_telemetry(
                retention_connection,
                cutoff=now - timedelta(days=30),
                batch_size=100,
            )
            purged_events = await repository.purge_dispatched_outbox(
                retention_connection,
                cutoff=now - timedelta(days=7),
                batch_size=100,
            )
            expired = await repository.expire_telemetry_runs(
                retention_connection,
                cutoff=now - timedelta(days=90),
                batch_size=100,
            )
            vector_ids = await repository.pending_vector_deletions(
                retention_connection,
                batch_size=100,
            )
            await repository.complete_vector_deletions(retention_connection, vector_ids)

        assert redacted == 2
        assert purged_events == 2
        assert expired == 1
        assert vector_ids == [expired_run_id]
        assert await connection.fetchval(
            "SELECT COUNT(*) FROM telemetry_runs WHERE id = $1", expired_run_id
        ) == 0
        held = await connection.fetchrow(
            "SELECT prompt_text, output_text FROM telemetry_runs WHERE id = $1",
            held_run_id,
        )
        assert held["prompt_text"] == "prompt-held"
        assert held["output_text"] == "output-held"
        assert await connection.fetchval(
            "SELECT COUNT(*) FROM telemetry_outbox WHERE run_id = $1", held_run_id
        ) == 1
        redacted_row = await connection.fetchrow(
            """
            SELECT prompt_text, output_text,
                   (raw_metadata->>'retention_redacted')::boolean AS retention_redacted
            FROM telemetry_runs WHERE id = $1
            """,
            redacted_run_id,
        )
        assert redacted_row["prompt_text"] == "[retention-redacted]"
        assert redacted_row["output_text"] == "[retention-redacted]"
        assert redacted_row["retention_redacted"] is True
        assert await connection.fetchval(
            """
            SELECT COUNT(*) FROM retention_vector_outbox
            WHERE run_id = $1 AND status = 'COMPLETED'
            """,
            expired_run_id,
        ) == 1
    finally:
        await connection.execute("DELETE FROM projects WHERE id = $1", project_id)
        await connection.execute(
            "DELETE FROM retention_vector_outbox WHERE run_id = $1", expired_run_id
        )
        await repository.close()
        await connection.close()
