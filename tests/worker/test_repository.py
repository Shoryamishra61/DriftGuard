from uuid import uuid4

import pytest

from app_worker.domain import ActionType, AlertRule, Evaluation
from app_worker.repository import PostgresRepository


class AsyncContext:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


class RecordingConnection:
    def __init__(self, *, schema=None, fetch_rows=None, alert_row=None):
        self.schema = schema
        self.fetch_rows = [] if fetch_rows is None else fetch_rows
        self.alert_row = alert_row
        self.queries = []

    def transaction(self):
        return AsyncContext()

    async def fetchrow(self, query, *args):
        self.queries.append((query, args))
        if "to_regclass" in query:
            return self.schema
        return self.alert_row

    async def fetch(self, query, *args):
        self.queries.append((query, args))
        return self.fetch_rows


class RecordingPool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return AsyncContext(self.connection)


def schema_row(
    *,
    delivery_ready: bool,
    baseline_ready: bool,
    delivery_index_ready: bool = True,
):
    return {
        "telemetry_runs": "telemetry_runs",
        "evaluations": "evaluations",
        "alert_rules": "alert_rules",
        "alerts": "alerts",
        "delivery_lease_index": (
            "idx_alerts_delivery_lease_token" if delivery_index_ready else None
        ),
        "delivery_schema_ready": delivery_ready,
        "baseline_schema_ready": baseline_ready,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delivery_ready", "baseline_ready", "delivery_index_ready"),
    [
        (False, True, True),
        (True, False, True),
        (False, False, True),
        (True, True, False),
    ],
)
async def test_startup_ping_rejects_pre_head_schema(
    delivery_ready,
    baseline_ready,
    delivery_index_ready,
) -> None:
    connection = RecordingConnection(
        schema=schema_row(
            delivery_ready=delivery_ready,
            baseline_ready=baseline_ready,
            delivery_index_ready=delivery_index_ready,
        )
    )
    repository = PostgresRepository(RecordingPool(connection))

    with pytest.raises(RuntimeError, match="not migrated"):
        await repository.ping()


@pytest.mark.asyncio
async def test_startup_ping_accepts_revision_0005_schema() -> None:
    connection = RecordingConnection(schema=schema_row(delivery_ready=True, baseline_ready=True))
    repository = PostgresRepository(RecordingPool(connection))

    await repository.ping()

    query = connection.queries[0][0]
    assert "COUNT(*) = 7" in query
    assert "active_baseline_set" in query
    assert "delivery_lease_token" in query
    assert "idx_alerts_delivery_lease_token" in query
    assert "created_at" not in query


@pytest.mark.asyncio
async def test_delivery_claim_uses_evaluated_day_utc_and_defensive_rule_filters() -> None:
    connection = RecordingConnection(fetch_rows=[])
    repository = PostgresRepository(RecordingPool(connection))

    batch = await repository.claim_due_deliveries(limit=100, lease_seconds=60)
    assert batch.claimed_count == 0
    assert batch.items == ()
    assert batch.digest is None

    query = connection.queries[0][0]
    assert "evaluated_at AT TIME ZONE 'UTC'" in query
    assert "a.created_at" not in query
    assert "a.alert_status = 'TRIGGERED'" in query
    assert "r.is_active = TRUE" in query
    assert "pg_try_advisory_xact_lock" in query
    assert "claimed_digest AS" in query
    assert "claimed_regular AS" in query
    assert "LIMIT $1" in query
    digest_candidates = query.split("digest_candidates AS", 1)[1].split("claimed_digest AS", 1)[0]
    assert "LIMIT" not in digest_candidates


@pytest.mark.asyncio
async def test_delivery_attempt_requires_every_lease_to_still_be_owned() -> None:
    alert_ids = [uuid4(), uuid4()]
    connection = RecordingConnection(fetch_rows=[{"id": alert_ids[0]}])
    repository = PostgresRepository(RecordingPool(connection))

    lease_token = uuid4()
    assert await repository.start_delivery_attempt(alert_ids, lease_token) is False

    query = connection.queries[0][0]
    assert "delivery_lease_until >= NOW()" in query
    assert "delivery_lease_token = $2" in query
    assert "a.alert_status = 'TRIGGERED'" in query
    assert "r.is_active = TRUE" in query
    assert "r.action_type = 'NOTIFY'" in query
    assert "RETURNING a.id" in query


@pytest.mark.asyncio
async def test_digest_due_time_is_explicitly_utc() -> None:
    project_id = uuid4()
    rule = AlertRule(
        1,
        project_id,
        "daily",
        0.3,
        ActionType.DIGEST,
        "mailto:ops@example.com",
    )
    evaluation = Evaluation(uuid4(), uuid4(), 0.4, uuid4(), 10, True)
    connection = RecordingConnection(
        alert_row={
            "alert_status": "TRIGGERED",
            "route_status": "PENDING",
            "delivery_attempts": 0,
        }
    )
    repository = PostgresRepository(RecordingPool(connection))

    await repository._persist_alert(connection, evaluation, rule)

    query = connection.queries[0][0]
    assert "NOW() AT TIME ZONE 'UTC'" in query
    assert ") AT TIME ZONE 'UTC'" in query
