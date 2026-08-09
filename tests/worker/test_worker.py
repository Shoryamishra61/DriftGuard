import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import date, datetime
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from app_worker.circuit_breaker import CircuitOpenError
from app_worker.config import (
    DEFAULT_EMBEDDING_REVISION,
    ConfigurationError,
    WorkerConfig,
)
from app_worker.domain import (
    UTC,
    ActionType,
    Alert,
    AlertRule,
    AlertStatus,
    BaselineMatch,
    DeliveryBatch,
    DeliveryItem,
    DigestDelivery,
    Evaluation,
    Job,
    RouteStatus,
    TelemetryRun,
)
from app_worker.worker import DriftWorker, EmptyOutputError, MalformedJobError


class MemoryRepository:
    def __init__(self, runs: list[TelemetryRun], rules: list[AlertRule]) -> None:
        self.runs = {run.id: run for run in runs}
        self.rules = rules
        self.inactive_rule_ids: set[int] = set()
        self.evaluations: dict[UUID, Evaluation] = {}
        self.alerts: dict[UUID, Alert] = {}
        self.run_status = {run.id: "queued" for run in runs}
        self.persist_calls = 0
        self.claimed: set[UUID] = set()
        self.active_digest_group: tuple[UUID, int, str] | None = None
        self.events: list[str] = []
        self.processing_locks: set[UUID] = set()

    @asynccontextmanager
    async def run_processing_lock(self, run_id):
        if run_id in self.processing_locks:
            yield False
            return
        self.processing_locks.add(run_id)
        try:
            yield True
        finally:
            self.processing_locks.remove(run_id)

    async def claim_run(self, run_id):
        run = self.runs.get(run_id)
        if run is None:
            return None, None
        existing = self.evaluations.get(run_id)
        self.run_status[run_id] = "completed" if existing else "processing"
        return run, existing

    async def active_rules(self, project_id):
        return [
            rule
            for rule in self.rules
            if rule.project_id == project_id and rule.id not in self.inactive_rule_ids
        ]

    async def persist_evaluation_and_alerts(
        self,
        *,
        run,
        drift_distance,
        matched_baseline_id,
        evaluation_latency_ms,
    ):
        self.persist_calls += 1
        existing = self.evaluations.get(run.id)
        created = existing is None
        current_rules = [
            rule
            for rule in self.rules
            if rule.project_id == run.project_id and rule.id not in self.inactive_rule_ids
        ]
        is_anomaly = drift_distance is not None and any(
            drift_distance > rule.threshold for rule in current_rules
        )
        evaluation = existing or Evaluation(
            id=uuid5(NAMESPACE_URL, f"evaluation:{run.id}"),
            run_id=run.id,
            drift_distance=drift_distance,
            matched_baseline_id=matched_baseline_id,
            evaluation_latency_ms=evaluation_latency_ms,
            is_anomaly=is_anomaly,
        )
        self.evaluations[run.id] = evaluation
        alerts = []
        if created and evaluation.drift_distance is not None:
            for rule in current_rules:
                if evaluation.drift_distance <= rule.threshold:
                    continue
                alert_id = uuid5(NAMESPACE_URL, f"alert:{run.id}:{rule.id}")
                muted = rule.action_type is ActionType.MUTE
                alert = Alert(
                    id=alert_id,
                    evaluation_id=evaluation.id,
                    rule=rule,
                    status=AlertStatus.SNOOZED if muted else AlertStatus.TRIGGERED,
                    created=True,
                    route_status=(RouteStatus.SUPPRESSED if muted else RouteStatus.PENDING),
                )
                self.alerts[alert_id] = alert
                alerts.append(alert)
        self.run_status[run.id] = "completed"
        return evaluation, created, alerts

    async def claim_due_deliveries(self, *, limit, lease_seconds):
        del lease_seconds
        eligible = [
            alert
            for alert in self.alerts.values()
            if alert.route_status is RouteStatus.PENDING
            and alert.status is AlertStatus.TRIGGERED
            and alert.rule.id not in self.inactive_rule_ids
            and alert.id not in self.claimed
        ]
        digest_alerts = [alert for alert in eligible if alert.rule.action_type is ActionType.DIGEST]
        digest_delivery = None
        if digest_alerts and self.active_digest_group is None:
            first = sorted(digest_alerts, key=lambda alert: str(alert.id))[0]
            group = (
                first.rule.project_id,
                first.rule.id,
                first.rule.notification_target,
            )
            grouped_digest_alerts = [
                alert
                for alert in sorted(digest_alerts, key=lambda alert: str(alert.id))
                if (
                    alert.rule.project_id,
                    alert.rule.id,
                    alert.rule.notification_target,
                )
                == group
            ]
            digest_token = uuid4()
            claimed_digest_alerts = []
            for alert in grouped_digest_alerts:
                claimed = replace(alert, delivery_lease_token=digest_token)
                self.alerts[alert.id] = claimed
                claimed_digest_alerts.append(claimed)
            self.claimed.update(alert.id for alert in claimed_digest_alerts)
            self.active_digest_group = group
            digest_delivery = DigestDelivery(
                lease_token=digest_token,
                project_id=first.rule.project_id,
                rule=first.rule,
                digest_day=date(2026, 8, 9),
                total_count=len(claimed_digest_alerts),
                evidence=tuple(self._delivery_item(alert) for alert in claimed_digest_alerts[:20]),
            )
        regular_candidates = sorted(
            [alert for alert in eligible if alert.rule.action_type is not ActionType.DIGEST],
            key=lambda alert: str(alert.id),
        )[:limit]
        selected = regular_candidates
        lease_token = uuid4()
        claimed_alerts = []
        for alert in selected:
            claimed = replace(alert, delivery_lease_token=lease_token)
            self.alerts[alert.id] = claimed
            claimed_alerts.append(claimed)
        self.claimed.update(alert.id for alert in claimed_alerts)
        return DeliveryBatch(
            items=tuple(self._delivery_item(alert) for alert in claimed_alerts),
            digest=digest_delivery,
        )

    async def start_delivery_attempt(self, alert_ids, lease_token):
        if any(self.alerts[alert_id].delivery_lease_token != lease_token for alert_id in alert_ids):
            return False
        for alert_id in alert_ids:
            alert = self.alerts[alert_id]
            self.alerts[alert_id] = replace(
                alert,
                delivery_attempts=alert.delivery_attempts + 1,
                created=False,
            )
        return True

    async def release_delivery_claim(
        self,
        alert_ids,
        *,
        lease_token,
        retry_delay_seconds=1,
    ):
        del retry_delay_seconds
        owned = [
            alert_id
            for alert_id in alert_ids
            if self.alerts[alert_id].delivery_lease_token == lease_token
        ]
        for alert_id in owned:
            self.alerts[alert_id] = replace(self.alerts[alert_id], delivery_lease_token=None)
        self.claimed.difference_update(owned)
        self._release_digest_group(owned)

    async def start_digest_delivery_attempt(self, lease_token, *, expected_count):
        owned = [
            alert
            for alert in self.alerts.values()
            if alert.delivery_lease_token == lease_token
            and alert.route_status is RouteStatus.PENDING
        ]
        if len(owned) != expected_count:
            return False
        for alert in owned:
            self.alerts[alert.id] = replace(
                alert,
                delivery_attempts=alert.delivery_attempts + 1,
            )
        return True

    async def release_digest_claim(self, lease_token, *, retry_delay_seconds=1):
        del retry_delay_seconds
        owned = [
            alert.id for alert in self.alerts.values() if alert.delivery_lease_token == lease_token
        ]
        for alert_id in owned:
            self.alerts[alert_id] = replace(
                self.alerts[alert_id],
                delivery_lease_token=None,
            )
        self.claimed.difference_update(owned)
        if owned:
            self.active_digest_group = None

    async def mark_digest_delivered(self, lease_token):
        owned = [
            alert.id for alert in self.alerts.values() if alert.delivery_lease_token == lease_token
        ]
        for alert_id in owned:
            self.alerts[alert_id] = replace(
                self.alerts[alert_id],
                route_status=RouteStatus.DELIVERED,
                delivery_lease_token=None,
                created=False,
            )
        self.claimed.difference_update(owned)
        if owned:
            self.active_digest_group = None

    async def record_digest_delivery_failure(
        self,
        lease_token,
        *,
        error_type,
        max_attempts,
    ):
        del error_type
        owned = [
            alert.id for alert in self.alerts.values() if alert.delivery_lease_token == lease_token
        ]
        for alert_id in owned:
            alert = self.alerts[alert_id]
            self.alerts[alert_id] = replace(
                alert,
                route_status=(
                    RouteStatus.FAILED
                    if alert.delivery_attempts >= max_attempts
                    else RouteStatus.PENDING
                ),
                delivery_lease_token=None,
            )
        self.claimed.difference_update(owned)
        if owned:
            self.active_digest_group = None

    async def mark_delivered(self, alert_ids, lease_token):
        for alert_id in alert_ids:
            if self.alerts[alert_id].delivery_lease_token != lease_token:
                continue
            self.alerts[alert_id] = replace(
                self.alerts[alert_id],
                route_status=RouteStatus.DELIVERED,
                delivery_lease_token=None,
                created=False,
            )
        self.claimed.difference_update(alert_ids)
        self._release_digest_group(alert_ids)

    async def mark_suppressed(self, alert_ids, lease_token):
        for alert_id in alert_ids:
            if self.alerts[alert_id].delivery_lease_token != lease_token:
                continue
            self.alerts[alert_id] = replace(
                self.alerts[alert_id],
                status=AlertStatus.SNOOZED,
                route_status=RouteStatus.SUPPRESSED,
                delivery_lease_token=None,
                created=False,
            )
        self.claimed.difference_update(alert_ids)
        self._release_digest_group(alert_ids)

    async def record_delivery_failure(
        self,
        alert_ids,
        *,
        lease_token,
        error_type,
        max_attempts,
    ):
        del error_type
        for alert_id in alert_ids:
            alert = self.alerts[alert_id]
            if alert.delivery_lease_token != lease_token:
                continue
            status = (
                RouteStatus.FAILED
                if alert.delivery_attempts >= max_attempts
                else RouteStatus.PENDING
            )
            self.alerts[alert_id] = replace(
                alert,
                route_status=status,
                delivery_lease_token=None,
                created=False,
            )
        self.claimed.difference_update(alert_ids)
        self._release_digest_group(alert_ids)

    async def mark_run_failed(self, run_id):
        self.events.append("mark_failed")
        self.run_status[run_id] = "failed"

    async def mark_run_queued(self, run_id):
        self.events.append("mark_queued")
        self.run_status[run_id] = "queued"

    async def close(self):
        return None

    def _delivery_item(self, alert: Alert) -> DeliveryItem:
        evaluation = next(
            item for item in self.evaluations.values() if item.id == alert.evaluation_id
        )
        return DeliveryItem(
            alert=alert,
            evaluation=evaluation,
            run=self.runs[evaluation.run_id],
        )

    def _release_digest_group(self, alert_ids) -> None:
        if any(
            self.alerts[alert_id].rule.action_type is ActionType.DIGEST for alert_id in alert_ids
        ):
            self.active_digest_group = None


class MemoryValkey:
    def __init__(self, events: list[str] | None = None) -> None:
        self.values = {}
        self.set_calls = []
        self.lpush_calls = []
        self.events = events if events is not None else []
        self.fail_lpush_keys: set[str] = set()

    async def set(self, key, value, **kwargs):
        self.set_calls.append((key, value, kwargs))
        if kwargs.get("nx") and key in self.values:
            return None
        self.values[key] = value
        return True

    async def get(self, key):
        return self.values.get(key)

    async def eval(self, _script, _key_count, key, owner):
        if self.values.get(key) == owner:
            self.values.pop(key, None)
            return 1
        return 0

    async def lpush(self, key, value):
        self.events.append(f"lpush:{key}")
        if key in self.fail_lpush_keys:
            raise ConnectionError("Valkey unavailable")
        self.lpush_calls.append((key, value))
        return 1

    async def delete(self, key):
        self.values.pop(key, None)
        return 1

    async def aclose(self):
        return None


class RecordingEmbedder:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.texts = []
        self.error = error

    async def embed(self, text):
        self.texts.append(text)
        if self.error is not None:
            raise self.error
        return [0.0] * 384


class FixedVectorStore:
    def __init__(self, match) -> None:
        self.match = match
        self.queries = []
        self.upserts = []

    async def nearest_baseline(
        self,
        embedding,
        project_id,
        baseline_set,
        embedding_model_revision,
    ):
        self.queries.append((embedding, project_id, baseline_set, embedding_model_revision))
        return self.match

    async def upsert_evaluation(self, embedding, **kwargs):
        self.upserts.append((embedding, kwargs))

    async def close(self):
        return None


class RecordingWebhook:
    def __init__(self, *, notify_failures=0, digest_failures=0) -> None:
        self.calls = []
        self.digest_calls = []
        self.notify_failures = notify_failures
        self.digest_failures = digest_failures

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        if self.notify_failures:
            self.notify_failures -= 1
            raise OSError("webhook unavailable")

    async def send_digest(
        self,
        items,
        *,
        total_count=None,
        digest_day=None,
        idempotency_key=None,
    ):
        self.digest_calls.append(
            {
                "items": list(items),
                "total_count": total_count,
                "digest_day": digest_day,
                "idempotency_key": idempotency_key,
            }
        )
        if self.digest_failures:
            self.digest_failures -= 1
            raise OSError("digest unavailable")

    async def close(self):
        return None


def make_config(**changes) -> WorkerConfig:
    config = WorkerConfig(
        database_url="postgresql://worker:secret@db:5432/driftguard",
        valkey_host="cache",
        valkey_port=6379,
        valkey_password="test-secret",  # noqa: S106
        qdrant_host="qdrant",
        qdrant_port=6333,
        qdrant_api_key="secret",
    )
    return replace(config, **changes)


def make_run(project_id: UUID, output: str) -> TelemetryRun:
    return TelemetryRun(
        uuid4(),
        project_id,
        output,
        datetime.now(UTC),
        "stable-v1",
    )


def build_worker(
    runs,
    rules,
    *,
    similarity=0.6,
    config=None,
    embedder=None,
    webhook=None,
    valkey=None,
    repository=None,
    monotonic=None,
):
    repository = repository or MemoryRepository(runs, rules)
    valkey = valkey or MemoryValkey(repository.events)
    embedder = embedder or RecordingEmbedder()
    baseline_id = uuid4()
    vector_store = FixedVectorStore(BaselineMatch(baseline_id, similarity))
    webhook = webhook or RecordingWebhook()
    options = {}
    if monotonic is not None:
        options["monotonic"] = monotonic
    worker = DriftWorker(
        config=config or make_config(),
        repository=repository,
        valkey=valkey,
        vector_store=vector_store,
        embedder=embedder,
        webhook_sender=webhook,
        worker_id="test-worker",
        **options,
    )
    return worker, repository, valkey, embedder, vector_store, webhook


def job(run_id: UUID, *, attempt: int = 1, event_id: UUID | None = None) -> Job:
    return Job(run_id, event_id or uuid4(), attempt)


def test_database_pool_reserves_capacity_beyond_processing_locks() -> None:
    with pytest.raises(
        ConfigurationError,
        match="DB_POOL_MAX_SIZE must be greater than WORKER_CONCURRENCY",
    ):
        make_config(db_pool_max_size=4, worker_concurrency=4)


@pytest.mark.asyncio
async def test_processing_is_idempotent_and_routes_all_actions_durably() -> None:
    project_id = uuid4()
    run = make_run(project_id, "  " + ("a" * 3000))
    rules = [
        AlertRule(1, project_id, "critical", 0.30, ActionType.NOTIFY, "https://8.8.8.8"),
        AlertRule(2, project_id, "daily", 0.20, ActionType.DIGEST, "mailto:ops@example.com"),
        AlertRule(3, project_id, "silent", 0.10, ActionType.MUTE, "muted"),
    ]
    worker, repository, valkey, embedder, store, webhook = build_worker([run], rules)

    first = await worker.process_job(job(run.id))
    second = await worker.process_job(job(run.id))

    assert first.created is True
    assert second.created is False
    assert first.evaluation.drift_distance == pytest.approx(0.4)
    assert repository.persist_calls == 1
    assert len(embedder.texts) == 1
    assert len(embedder.texts[0]) == 2048
    assert store.queries[0][1:] == (
        project_id,
        "stable-v1",
        DEFAULT_EMBEDDING_REVISION,
    )
    assert store.upserts[0][1]["baseline_set"] == "stable-v1"
    assert store.upserts[0][1]["embedding_model_revision"] == DEFAULT_EMBEDDING_REVISION
    assert webhook.calls == []
    assert webhook.digest_calls == []

    assert await worker.deliver_due_once() == 2

    assert len(webhook.calls) == 1
    assert len(webhook.digest_calls) == 1
    routes = {alert.rule.action_type: alert.route_status for alert in repository.alerts.values()}
    assert routes == {
        ActionType.NOTIFY: RouteStatus.DELIVERED,
        ActionType.DIGEST: RouteStatus.DELIVERED,
        ActionType.MUTE: RouteStatus.SUPPRESSED,
    }
    dedupe_receipts = [
        call
        for call in valkey.set_calls
        if "alert-dedupe" in call[0] and not call[0].endswith(":inflight")
    ]
    assert dedupe_receipts[-1][2] == {"ex": 60}


@pytest.mark.asyncio
async def test_embedding_cache_reuses_compute_but_evaluates_every_run() -> None:
    project_id = uuid4()
    runs = [make_run(project_id, "same output"), make_run(project_id, "same output")]
    worker, repository, _valkey, embedder, store, _webhook = build_worker(runs, [])

    await worker.process_job(job(runs[0].id))
    await worker.process_job(job(runs[1].id))

    assert len(embedder.texts) == 1
    assert len(store.queries) == 2
    assert len(store.upserts) == 2
    assert len(repository.evaluations) == 2
    assert worker._embedding_cache_hits == 1
    assert worker._embedding_cache_misses == 1


@pytest.mark.asyncio
async def test_embedding_cache_is_tenant_scoped() -> None:
    runs = [make_run(uuid4(), "same output"), make_run(uuid4(), "same output")]
    worker, _repo, _valkey, embedder, _store, _webhook = build_worker(runs, [])

    await worker.process_job(job(runs[0].id))
    await worker.process_job(job(runs[1].id))

    assert len(embedder.texts) == 2
    assert worker._embedding_cache_hits == 0


@pytest.mark.asyncio
async def test_replay_does_not_apply_new_rules_retroactively() -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    worker, repository, _valkey, _embedder, _store, _webhook = build_worker([run], [])
    queued = job(run.id)

    await worker.process_job(queued)
    repository.rules.append(
        AlertRule(7, project_id, "new", 0.1, ActionType.NOTIFY, "https://8.8.8.8")
    )
    replay = await worker.process_job(queued)

    assert replay.created is False
    assert repository.alerts == {}
    assert repository.persist_calls == 1


@pytest.mark.asyncio
async def test_duplicate_run_lock_prevents_vector_race_across_baseline_switch() -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    repository = MemoryRepository([run], [])
    embed_started = asyncio.Event()
    release_embed = asyncio.Event()

    class BlockingEmbedder(RecordingEmbedder):
        async def embed(self, text):
            self.texts.append(text)
            embed_started.set()
            await release_embed.wait()
            return [0.0] * 384

    worker, _repository, _valkey, _embedder, store, _webhook = build_worker(
        [run],
        [],
        repository=repository,
        embedder=BlockingEmbedder(),
    )
    owner_job = job(run.id)
    owner = asyncio.create_task(worker.process_job(owner_job))
    await embed_started.wait()

    repository.runs[run.id] = replace(run, active_baseline_set="stable-v2")
    duplicate_payload = json.dumps(
        {
            "event_id": str(uuid4()),
            "run_id": str(run.id),
            "attempt": 1,
        }
    )
    await worker.handle_queue_payload(duplicate_payload)

    assert repository.persist_calls == 0
    assert store.queries == []
    assert store.upserts == []
    assert worker._last_completed_at is None
    assert worker._last_evaluation_latency_ms is None

    release_embed.set()
    owner_result = await owner
    assert owner_result is not None
    assert owner_result.created is True
    assert store.queries[0][2] == "stable-v1"
    assert store.upserts[0][1]["baseline_set"] == "stable-v1"

    replay = await worker.process_job(job(run.id))
    assert replay is not None
    assert replay.created is False
    assert len(store.queries) == 1
    assert len(store.upserts) == 1


@pytest.mark.asyncio
async def test_cancelled_processing_releases_run_lock_for_recovery() -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    repository = MemoryRepository([run], [])
    embed_started = asyncio.Event()
    never_release = asyncio.Event()

    class BlockingEmbedder(RecordingEmbedder):
        async def embed(self, text):
            self.texts.append(text)
            embed_started.set()
            await never_release.wait()
            return [0.0] * 384

    first_worker, *_ = build_worker(
        [run],
        [],
        repository=repository,
        embedder=BlockingEmbedder(),
    )
    owner = asyncio.create_task(first_worker.process_job(job(run.id)))
    await embed_started.wait()
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner

    assert run.id not in repository.processing_locks
    recovery_worker, _repo, _valkey, _embedder, store, _webhook = build_worker(
        [run],
        [],
        repository=repository,
    )
    recovered = await recovery_worker.process_job(job(run.id))

    assert recovered is not None
    assert recovered.created is True
    assert len(store.upserts) == 1


@pytest.mark.asyncio
async def test_rule_is_revalidated_after_embedding_before_alert_insert() -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    rule = AlertRule(
        17,
        project_id,
        "changing",
        0.1,
        ActionType.NOTIFY,
        "https://8.8.8.8",
    )
    repository = MemoryRepository([run], [rule])

    class DeactivatingEmbedder(RecordingEmbedder):
        async def embed(self, text):
            repository.inactive_rule_ids.add(rule.id)
            return await super().embed(text)

    worker, *_ = build_worker(
        [run],
        [rule],
        repository=repository,
        embedder=DeactivatingEmbedder(),
    )

    result = await worker.process_job(job(run.id))

    assert result.evaluation.is_anomaly is False
    assert repository.alerts == {}


@pytest.mark.asyncio
async def test_same_failure_mode_suppresses_followers_only_after_success() -> None:
    project_id = uuid4()
    runs = [make_run(project_id, "same failure"), make_run(project_id, "same failure")]
    rule = AlertRule(9, project_id, "critical", 0.3, ActionType.NOTIFY, "https://8.8.8.8")
    worker, repository, _valkey, _embedder, _store, webhook = build_worker(runs, [rule])
    for run in runs:
        await worker.process_job(job(run.id))

    await worker.deliver_due_once()

    assert len(webhook.calls) == 1
    assert {alert.route_status for alert in repository.alerts.values()} == {
        RouteStatus.DELIVERED,
        RouteStatus.SUPPRESSED,
    }


@pytest.mark.asyncio
async def test_failed_notify_keeps_owner_and_followers_pending() -> None:
    project_id = uuid4()
    runs = [make_run(project_id, "same failure"), make_run(project_id, "same failure")]
    rule = AlertRule(9, project_id, "critical", 0.3, ActionType.NOTIFY, "https://8.8.8.8")
    webhook = RecordingWebhook(notify_failures=1)
    worker, repository, _valkey, _embedder, _store, _webhook = build_worker(
        runs,
        [rule],
        webhook=webhook,
    )
    for run in runs:
        await worker.process_job(job(run.id))

    await worker.deliver_due_once()

    assert len(webhook.calls) == 1
    assert {alert.route_status for alert in repository.alerts.values()} == {RouteStatus.PENDING}
    assert {alert.delivery_attempts for alert in repository.alerts.values()} == {1}


@pytest.mark.asyncio
async def test_receipt_recovers_delivered_leader_after_db_commit_crash() -> None:
    project_id = uuid4()
    runs = [make_run(project_id, "same failure"), make_run(project_id, "same failure")]
    rule = AlertRule(9, project_id, "critical", 0.3, ActionType.NOTIFY, "https://8.8.8.8")

    class FailFirstDeliveryCommit(MemoryRepository):
        def __init__(self):
            super().__init__(runs, [rule])
            self.fail_commit = True

        async def mark_delivered(self, alert_ids, lease_token):
            if self.fail_commit:
                self.fail_commit = False
                raise ConnectionError("database commit unavailable")
            await super().mark_delivered(alert_ids, lease_token)

    repository = FailFirstDeliveryCommit()
    worker, _repository, valkey, _embedder, _store, webhook = build_worker(
        runs,
        [rule],
        repository=repository,
    )
    for run in runs:
        await worker.process_job(job(run.id))

    with pytest.raises(ConnectionError, match="database commit"):
        await worker.deliver_due_once()
    assert len(webhook.calls) == 1
    old_token = next(iter(repository.alerts.values())).delivery_lease_token
    assert old_token is not None
    await repository.release_delivery_claim(
        list(repository.alerts),
        lease_token=old_token,
    )

    await worker.deliver_due_once()

    assert len(webhook.calls) == 1
    assert {alert.route_status for alert in repository.alerts.values()} == {
        RouteStatus.DELIVERED,
        RouteStatus.SUPPRESSED,
    }
    receipt_calls = [call for call in valkey.set_calls if "delivery-receipt" in call[0]]
    assert receipt_calls[0][2] == {"ex": 86400}


@pytest.mark.asyncio
async def test_distinct_outputs_are_not_collapsed_into_one_failure_mode() -> None:
    project_id = uuid4()
    runs = [make_run(project_id, "failure one"), make_run(project_id, "failure two")]
    rule = AlertRule(9, project_id, "critical", 0.3, ActionType.NOTIFY, "https://8.8.8.8")
    worker, repository, _valkey, _embedder, _store, webhook = build_worker(runs, [rule])
    for run in runs:
        await worker.process_job(job(run.id))

    await worker.deliver_due_once()

    assert len(webhook.calls) == 2
    assert all(alert.route_status is RouteStatus.DELIVERED for alert in repository.alerts.values())


@pytest.mark.asyncio
async def test_inactive_or_snoozed_rules_are_never_routed() -> None:
    project_id = uuid4()
    runs = [make_run(project_id, "inactive"), make_run(project_id, "snoozed")]
    rules = [
        AlertRule(10, project_id, "inactive", 0.3, ActionType.NOTIFY, "https://8.8.8.8"),
        AlertRule(11, project_id, "snoozed", 0.3, ActionType.NOTIFY, "https://8.8.8.8"),
    ]
    worker, repository, _valkey, _embedder, _store, webhook = build_worker(runs, rules)
    for run in runs:
        await worker.process_job(job(run.id))
    repository.inactive_rule_ids.add(10)
    for alert_id, alert in list(repository.alerts.items()):
        if alert.rule.id == 11:
            repository.alerts[alert_id] = replace(
                alert,
                status=AlertStatus.SNOOZED,
            )

    assert await worker.deliver_due_once() == 0
    assert webhook.calls == []


@pytest.mark.asyncio
async def test_digest_is_one_bounded_summary_across_workers_and_batch_limit() -> None:
    project_id = uuid4()
    runs = [make_run(project_id, f"output-{index}") for index in range(25)]
    rule = AlertRule(4, project_id, "daily", 0.1, ActionType.DIGEST, "mailto:ops@example.com")
    config = make_config(delivery_batch_size=2)
    repository = MemoryRepository(runs, [rule])
    valkey = MemoryValkey(repository.events)
    webhook = RecordingWebhook()
    worker1, *_ = build_worker(
        runs,
        [rule],
        config=config,
        repository=repository,
        valkey=valkey,
        webhook=webhook,
    )
    worker2, *_ = build_worker(
        runs,
        [rule],
        config=config,
        repository=repository,
        valkey=valkey,
        webhook=webhook,
    )
    for run in runs:
        await worker1.process_job(job(run.id))

    claimed = await asyncio.gather(
        worker1.deliver_due_once(),
        worker2.deliver_due_once(),
    )
    assert sorted(claimed) == [0, 25]
    assert len(webhook.digest_calls) == 1
    summary = webhook.digest_calls[0]
    assert summary["total_count"] == 25
    assert len(summary["items"]) == 20
    assert summary["digest_day"] == "2026-08-09"
    assert summary["idempotency_key"]
    assert all(alert.route_status is RouteStatus.DELIVERED for alert in repository.alerts.values())


@pytest.mark.asyncio
async def test_digest_receipt_prevents_resend_after_db_commit_crash() -> None:
    project_id = uuid4()
    runs = [make_run(project_id, "digest-one"), make_run(project_id, "digest-two")]
    rule = AlertRule(
        4,
        project_id,
        "daily",
        0.1,
        ActionType.DIGEST,
        "mailto:ops@example.com",
    )

    class FailFirstDigestCommit(MemoryRepository):
        def __init__(self):
            super().__init__(runs, [rule])
            self.fail_commit = True

        async def mark_digest_delivered(self, lease_token):
            if self.fail_commit:
                self.fail_commit = False
                raise ConnectionError("database commit unavailable")
            await super().mark_digest_delivered(lease_token)

    repository = FailFirstDigestCommit()
    worker, _repo, valkey, _embedder, _store, webhook = build_worker(
        runs,
        [rule],
        repository=repository,
    )
    for run in runs:
        await worker.process_job(job(run.id))

    with pytest.raises(ConnectionError, match="database commit"):
        await worker.deliver_due_once()
    assert len(webhook.digest_calls) == 1
    old_token = next(iter(repository.alerts.values())).delivery_lease_token
    assert old_token is not None
    await repository.release_digest_claim(old_token)

    await worker.deliver_due_once()

    assert len(webhook.digest_calls) == 1
    assert all(alert.route_status is RouteStatus.DELIVERED for alert in repository.alerts.values())
    receipts = [call for call in valkey.set_calls if "digest-receipt" in call[0]]
    assert len(receipts) == 1
    assert all(call[2] == {"ex": 86400} for call in receipts)


@pytest.mark.asyncio
async def test_compute_latency_uses_monotonic_evaluation_time() -> None:
    project_id = uuid4()
    run = replace(make_run(project_id, "output"), ingested_at=datetime(2000, 1, 1, tzinfo=UTC))
    readings = iter([10.0, 10.421])
    worker, _repo, _valkey, _embedder, _store, _webhook = build_worker(
        [run],
        [],
        monotonic=lambda: next(readings),
    )

    result = await worker.process_job(job(run.id))

    assert result.evaluation.evaluation_latency_ms == 421


@pytest.mark.asyncio
async def test_no_active_baseline_set_skips_query_and_persists_nullable_distance() -> None:
    project_id = uuid4()
    run = replace(make_run(project_id, "output"), active_baseline_set=None)
    worker, repository, _valkey, _embedder, store, _webhook = build_worker([run], [])

    result = await worker.process_job(job(run.id))

    assert store.queries == []
    assert result.evaluation.drift_distance is None
    assert store.upserts[0][1]["baseline_set"] is None
    assert repository.alerts == {}


@pytest.mark.asyncio
async def test_terminal_run_is_not_failed_when_dead_letter_persistence_fails() -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    embedder = RecordingEmbedder(error=RuntimeError("model failed"))
    config = make_config(max_job_attempts=1)
    worker, repository, valkey, *_ = build_worker(
        [run],
        [],
        config=config,
        embedder=embedder,
    )
    valkey.fail_lpush_keys.add(config.dead_letter_queue)
    payload = json.dumps({"event_id": str(uuid4()), "run_id": str(run.id)})

    with pytest.raises(ConnectionError):
        await worker.handle_queue_payload(payload)

    assert repository.run_status[run.id] == "processing"
    assert "mark_failed" not in repository.events


@pytest.mark.asyncio
async def test_terminal_dead_letter_preserves_event_identity_before_failed_state() -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    event_id = uuid4()
    embedder = RecordingEmbedder(error=RuntimeError("terminal"))
    config = make_config(max_job_attempts=1)
    worker, repository, valkey, *_ = build_worker(
        [run],
        [],
        config=config,
        embedder=embedder,
    )

    await worker.handle_queue_payload(
        json.dumps({"event_id": str(event_id), "run_id": str(run.id)})
    )

    dead_letter = json.loads(valkey.lpush_calls[-1][1])
    assert dead_letter["event_id"] == str(event_id)
    assert dead_letter["run_id"] == str(run.id)
    assert repository.events[-2:] == ["lpush:drift_eval_dead_letter", "mark_failed"]
    assert repository.run_status[run.id] == "failed"


@pytest.mark.asyncio
async def test_retry_preserves_event_identity_and_enqueues_before_status() -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    event_id = uuid4()
    embedder = RecordingEmbedder(error=RuntimeError("temporary"))
    worker, repository, valkey, *_ = build_worker([run], [], embedder=embedder)

    async def no_wait(_delay):
        return None

    worker._wait_or_stop = no_wait
    await worker.handle_queue_payload(
        json.dumps({"event_id": str(event_id), "run_id": str(run.id)})
    )

    retry = json.loads(valkey.lpush_calls[-1][1])
    assert retry == {
        "event_id": str(event_id),
        "run_id": str(run.id),
        "attempt": 2,
    }
    assert repository.events[-2:] == ["lpush:drift_eval_queue", "mark_queued"]
    assert repository.run_status[run.id] == "queued"


@pytest.mark.asyncio
async def test_open_qdrant_circuit_requeues_without_consuming_attempt() -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    event_id = uuid4()
    worker, repository, valkey, *_ = build_worker([run], [])

    async def open_circuit(*_args, **_kwargs):
        raise CircuitOpenError("open")

    async def no_wait(_delay):
        return None

    worker.vector_store.nearest_baseline = open_circuit
    worker._wait_or_stop = no_wait
    await worker.handle_queue_payload(
        json.dumps(
            {
                "event_id": str(event_id),
                "run_id": str(run.id),
                "attempt": 3,
            }
        )
    )

    replay = json.loads(valkey.lpush_calls[-1][1])
    assert replay["attempt"] == 3
    assert replay["event_id"] == str(event_id)
    assert repository.run_status[run.id] == "queued"
    assert "mark_failed" not in repository.events


def test_queue_contract_requires_event_and_run_identity() -> None:
    run_id = uuid4()
    event_id = uuid4()
    parsed = DriftWorker.parse_job(
        json.dumps(
            {
                "event_id": str(event_id),
                "run_id": str(run_id),
                "project_id": str(uuid4()),
            }
        )
    )

    assert parsed == Job(run_id=run_id, event_id=event_id, attempt=1)
    with pytest.raises(MalformedJobError):
        DriftWorker.parse_job(json.dumps({"run_id": str(run_id)}))
    with pytest.raises(MalformedJobError):
        DriftWorker.parse_job(json.dumps({"event_id": str(event_id)}))


def test_safe_text_bound_and_distance_math() -> None:
    assert DriftWorker.prepare_text("  caf\u0065\u0301  ", 2048) == "caf\u00e9"
    assert len(DriftWorker.prepare_text("x" * 4096, 2048)) == 2048
    with pytest.raises(EmptyOutputError):
        DriftWorker.prepare_text("   ")
    assert DriftWorker.cosine_distance(0.7) == pytest.approx(0.3)


@pytest.mark.asyncio
async def test_blpop_zero_is_interruptible_for_shutdown() -> None:
    class BlockingValkey(MemoryValkey):
        def __init__(self):
            super().__init__()
            self.calls = []
            self.blocked = asyncio.Event()

        async def blpop(self, queue, **options):
            self.calls.append((queue, options["timeout"]))
            self.blocked.set()
            await asyncio.Event().wait()

    project_id = uuid4()
    run = make_run(project_id, "output")
    valkey = BlockingValkey()
    worker, *_ = build_worker([run], [], valkey=valkey)

    pop = asyncio.create_task(worker._interruptible_blpop())
    await valkey.blocked.wait()
    worker.request_shutdown()

    assert await asyncio.wait_for(pop, timeout=1) is None
    assert valkey.calls == [("drift_eval_queue", 0)]


@pytest.mark.asyncio
async def test_consumer_never_exceeds_configured_in_flight_bound() -> None:
    class QueueValkey(MemoryValkey):
        def __init__(self, payloads):
            super().__init__()
            self.payloads = list(payloads)

        async def blpop(self, queue, **options):
            assert options["timeout"] == 0
            if self.payloads:
                return queue, self.payloads.pop(0)
            await asyncio.Event().wait()

    project_id = uuid4()
    runs = [make_run(project_id, f"output-{index}") for index in range(7)]
    payloads = [json.dumps({"event_id": str(uuid4()), "run_id": str(run.id)}) for run in runs]
    valkey = QueueValkey(payloads)
    config = make_config(worker_concurrency=3)
    worker, *_ = build_worker(runs, [], config=config, valkey=valkey)
    active = 0
    peak = 0
    completed = 0
    filled = asyncio.Event()
    release = asyncio.Event()

    async def bounded_handler(_payload):
        nonlocal active, peak, completed
        active += 1
        peak = max(peak, active)
        if active == config.worker_concurrency:
            filled.set()
        await release.wait()
        active -= 1
        completed += 1
        if completed == len(runs):
            worker.request_shutdown()

    worker._handle_queue_payload_guarded = bounded_handler

    run_task = asyncio.create_task(worker.run())
    await asyncio.wait_for(filled.wait(), timeout=1)
    assert active == config.worker_concurrency
    release.set()
    await asyncio.wait_for(run_task, timeout=2)

    assert completed == len(runs)
    assert peak == 3


@pytest.mark.asyncio
async def test_heartbeat_refreshes_readiness_for_bounded_active_work(
    monkeypatch,
) -> None:
    project_id = uuid4()
    run = make_run(project_id, "output")
    config = make_config(heartbeat_interval_seconds=1, job_health_timeout_seconds=60)
    worker, _repo, valkey, *_ = build_worker([run], [], config=config)
    refreshes = []
    monkeypatch.setattr(
        "app_worker.worker.refresh_readiness_marker",
        lambda: refreshes.append(True),
    )
    worker._active_job_count = 1
    worker._active_job_started[1] = __import__("time").monotonic() - 10

    heartbeat_task = asyncio.create_task(worker._heartbeat_loop())
    for _ in range(20):
        if refreshes:
            break
        await asyncio.sleep(0)
    worker.request_shutdown()
    await heartbeat_task

    assert refreshes == [True]
    body = json.loads(
        next(
            value
            for key, value, _kwargs in valkey.set_calls
            if key == "driftguard:worker:heartbeat"
        )
    )
    assert body["active_jobs"] == 1
    assert body["concurrency_limit"] == 4
    assert 9 <= body["oldest_job_age_seconds"] <= 11


@pytest.mark.asyncio
async def test_heartbeat_does_not_mask_a_job_past_health_timeout(monkeypatch) -> None:
    run = make_run(uuid4(), "output")
    config = make_config(heartbeat_interval_seconds=1, job_health_timeout_seconds=60)
    worker, _repo, valkey, *_ = build_worker([run], [], config=config)
    refreshes = []
    monkeypatch.setattr(
        "app_worker.worker.refresh_readiness_marker",
        lambda: refreshes.append(True),
    )
    worker._active_job_count = 1
    worker._active_job_started[1] = __import__("time").monotonic() - 61

    heartbeat_task = asyncio.create_task(worker._heartbeat_loop())
    for _ in range(20):
        if len(valkey.set_calls) >= 2:
            break
        await asyncio.sleep(0)
    worker.request_shutdown()
    await heartbeat_task

    assert refreshes == []
