"""Long-lived DriftGuard queue consumer and semantic evaluation orchestrator."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import socket
import threading
import time
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from redis.asyncio import Redis

from .circuit_breaker import CircuitOpenError
from .config import WorkerConfig
from .domain import (
    ActionType,
    DeliveryItem,
    DigestDelivery,
    Job,
    ProcessingResult,
)
from .embedding import SentenceTransformerEmbedder
from .notifications import WebhookSender
from .readiness import refresh_readiness_marker
from .repository import PostgresRepository
from .retention import RetentionManager
from .retry import retry_startup
from .vector_store import QdrantVectorStore

LOGGER = logging.getLogger("driftguard.worker")
UTC = getattr(__import__("datetime"), "UTC", timezone.utc)  # noqa: UP017


class MalformedJobError(ValueError):
    """Raised when a queue message cannot identify a valid telemetry run."""


class RunNotFoundError(LookupError):
    """Raised when the database has no run matching a queued identifier."""


class EmptyOutputError(ValueError):
    """Raised when a telemetry output has no embeddable text."""


class DriftWorker:
    def __init__(
        self,
        *,
        config: WorkerConfig,
        repository: Any,
        valkey: Any,
        vector_store: Any,
        embedder: Any,
        webhook_sender: Any,
        worker_id: str | None = None,
        utc_now: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.perf_counter,
    ) -> None:
        self.config = config
        self.repository = repository
        self.valkey = valkey
        self.vector_store = vector_store
        self.embedder = embedder
        self.webhook_sender = webhook_sender
        self.worker_id = worker_id or self._default_worker_id()
        self.heartbeat_key = f"driftguard:worker:heartbeat:{self.worker_id}"
        self.heartbeat_alias_key = "driftguard:worker:heartbeat"
        self._stop_event = asyncio.Event()
        self._active_runs: Counter[UUID] = Counter()
        self._active_job_count = 0
        self._active_job_started: dict[int, float] = {}
        self._last_completed_at: str | None = None
        self._embedding_cache_hits = 0
        self._embedding_cache_misses = 0
        self._consumer_waiting = False
        self._last_consumer_success = 0.0
        self._utc_now = utc_now or (lambda: datetime.now(UTC))
        self._monotonic = monotonic
        self._last_evaluation_latency_ms: int | None = None
        self._closed = False
        self._retention_manager = RetentionManager(
            config=config,
            repository=repository,
            vector_store=vector_store,
            utc_now=self._utc_now,
        )

    @classmethod
    async def create(cls, config: WorkerConfig) -> DriftWorker:
        repository: PostgresRepository | None = None
        valkey: Redis | None = None
        vector_store: QdrantVectorStore | None = None
        webhook_sender: WebhookSender | None = None
        try:
            dependency_results = await asyncio.gather(
                retry_startup(
                    "PostgreSQL",
                    lambda: PostgresRepository.connect(
                        config.database_url,
                        max_size=config.db_pool_max_size,
                    ),
                ),
                retry_startup(
                    "Valkey",
                    lambda: cls._connect_valkey(config),
                ),
                retry_startup(
                    "Qdrant",
                    lambda: QdrantVectorStore.connect(
                        url=config.qdrant_url,
                        api_key=config.qdrant_api_key,
                        collection=config.qdrant_collection,
                        dimension=config.embedding_dimension,
                        circuit_failure_threshold=(config.qdrant_circuit_failure_threshold),
                        circuit_reset_seconds=config.qdrant_circuit_reset_seconds,
                    ),
                ),
                return_exceptions=True,
            )
            if not isinstance(dependency_results[0], BaseException):
                repository = dependency_results[0]
            if not isinstance(dependency_results[1], BaseException):
                valkey = dependency_results[1]
            if not isinstance(dependency_results[2], BaseException):
                vector_store = dependency_results[2]
            failure = next(
                (result for result in dependency_results if isinstance(result, BaseException)),
                None,
            )
            if failure is not None:
                raise failure

            embedder = await SentenceTransformerEmbedder.load(
                config.embedding_model,
                dimension=config.embedding_dimension,
                local_files_only=True,
            )
            await embedder.embed("DriftGuard semantic evaluation readiness probe.")
            webhook_sender = WebhookSender(
                timeout_seconds=config.webhook_timeout_seconds,
                max_attempts=config.webhook_max_attempts,
                allowed_hosts=config.webhook_allowed_hosts,
                smtp_host=config.smtp_host,
                smtp_port=config.smtp_port,
                smtp_username=config.smtp_username,
                smtp_password=config.smtp_password,
                smtp_from_address=config.smtp_from_address,
                smtp_security=config.smtp_security,
                smtp_timeout_seconds=config.smtp_timeout_seconds,
            )
            return cls(
                config=config,
                repository=repository,
                valkey=valkey,
                vector_store=vector_store,
                embedder=embedder,
                webhook_sender=webhook_sender,
            )
        except BaseException:
            closers = []
            if webhook_sender is not None:
                closers.append(webhook_sender.close())
            if vector_store is not None:
                closers.append(vector_store.close())
            if valkey is not None:
                closers.append(valkey.aclose())
            if repository is not None:
                closers.append(repository.close())
            if closers:
                await asyncio.gather(*closers, return_exceptions=True)
            raise

    @staticmethod
    async def _connect_valkey(config: WorkerConfig) -> Redis:
        client = Redis(
            host=config.valkey_host,
            port=config.valkey_port,
            password=config.valkey_password,
            decode_responses=True,
            socket_connect_timeout=5.0,
            socket_timeout=None,
            health_check_interval=15,
        )
        try:
            await client.ping()
        except Exception:
            await client.aclose()
            raise
        return client

    async def run(self) -> None:
        """Consume ``drift_eval_queue`` until shutdown is requested."""

        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="driftguard-worker-heartbeat"
        )
        delivery_task = asyncio.create_task(
            self._delivery_loop(), name="driftguard-worker-delivery"
        )
        retention_task = (
            asyncio.create_task(
                self._retention_loop(),
                name="driftguard-worker-retention",
            )
            if self.config.retention_enabled
            else None
        )
        in_flight: set[asyncio.Task[None]] = set()
        queue_failures = 0
        LOGGER.info("worker %s is consuming %s", self.worker_id, self.config.queue_name)
        try:
            while not self._stop_event.is_set():
                if len(in_flight) >= self.config.worker_concurrency:
                    completed, _pending = await asyncio.wait(
                        in_flight,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    in_flight.difference_update(completed)
                    await asyncio.gather(*completed, return_exceptions=True)
                    continue
                try:
                    item = await self._interruptible_blpop()
                    if item is None:
                        break
                    self._last_consumer_success = time.monotonic()
                    queue_failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    queue_failures += 1
                    await self._log_failure_once(None, exc, context="queue-consume")
                    await self._wait_or_stop(min(30.0, float(2 ** min(queue_failures, 5))))
                    continue

                raw_payload = item[1]
                task = asyncio.create_task(
                    self._handle_queue_payload_guarded(raw_payload),
                    name="driftguard-worker-job",
                )
                in_flight.add(task)
        finally:
            self._stop_event.set()
            if in_flight:
                await asyncio.gather(*in_flight, return_exceptions=True)
            heartbeat_task.cancel()
            delivery_task.cancel()
            if retention_task is not None:
                retention_task.cancel()
            await asyncio.gather(
                heartbeat_task,
                delivery_task,
                *([retention_task] if retention_task is not None else []),
                return_exceptions=True,
            )
            await self._remove_heartbeat()

    async def _handle_queue_payload_guarded(self, raw_payload: str | bytes) -> None:
        try:
            await self.handle_queue_payload(raw_payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._log_failure_once(None, exc, context="unhandled-queue-item")

    async def _interruptible_blpop(self) -> Any | None:
        """Race canonical ``BLPOP queue 0`` against graceful shutdown."""

        pop_task = asyncio.create_task(
            self.valkey.blpop(self.config.queue_name, timeout=0),
            name="driftguard-worker-blpop",
        )
        stop_task = asyncio.create_task(
            self._stop_event.wait(),
            name="driftguard-worker-stop-wait",
        )
        self._consumer_waiting = True
        try:
            done, _pending = await asyncio.wait(
                {pop_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if pop_task in done:
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
                return await pop_task
            pop_task.cancel()
            await asyncio.gather(pop_task, return_exceptions=True)
            return None
        finally:
            self._consumer_waiting = False
            for task in (pop_task, stop_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(pop_task, stop_task, return_exceptions=True)

    async def handle_queue_payload(self, raw_payload: str | bytes) -> None:
        """Process one queue item, retry transient failures, and dead-letter final failures."""

        try:
            job = self.parse_job(raw_payload)
        except Exception as exc:
            await self._log_failure_once(None, exc, context="malformed-job")
            await self._dead_letter(None, None, 1, exc, raw_payload)
            return

        self._active_runs[job.run_id] += 1
        self._active_job_count += 1
        task = asyncio.current_task()
        task_key = id(task) if task is not None else id(job)
        self._active_job_started[task_key] = time.monotonic()
        try:
            result = await self.process_job(job)
            if result is not None:
                self._last_evaluation_latency_ms = result.evaluation.evaluation_latency_ms
                self._last_completed_at = self._utc_now().isoformat()
        except asyncio.CancelledError:
            raise
        except CircuitOpenError as exc:
            await self._log_failure_once(job.run_id, exc, context="qdrant-backpressure")
            await self._retry_job_safely(
                job,
                increment_attempt=False,
                delay_seconds=self.config.qdrant_circuit_reset_seconds,
            )
        except (RunNotFoundError, EmptyOutputError, MalformedJobError) as exc:
            await self._log_failure_once(job.run_id, exc, context="permanent-processing")
            await self._dead_letter(
                job.run_id,
                job.event_id,
                job.attempt,
                exc,
                raw_payload,
            )
            if not isinstance(exc, RunNotFoundError):
                await self._mark_failed_safely(job.run_id)
        except Exception as exc:
            await self._log_failure_once(job.run_id, exc, context="processing")
            if job.attempt < self.config.max_job_attempts:
                await self._retry_job_safely(job)
            else:
                await self._dead_letter(
                    job.run_id,
                    job.event_id,
                    job.attempt,
                    exc,
                    raw_payload,
                )
                await self._mark_failed_safely(job.run_id)
        finally:
            self._active_job_count -= 1
            self._active_job_started.pop(task_key, None)
            self._active_runs[job.run_id] -= 1
            if self._active_runs[job.run_id] <= 0:
                del self._active_runs[job.run_id]

    async def process_job(self, job: Job) -> ProcessingResult | None:
        """Evaluate one run only while this session owns its advisory lock."""

        async with self.repository.run_processing_lock(job.run_id) as acquired:
            if not acquired:
                return None
            return await self._process_job_locked(job)

    async def _process_job_locked(self, job: Job) -> ProcessingResult:
        """Evaluate a locked run using database-authoritative project and output data."""

        run, existing_evaluation = await self.repository.claim_run(job.run_id)
        if run is None:
            raise RunNotFoundError(f"telemetry run {job.run_id} does not exist")

        if existing_evaluation is not None:
            return ProcessingResult(
                evaluation=existing_evaluation,
                created=False,
            )
        evaluation_started = self._monotonic()

        text = self.prepare_text(run.output_text, self.config.max_text_characters)
        embedding = await self._embedding_for_text(text, run.project_id)
        if len(embedding) != self.config.embedding_dimension:
            raise ValueError(
                f"embedding has {len(embedding)} dimensions, "
                f"expected {self.config.embedding_dimension}"
            )
        match = None
        if run.active_baseline_set is not None:
            match = await self.vector_store.nearest_baseline(
                embedding,
                run.project_id,
                run.active_baseline_set,
                self.config.embedding_model_revision,
            )
        if match is None:
            drift_distance = None
            matched_baseline_id = None
        else:
            drift_distance = self.cosine_distance(match.similarity)
            matched_baseline_id = match.id

        await self.vector_store.upsert_evaluation(
            embedding,
            run_id=run.id,
            project_id=run.project_id,
            drift_distance=drift_distance,
            matched_baseline_id=matched_baseline_id,
            baseline_set=run.active_baseline_set,
            embedding_model_revision=self.config.embedding_model_revision,
        )

        latency_ms = min(
            2_147_483_647,
            max(0, round((self._monotonic() - evaluation_started) * 1000.0)),
        )
        evaluation, created, _alerts = await self.repository.persist_evaluation_and_alerts(
            run=run,
            drift_distance=drift_distance,
            matched_baseline_id=matched_baseline_id,
            evaluation_latency_ms=latency_ms,
        )
        return ProcessingResult(
            evaluation=evaluation,
            created=created,
        )

    async def _embedding_for_text(self, text: str, project_id: UUID) -> list[float]:
        """Reuse tenant-scoped normalized-output vectors without dropping run rows."""

        text_digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        revision_digest = hashlib.sha256(
            self.config.embedding_model_revision.encode("utf-8")
        ).hexdigest()[:16]
        cache_key = f"driftguard:embedding:{project_id}:{revision_digest}:{text_digest}"
        try:
            cached = await self.valkey.get(cache_key)
            if cached is not None:
                if isinstance(cached, bytes):
                    cached = cached.decode("utf-8")
                payload = json.loads(cached)
                vector = payload.get("vector") if isinstance(payload, dict) else None
                revision = payload.get("revision") if isinstance(payload, dict) else None
                if (
                    revision == self.config.embedding_model_revision
                    and isinstance(vector, list)
                    and len(vector) == self.config.embedding_dimension
                    and all(
                        not isinstance(value, bool)
                        and isinstance(value, (int, float))
                        and math.isfinite(float(value))
                        for value in vector
                    )
                ):
                    self._embedding_cache_hits += 1
                    return [float(value) for value in vector]
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            LOGGER.debug("embedding cache record rejected: %s", type(exc).__name__)
        except Exception as exc:
            LOGGER.debug("embedding cache read unavailable: %s", type(exc).__name__)

        self._embedding_cache_misses += 1
        embedding = await self.embedder.embed(text)
        try:
            await self.valkey.set(
                cache_key,
                json.dumps(
                    {
                        "revision": self.config.embedding_model_revision,
                        "vector": embedding,
                    },
                    separators=(",", ":"),
                ),
                ex=self.config.embedding_cache_ttl_seconds,
            )
        except Exception as exc:
            LOGGER.debug("embedding cache write unavailable: %s", type(exc).__name__)
        return embedding

    def _failure_mode_digest(self, item: DeliveryItem) -> str:
        normalized_output = self.prepare_text(
            item.run.output_text,
            self.config.max_text_characters,
        )
        output_digest = hashlib.sha256(normalized_output.encode("utf-8")).hexdigest()
        failure_mode = (
            f"{item.run.project_id}:{item.alert.rule.id}:"
            f"{item.evaluation.matched_baseline_id or 'unmatched'}:{output_digest}"
        )
        return hashlib.sha256(failure_mode.encode("utf-8")).hexdigest()

    @staticmethod
    def _delivery_lease_token(items: list[DeliveryItem]) -> UUID:
        tokens = {item.alert.delivery_lease_token for item in items}
        if len(tokens) != 1 or None in tokens:
            raise RuntimeError("delivery batch has no unique PostgreSQL lease token")
        token = next(iter(tokens))
        if token is None:
            raise RuntimeError("delivery batch lease token is null")
        return token

    async def _release_route_lock(self, key: str, owner: str) -> None:
        script = (
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
            "return redis.call('del', KEYS[1]) else return 0 end"
        )
        try:
            await self.valkey.eval(script, 1, key, owner)
        except Exception as exc:
            LOGGER.debug("alert route lock release failed: %s", type(exc).__name__)

    async def _recover_delivery_receipt(
        self,
        *,
        key: str,
        raw_receipt: str | bytes,
        items: list[DeliveryItem],
    ) -> list[DeliveryItem]:
        """Recover success after a crash between webhook 2xx and PostgreSQL commit."""

        if isinstance(raw_receipt, bytes):
            try:
                raw_receipt = raw_receipt.decode("ascii")
            except UnicodeDecodeError:
                await self.valkey.delete(key)
                return items
        try:
            payload = json.loads(raw_receipt)
            delivered_alert_id = UUID(payload["leader_id"])
            receipted_ids = {UUID(value) for value in payload["alert_ids"]}
            if (
                not isinstance(payload, dict)
                or not receipted_ids
                or len(receipted_ids) > self.config.delivery_batch_size
                or delivered_alert_id not in receipted_ids
            ):
                raise ValueError("invalid receipt shape")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            await self.valkey.delete(key)
            return items
        matched_ids = [item.alert.id for item in items if item.alert.id in receipted_ids]
        if not matched_ids:
            return items
        lease_token = self._delivery_lease_token(items)
        if delivered_alert_id in matched_ids:
            await self.repository.mark_delivered([delivered_alert_id], lease_token)
        await self.repository.mark_suppressed(
            [alert_id for alert_id in matched_ids if alert_id != delivered_alert_id],
            lease_token,
        )
        return [item for item in items if item.alert.id not in receipted_ids]

    async def _has_recent_failure_mode_receipt(self, key: str) -> bool:
        raw_receipt = await self.valkey.get(key)
        if raw_receipt is None:
            return False
        if isinstance(raw_receipt, bytes):
            try:
                raw_receipt = raw_receipt.decode("ascii")
            except UnicodeDecodeError:
                await self.valkey.delete(key)
                return False
        try:
            UUID(raw_receipt)
        except (TypeError, ValueError):
            await self.valkey.delete(key)
            return False
        return True

    @staticmethod
    def _digest_group_id(digest: DigestDelivery) -> str:
        identity = f"{digest.project_id}:{digest.rule.id}:{digest.digest_day.isoformat()}"
        return hashlib.sha256(identity.encode("ascii")).hexdigest()

    async def _recover_digest_receipt(
        self,
        *,
        digest: DigestDelivery,
        key: str,
        raw_receipt: str | bytes,
    ) -> bool:
        if isinstance(raw_receipt, bytes):
            try:
                raw_receipt = raw_receipt.decode("ascii")
            except UnicodeDecodeError:
                await self.valkey.delete(key)
                return False
        if raw_receipt != self._digest_group_id(digest):
            await self.valkey.delete(key)
            return False
        await self.repository.mark_digest_delivered(digest.lease_token)
        return True

    async def deliver_due_once(self) -> int:
        batch = await self.repository.claim_due_deliveries(
            limit=self.config.delivery_batch_size,
            lease_seconds=self.config.delivery_lease_seconds,
        )
        if batch.claimed_count == 0:
            return 0

        notify_groups: dict[str, list[DeliveryItem]] = defaultdict(list)
        operations: list[Callable[[], Awaitable[None]]] = []
        for item in batch.items:
            action = item.alert.rule.action_type
            if action is ActionType.NOTIFY:
                notify_groups[self._failure_mode_digest(item)].append(item)
            else:
                operations.append(
                    lambda item=item: self.repository.mark_suppressed(
                        [item.alert.id],
                        self._delivery_lease_token([item]),
                    )
                )
        operations.extend(
            lambda group=group: self._deliver_notify_group(group)
            for group in notify_groups.values()
        )
        if batch.digest is not None:
            operations.append(lambda: self._deliver_digest(batch.digest))

        semaphore = asyncio.Semaphore(self.config.worker_concurrency)

        async def bounded(operation: Callable[[], Awaitable[None]]) -> None:
            async with semaphore:
                await operation()

        await asyncio.gather(*(bounded(operation) for operation in operations))
        return batch.claimed_count

    async def _deliver_notify_group(self, items: list[DeliveryItem]) -> None:
        """Deliver one failure mode and suppress followers only after confirmed 2xx."""

        if not items:
            return
        ordered = sorted(items, key=lambda item: str(item.alert.id))
        lease_token = self._delivery_lease_token(ordered)
        digest = self._failure_mode_digest(ordered[0])
        dedupe_key = f"driftguard:alert-dedupe:{digest}"
        receipt_key = f"driftguard:delivery-receipt:{digest}"
        lock_key = f"{dedupe_key}:inflight"
        lock_owner = f"{self.worker_id}:{ordered[0].alert.id}"
        lock_acquired = False
        alert_ids = [item.alert.id for item in ordered]

        try:
            receipt = await self.valkey.get(receipt_key)
            if receipt is not None:
                ordered = await self._recover_delivery_receipt(
                    key=receipt_key,
                    raw_receipt=receipt,
                    items=ordered,
                )
            if not ordered:
                return
            alert_ids = [item.alert.id for item in ordered]
            if await self._has_recent_failure_mode_receipt(dedupe_key):
                await self.repository.mark_suppressed(alert_ids, lease_token)
                return
            leader = ordered[0]
            lock_owner = f"{self.worker_id}:{leader.alert.id}"
            acquired = await self.valkey.set(
                lock_key,
                lock_owner,
                ex=self.config.delivery_lease_seconds,
                nx=True,
            )
            if not acquired:
                await self.repository.release_delivery_claim(
                    alert_ids,
                    lease_token=lease_token,
                )
                return
            lock_acquired = True
            receipt = await self.valkey.get(receipt_key)
            if receipt is not None:
                ordered = await self._recover_delivery_receipt(
                    key=receipt_key,
                    raw_receipt=receipt,
                    items=ordered,
                )
            if not ordered:
                return
            alert_ids = [item.alert.id for item in ordered]
            if await self._has_recent_failure_mode_receipt(dedupe_key):
                await self.repository.mark_suppressed(alert_ids, lease_token)
                return
            leader = ordered[0]
            attempt_started = await self.repository.start_delivery_attempt(
                alert_ids,
                lease_token,
            )
            if not attempt_started:
                await self.repository.release_delivery_claim(
                    alert_ids,
                    lease_token=lease_token,
                )
                return
            await self.webhook_sender.send(
                alert=leader.alert,
                evaluation=leader.evaluation,
                run=leader.run,
                idempotency_key=str(leader.alert.id),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.repository.record_delivery_failure(
                alert_ids,
                lease_token=lease_token,
                error_type=type(exc).__name__,
                max_attempts=self.config.delivery_max_attempts,
            )
        else:
            follower_ids = [item.alert.id for item in ordered[1:]]
            receipt_payload = json.dumps(
                {
                    "leader_id": str(leader.alert.id),
                    "alert_ids": [str(item.alert.id) for item in ordered],
                },
                separators=(",", ":"),
            )
            try:
                await self.valkey.set(
                    receipt_key,
                    receipt_payload,
                    ex=self.config.delivery_receipt_ttl_seconds,
                )
                await self.valkey.set(
                    dedupe_key,
                    str(leader.alert.id),
                    ex=self.config.failure_dedupe_ttl_seconds,
                )
            except Exception as exc:
                LOGGER.warning(
                    "alert delivery succeeded but receipt persistence failed (%s)",
                    type(exc).__name__,
                )
            await self.repository.mark_delivered([leader.alert.id], lease_token)
            await self.repository.mark_suppressed(follower_ids, lease_token)
        finally:
            if lock_acquired:
                await self._release_route_lock(lock_key, lock_owner)

    async def _deliver_digest(self, digest: DigestDelivery) -> None:
        group_id = self._digest_group_id(digest)
        receipt_key = f"driftguard:digest-receipt:{group_id}"
        lock_key = f"driftguard:digest-inflight:{group_id}"
        lock_owner = f"{self.worker_id}:{group_id}"
        lock_acquired = False
        try:
            receipt = await self.valkey.get(receipt_key)
            if receipt is not None and await self._recover_digest_receipt(
                digest=digest,
                key=receipt_key,
                raw_receipt=receipt,
            ):
                return
            acquired = await self.valkey.set(
                lock_key,
                lock_owner,
                ex=self.config.delivery_lease_seconds,
                nx=True,
            )
            if not acquired:
                await self.repository.release_digest_claim(digest.lease_token)
                return
            lock_acquired = True
            receipt = await self.valkey.get(receipt_key)
            if receipt is not None and await self._recover_digest_receipt(
                digest=digest,
                key=receipt_key,
                raw_receipt=receipt,
            ):
                return
            attempt_started = await self.repository.start_digest_delivery_attempt(
                digest.lease_token,
                expected_count=digest.total_count,
            )
            if not attempt_started:
                await self.repository.release_digest_claim(digest.lease_token)
                return
            await self.webhook_sender.send_digest(
                list(digest.evidence),
                total_count=digest.total_count,
                digest_day=digest.digest_day.isoformat(),
                idempotency_key=group_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.repository.record_digest_delivery_failure(
                digest.lease_token,
                error_type=type(exc).__name__,
                max_attempts=self.config.delivery_max_attempts,
            )
        else:
            try:
                await self.valkey.set(
                    receipt_key,
                    group_id,
                    ex=self.config.delivery_receipt_ttl_seconds,
                )
            except Exception as exc:
                LOGGER.warning(
                    "digest delivery succeeded but receipt persistence failed (%s)",
                    type(exc).__name__,
                )
            await self.repository.mark_digest_delivered(digest.lease_token)
        finally:
            if lock_acquired:
                await self._release_route_lock(lock_key, lock_owner)

    async def _retention_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                result = await self._retention_manager.run_once()
                if result.lock_acquired and any(
                    (
                        result.redacted_runs,
                        result.purged_outbox_events,
                        result.expired_runs,
                        result.deleted_vectors,
                        result.purged_vector_receipts,
                    )
                ):
                    LOGGER.info(
                        "retention redacted=%d outbox=%d expired=%d vectors=%d receipts=%d",
                        result.redacted_runs,
                        result.purged_outbox_events,
                        result.expired_runs,
                        result.deleted_vectors,
                        result.purged_vector_receipts,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._log_failure_once(None, exc, context="retention")
            await self._wait_or_stop(float(self.config.retention_interval_seconds))

    async def _delivery_loop(self) -> None:
        failure_count = 0
        while not self._stop_event.is_set():
            try:
                delivered = await self.deliver_due_once()
                failure_count = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failure_count += 1
                await self._log_failure_once(None, exc, context="alert-delivery")
                await self._wait_or_stop(min(30.0, float(2 ** min(failure_count, 5))))
                continue
            if delivered >= self.config.delivery_batch_size:
                await asyncio.sleep(0)
            else:
                await self._wait_or_stop(self.config.delivery_poll_interval_seconds)

    async def _retry_job(
        self,
        job: Job,
        *,
        increment_attempt: bool = True,
        delay_seconds: float | None = None,
    ) -> None:
        delay = (
            min(30.0, float(2 ** max(0, job.attempt - 1)))
            if delay_seconds is None
            else delay_seconds
        )
        await self._wait_or_stop(delay)
        if self._stop_event.is_set():
            return
        payload = json.dumps(
            {
                "event_id": str(job.event_id),
                "run_id": str(job.run_id),
                "attempt": job.attempt + 1 if increment_attempt else job.attempt,
            },
            separators=(",", ":"),
        )
        await self.valkey.lpush(self.config.queue_name, payload)
        await self.repository.mark_run_queued(job.run_id)

    async def _retry_job_safely(
        self,
        job: Job,
        *,
        increment_attempt: bool = True,
        delay_seconds: float | None = None,
    ) -> None:
        try:
            await self._retry_job(
                job,
                increment_attempt=increment_attempt,
                delay_seconds=delay_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The run remains processing, so API stale-run recovery can republish it.
            await self._log_failure_once(job.run_id, exc, context="retry-enqueue")

    async def _dead_letter(
        self,
        run_id: UUID | None,
        event_id: UUID | None,
        attempt: int,
        error: Exception,
        raw_payload: str | bytes,
    ) -> None:
        raw = (
            raw_payload.decode("utf-8", errors="replace")
            if isinstance(raw_payload, bytes)
            else raw_payload
        )
        record = {
            "run_id": str(run_id) if run_id is not None else None,
            "event_id": str(event_id) if event_id is not None else None,
            "attempt": attempt,
            "failure_type": type(error).__name__,
            "failure_fingerprint": hashlib.sha256(
                f"{type(error).__name__}:{error}".encode("utf-8", errors="replace")
            ).hexdigest(),
            "payload_fingerprint": hashlib.sha256(
                raw.encode("utf-8", errors="replace")
            ).hexdigest(),
            "failed_at": datetime.now(UTC).isoformat(),
        }
        await self.valkey.lpush(
            self.config.dead_letter_queue,
            json.dumps(record, separators=(",", ":")),
        )

    async def _mark_failed_safely(self, run_id: UUID) -> None:
        try:
            await self.repository.mark_run_failed(run_id)
        except Exception as exc:
            LOGGER.error("could not mark run %s failed: %s", run_id, type(exc).__name__)

    async def _log_failure_once(
        self,
        run_id: UUID | None,
        error: Exception,
        *,
        context: str,
    ) -> None:
        fingerprint = hashlib.sha256(
            f"{context}:{type(error).__name__}:{error}".encode("utf-8", errors="replace")
        ).hexdigest()
        key = f"driftguard:failure-dedupe:{fingerprint}"
        should_log = True
        try:
            should_log = bool(
                await self.valkey.set(
                    key,
                    self.worker_id,
                    ex=self.config.failure_dedupe_ttl_seconds,
                    nx=True,
                )
            )
        except Exception as exc:
            LOGGER.debug("failure dedupe unavailable: %s", type(exc).__name__)
        if should_log:
            LOGGER.error(
                "worker failure context=%s run_id=%s type=%s",
                context,
                run_id,
                type(error).__name__,
            )

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            now_monotonic = time.monotonic()
            oldest_job_age_seconds = (
                max(
                    0.0,
                    now_monotonic - min(self._active_job_started.values()),
                )
                if self._active_job_started
                else None
            )
            heartbeat = {
                "worker_id": self.worker_id,
                "status": "processing" if self._active_job_count else "idle",
                "current_run_id": (
                    str(next(iter(self._active_runs))) if self._active_runs else None
                ),
                "active_jobs": self._active_job_count,
                "concurrency_limit": self.config.worker_concurrency,
                "oldest_job_age_seconds": oldest_job_age_seconds,
                "last_completed_at": self._last_completed_at,
                "last_evaluation_latency_ms": self._last_evaluation_latency_ms,
                "embedding_cache_hits": self._embedding_cache_hits,
                "embedding_cache_misses": self._embedding_cache_misses,
                "active_threads": threading.active_count(),
                "pid": os.getpid(),
                "queue": self.config.queue_name,
                "timestamp": datetime.now(UTC).isoformat(),
            }
            try:
                encoded = json.dumps(heartbeat, separators=(",", ":"))
                await asyncio.gather(
                    self.valkey.set(
                        self.heartbeat_key,
                        encoded,
                        ex=self.config.heartbeat_ttl_seconds,
                    ),
                    self.valkey.set(
                        self.heartbeat_alias_key,
                        encoded,
                        ex=self.config.heartbeat_ttl_seconds,
                    ),
                )
                consumer_recent = time.monotonic() - self._last_consumer_success <= 15.0
                active_jobs_healthy = (
                    oldest_job_age_seconds is not None
                    and oldest_job_age_seconds <= float(self.config.job_health_timeout_seconds)
                )
                if not self._stop_event.is_set() and (
                    self._consumer_waiting or consumer_recent or active_jobs_healthy
                ):
                    refresh_readiness_marker()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("heartbeat update failed: %s", type(exc).__name__)
            await self._wait_or_stop(float(self.config.heartbeat_interval_seconds))

    async def _remove_heartbeat(self) -> None:
        try:
            await self.valkey.delete(self.heartbeat_key)
        except Exception as exc:
            LOGGER.debug("heartbeat removal failed: %s", type(exc).__name__)

    async def _wait_or_stop(self, delay_seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay_seconds)
        except TimeoutError:
            return

    def request_shutdown(self) -> None:
        self._stop_event.set()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.request_shutdown()
        await self._remove_heartbeat()
        await asyncio.gather(
            self.webhook_sender.close(),
            self.vector_store.close(),
            self.valkey.aclose(),
            self.repository.close(),
            return_exceptions=True,
        )

    @staticmethod
    def parse_job(raw_payload: str | bytes) -> Job:
        if isinstance(raw_payload, bytes):
            try:
                raw_payload = raw_payload.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise MalformedJobError("queue payload is not UTF-8") from exc
        try:
            payload = json.loads(raw_payload)
        except (json.JSONDecodeError, TypeError) as exc:
            raise MalformedJobError("queue payload is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise MalformedJobError("queue payload must be a JSON object")
        try:
            run_id = UUID(str(payload["run_id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise MalformedJobError("queue payload must contain a UUID run_id") from exc
        try:
            event_id = UUID(str(payload["event_id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise MalformedJobError("queue payload must contain a UUID event_id") from exc
        attempt = payload.get("attempt", 1)
        if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= 100:
            raise MalformedJobError("queue payload attempt must be an integer from 1 to 100")
        return Job(run_id=run_id, event_id=event_id, attempt=attempt)

    @staticmethod
    def prepare_text(output_text: str, maximum_characters: int = 2048) -> str:
        if not isinstance(output_text, str):
            raise EmptyOutputError("telemetry output must be text")
        normalized = unicodedata.normalize("NFC", output_text).strip()
        bounded = normalized[:maximum_characters]
        if not bounded:
            raise EmptyOutputError("telemetry output is empty")
        return bounded

    @staticmethod
    def cosine_distance(similarity: float) -> float:
        numeric = float(similarity)
        if not -1.000001 <= numeric <= 1.000001:
            raise ValueError("cosine similarity must be between -1 and 1")
        return max(0.0, min(2.0, 1.0 - numeric))

    @staticmethod
    def _default_worker_id() -> str:
        container_id = os.environ.get("ZEROPS_CONTAINER_ID", "").strip()
        return container_id or f"{socket.gethostname()}-{os.getpid()}"
