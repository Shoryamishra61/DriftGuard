# DriftGuard Canonical Engineering Contract

This document freezes the reconciled Phase 1 contract supplied after the PRD/SRS audit. When older files disagree with this document, this contract controls implementation naming and boundaries.

## Service topology

DriftGuard has six Zerops services:

| Hostname | Type | Exposure | Responsibility |
| --- | --- | --- | --- |
| `dashboard` | Next.js runtime | Public | Dashboard and server-side API proxy |
| `api` | FastAPI runtime | Public | Authenticated telemetry ingestion and diagnostics |
| `worker` | Python runtime | Private | Embedding, vector search, evaluation, and alert routing |
| `db` | PostgreSQL 16 HA | Private | Relational state and transactional outbox |
| `cache` | Valkey 7.2 single | Private | Queue, deduplication, caches, and worker heartbeat |
| `qdrant` | Qdrant 1.12 single | Private | Baseline and evaluated vectors |

All internal HTTP traffic uses private hostnames and plain HTTP, including `http://api:8000` and `http://qdrant:6333`. PostgreSQL, Valkey, and Qdrant must never receive public access.

## Ingestion contract

`POST /api/v1/logs` requires an `X-API-Key` header and this JSON body:

```json
{
  "session_id": "string",
  "prompt_text": "string",
  "output_text": "string",
  "metadata": {}
}
```

The API hashes the supplied key with SHA-256 and resolves `projects.api_key_hash`; plaintext API keys are never stored. Each text field is limited to 50 KiB encoded as UTF-8. A successful request atomically inserts one `telemetry_runs` row and one `telemetry_outbox` row before returning `202 Accepted` with the run UUID.

## Queue and delivery contract

- Queue: `drift_eval_queue`.
- Producer: `LPUSH` after the PostgreSQL transaction commits.
- Consumer: `BLPOP`.
- Message identity: outbox event UUID plus telemetry run UUID.
- Delivery is at least once. The outbox dispatcher claims due events with `FOR UPDATE SKIP LOCKED`; the worker is idempotent on the unique evaluation `run_id`.
- Failed startup connections use an initial attempt plus five retries with exponential delays beginning at two seconds.

## Relational contract

The canonical tables are `projects`, `telemetry_runs`, `evaluations`, `alert_rules`, `alerts`, and `telemetry_outbox`. Vectors are not stored in PostgreSQL. Foreign keys that express ownership are non-null. Drift values and thresholds are constrained to the cosine-distance range `[0, 2]`.

Canonical state values are:

- telemetry run: `queued`, `processing`, `completed`, `failed`;
- alert action: `NOTIFY`, `DIGEST`, `MUTE`;
- alert status: `TRIGGERED`, `RESOLVED`, `SNOOZED`;
- outbox status: `PENDING`, `DISPATCHED`, `FAILED`.

### Durable routing amendment

The frozen alert table did not contain enough state to distinguish an incident
from a successfully delivered notification. Revision `20260809_0002` therefore
adds delivery-only metadata without changing the six canonical domain tables:
`route_status`, `route_due_at`, `delivery_lease_until`, `delivery_attempts`, and
`last_delivery_error`. It also makes `notified_at` nullable so it is written only
after a successful delivery. `alert_status` remains the incident state;
`route_status` is one of `PENDING`, `DELIVERED`, `SUPPRESSED`, or `FAILED`.

This forward migration is required for crash-safe NOTIFY retries, consolidated
DIGEST delivery, MUTE suppression, and horizontally scaled worker claims. It is
kept separate from the initial schema so an environment that already recorded
revision `20260809_0001` cannot silently skip the reliability columns.

A DIGEST lease covers the full `(rule_id, UTC evaluation day)` group, while its
outbound payload carries the authoritative total and a bounded top-20 evidence
sample. A stable group receipt makes a successful external send recoverable
when it was persisted before a worker crash. Notification delivery is at least
once, not exactly once: a provider that ignores the deterministic idempotency
key can duplicate if the worker dies after the external `2xx` but before it can
persist the Valkey receipt.

Revision `20260809_0004` adds a nullable `delivery_lease_token` UUID. Every
delivery claim assigns a fresh token and every attempt, success, suppression,
or failure update must match and clear that token. Expiry still controls when a
claim can be reclaimed, while the token fences a paused former owner from
mutating or sending after a different worker takes ownership.

Revision `20260809_0005` adds a partial index for active delivery lease tokens
and a descending evaluation-time index for dashboard time-window scans. Worker
readiness requires the delivery-token index, preventing a worker from starting
against an older schema during a rolling migration.

Duplicate queue deliveries are also fenced per run. A worker acquires a
nonblocking PostgreSQL session advisory lock before reading the active baseline
set and holds that ownership through embedding, Qdrant writes, and authoritative
PostgreSQL persistence. A contending duplicate performs no mutation. The lock
uses a dedicated pooled connection and is explicitly released on that same
session; a session whose cleanup cannot prove release is terminated. The worker
database pool must therefore be strictly larger than worker concurrency.

## Vector contract

Qdrant collection `drift_baselines` uses 384-dimensional vectors with cosine distance. Every point contains `project_id` and `point_type` in its payload. Seeded gold vectors use `point_type=baseline`; evaluated production outputs use `point_type=evaluation`. Every nearest-neighbor query must apply exact `project_id` and `point_type=baseline` payload filters; an unfiltered or mixed-type search is a tenant-isolation and correctness defect.

Revision `20260809_0003` adds `projects.active_baseline_set`. Baseline seeding
fully persists a named set before atomically activating it in PostgreSQL. Vector
queries additionally require the exact active `baseline_set`, so retained older
sets remain auditable but cannot silently influence current drift scores. A
project with no active set produces a nullable no-match evaluation rather than
falling back across versions.

Each `(project_id, baseline_set, embedding_model_revision)` is an immutable
version. A deterministic `point_type=baseline_manifest` marker records its
content hash and count: identical reruns are idempotent, while changed or fewer
records must use a new set name. Manifest markers share the collection for
atomic discovery but are excluded by all baseline and evaluation filters.

The embedding model is pinned to Hugging Face revision
`1110a243fdf4706b3f48f1d95db1a4f5529b4d41`. Baseline and evaluation payloads
record this revision, and nearest-neighbor filters require it in addition to
project, point type, and active baseline set. A model upgrade therefore requires
seeding and activating a new compatible baseline set; it cannot silently compare
new evaluation vectors with vectors produced by a different model artifact.

Telemetry remains an immutable per-request audit trail, so identical ingestion
requests still create distinct runs. Valkey deduplicates the expensive embedding
computation instead, using a project-scoped, model-revision-scoped hash of the
normalized output text. This preserves production volume evidence without
performing redundant model inference.

## Zerops lifecycle contract

- Runtime ports must be between 10 and 65435.
- Build caches include `node_modules` for the dashboard and `/root/.cache/pip` for Python services.
- `deploy.readinessCheck` gates new deployments; `run.healthCheck` monitors active containers.
- Runtime configuration lives in `zerops.yaml`. Rotatable credentials live in Zerops secret variables or explicit references to managed-service generated secrets.
- Current Zerops schemas require import `services` at the document top level and current mode-qualified managed types. Therefore executable manifests use `postgresql:ha@16`, `valkey:single@7.2`, and `qdrant:single@1.12` rather than obsolete example forms.

## Capacity and data lifecycle boundary

The PRD throughput and latency figures are deployment acceptance SLOs, not facts
that can be established by schema or unit tests. They require a sustained load
test on the selected Zerops sizes, with queue delay, evaluation latency, and
dependency saturation recorded. The application does not delete audit data or
Qdrant evaluation points automatically: no retention duration or archive target
was ratified in the supplied product contract. Before production traffic, the
operator must approve retention, legal-hold, archive, and restore policy and
then provision capacity or partitioning for that policy. Silent destructive
expiry is outside this contract.

The consolidated DIGEST path leases and transitions a complete rule/day group.
That correctness model is verified at bounded integration volume, but its lease
duration is not certified for a multi-million-alert day. High-volume production
acceptance must either prove the whole-group transaction fits its lease or add a
materialized digest rollup/outbox; splitting the group into multiple emails is
not an allowed fallback.
