# DriftGuard

DriftGuard is a production LLM-output semantic drift and reliability monitor for Zerops. It accepts project-authenticated telemetry through FastAPI, commits the run and its queue event in one PostgreSQL transaction, embeds outputs asynchronously with a pinned 384-dimensional MiniLM model, compares them with the project's active Qdrant baseline set, and durably routes NOTIFY, DIGEST, or MUTE outcomes.

The executable topology has six services because PostgreSQL is mandatory to the five logical application/data roles:

| Service | Zerops hostname | Exposure | Purpose |
| --- | --- | --- | --- |
| Next.js | `dashboard` | public | Authenticated dashboard and server-side API proxy |
| FastAPI | `api` | public | Telemetry ingestion, administration, and diagnostics |
| Python worker | `worker` | private | Embedding, nearest-neighbor evaluation, and alert delivery |
| PostgreSQL 16 HA | `db` | private | Domain data and transactional outbox |
| Valkey 7.2 | `cache` | private | Queue, rate limits, compute cache, dedupe, and heartbeat |
| Qdrant 1.12 | `qdrant` | private | Project-filtered baseline and evaluation vectors |

The reconciled naming and reliability decisions are frozen in [driftguard-contract.md](driftguard-contract.md). No database, cache, or vector port is exposed publicly. Private HTTP calls use `http://api:8000` and `http://qdrant:6333`; Zerops terminates TLS for public ingress.

## Zerops cold start

The import manifest enables Zerops YAML preprocessing. It creates high-entropy API/admin/dashboard secrets during import and references managed-service credentials without committing plaintext values.

1. Authenticate zCLI, then create the project and six services:

   ```sh
   zcli project project-import zerops-project-import.yaml
   ```

   The same manifest can be pasted into the Zerops project-import screen.

2. Build and deploy the runtime services from the repository root. If more than one project matches, add `--project-id` to each command:

   ```sh
   zcli service push api --setup api --no-git
   zcli service push worker --setup worker --no-git
   zcli service push dashboard --setup dashboard --no-git
   ```

   Deploying the API first is intentional. Its start command acquires the migration advisory lock, upgrades PostgreSQL to Alembic head, and idempotently creates the fixed `Zerops Dashboard` project. Multiple API containers may start concurrently without racing the schema or project bootstrap.

3. In the Zerops environment-variable UI, securely retrieve the generated `DRIFTGUARD_BOOTSTRAP_PROJECT_KEY` and `DRIFTGUARD_ADMIN_TOKEN` from `api`, plus `DRIFTGUARD_DASHBOARD_PASSWORD` from `dashboard`. The dashboard username is `driftguard`. If the UI does not permit revealing an imported secret, replace it with your own value of at least 32 UTF-8 bytes and reload the affected services.

4. Get the bootstrap project UUID from the API startup log's `project_id` field, or provision an additional tenant inside an API container:

   ```sh
   python -m app_api.project_keys provision --name production
   ```

   The generated API key is printed exactly once. Store it in a secret manager; PostgreSQL stores only its SHA-256 digest. To supply rather than generate a key, put it in a temporary secret environment variable and pass `--api-key-env VARIABLE_NAME`.

5. Seed and atomically activate a baseline set inside a worker container. Input is UTF-8 JSONL with one bounded `text` value per record:

   ```sh
   printf '%s\n' '{"text":"The approved production answer."}' | \
     python -m app_worker.seed_baselines \
       --project-id 00000000-0000-0000-0000-000000000000 \
       --baseline-set production-v1 \
       --input -
   ```

   Replace the UUID with the real project ID. Seeding validates the project, embeds and persists the complete set, writes a Qdrant `point_type=baseline_manifest` marker, warms the bounded Valkey baseline cache, and only then changes `projects.active_baseline_set`. Baseline-set names are immutable versions: rerunning the exact same content is idempotent, while changed or fewer records require a new name such as `production-v2`; an active legacy set without a manifest cannot be overwritten. Use `--no-activate` to stage a set without changing production comparisons. Old sets remain auditable but are excluded from nearest-neighbor searches.

6. Sign in to the dashboard and create an alert rule, or call the API directly. Administrative endpoints require both the project key and the shared admin token:

   ```sh
   curl -X POST "https://YOUR_API_HOST/api/v1/alert-rules" \
     -H "Content-Type: application/json" \
     -H "X-API-Key: YOUR_PROJECT_KEY" \
     -H "X-DriftGuard-Admin-Token: YOUR_ADMIN_TOKEN" \
     --data '{"rule_name":"critical-drift","threshold":0.45,"action_type":"NOTIFY","notification_target":"https://hooks.slack.com/services/TENANT/CHANNEL/SECRET","is_active":true}'
   ```

7. Send a telemetry record:

   ```sh
   curl -X POST "https://YOUR_API_HOST/api/v1/logs" \
     -H "Content-Type: application/json" \
     -H "X-API-Key: YOUR_PROJECT_KEY" \
     --data '{"session_id":"release-42","prompt_text":"What is the approved answer?","output_text":"The production answer returned by the model.","metadata":{"model":"release-42"}}'
   ```

   A successful response is `202 Accepted`. The run/outbox commit has already succeeded at that point; Valkey publication and semantic evaluation continue asynchronously.

## Alert destinations

- `NOTIFY` accepts strict Slack (`https://hooks.slack.com/services/...`) or Discord (`https://discord.com/api/webhooks/...`) URLs, `pagerduty://ROUTING_KEY`, or a generic public HTTPS webhook whose hostname is explicitly allowlisted.
- `DIGEST` accepts a Slack/Discord or allowlisted public HTTPS webhook, or one `mailto:user@example.com` recipient. PagerDuty is intentionally immediate-only.
- `MUTE` creates a suppressed audit record and sends nothing.

Native adapters emit provider-valid Slack, Discord, and PagerDuty Events API envelopes. Generic URLs are rejected unless the same comma-separated `WEBHOOK_ALLOWED_HOSTS` value is set for both API admission and worker delivery. Webhook delivery rejects credentials in URLs, redirects, private/non-routable resolved addresses, and invalid ports; it pins the validated address while retaining the original TLS SNI and Host name to prevent DNS-rebinding between validation and connection. Configure these worker secrets to enable email digests:

```text
SMTP_HOST
SMTP_PORT
SMTP_USERNAME
SMTP_PASSWORD
SMTP_FROM_ADDRESS
SMTP_SECURITY=starttls|tls
```

`notified_at` is written only after successful delivery. Pending routes are leased from PostgreSQL with `FOR UPDATE SKIP LOCKED`; unsuccessful delivery is retried with bounded backoff and ultimately becomes `route_status=FAILED` rather than being reported as delivered. A DIGEST claim covers the complete rule/UTC-day group and emits one summary with its authoritative total plus at most 20 highest-drift evidence rows; each carries normalized prompt/output excerpts capped at 240 characters. Stable Valkey receipts recover a successful send when the worker survives long enough to record that receipt but crashes before the PostgreSQL transition. Delivery remains at least once: Slack, Discord, SMTP, or a generic webhook that ignores the supplied idempotency key can duplicate in the irreducible interval between its external `2xx` and receipt persistence.

## Health and readiness

| Probe | Meaning |
| --- | --- |
| Dashboard `/api/live` | Local Next.js process liveness only |
| Dashboard `/api/health` | Required secrets exist and the API accepts both dashboard credentials |
| API `/healthz` | Local API process liveness |
| API `/status` | PostgreSQL, Valkey, Qdrant, and outbox runtime initialized |
| Worker exec readiness | Model loaded and warmed; PostgreSQL is at migration head; Valkey and Qdrant are usable |
| Worker exec health | Readiness marker is fresh; a stuck job or lost heartbeat eventually fails the probe |

The Infrastructure Pulse polls every two seconds and reports PostgreSQL pool health, Valkey queue depth/latency, Qdrant latency/vector count, and worker heartbeat. Dependency outages do not kill the diagnostic dashboard process; they make readiness or service cards degraded.

Every PostgreSQL, Valkey, and Qdrant startup path performs one initial attempt followed by five retries at 2, 4, 8, 16, and 32 seconds. The CPU-only PyTorch runtime is installed through the official CPU index. The MiniLM artifact is fetched at the immutable revision `1110a243fdf4706b3f48f1d95db1a4f5529b4d41` during the worker build, dimension-checked, saved into the deployment artifact, and loaded with network access disabled at runtime. Zerops run-prepare layers install the pinned API and worker dependencies; build-container packages are never assumed to survive into the run image.

Runtime Qdrant calls are protected by a concurrency-safe closed/open/half-open circuit breaker. An open breaker applies queue backpressure without exhausting a run's delivery attempts; one probe is admitted after the reset interval and successful recovery closes the circuit.

At-least-once duplicates are fenced with a nonblocking PostgreSQL advisory lock per run. The lock is held on one dedicated session across baseline selection, embedding, Qdrant mutation, and PostgreSQL persistence, so a second worker cannot leave the vector point inconsistent with the authoritative evaluation. `DB_POOL_MAX_SIZE` must remain strictly greater than `WORKER_CONCURRENCY`; startup rejects an invalid pair.

## Secret rotation

Zerops runtime secrets are separate from `zerops.yaml`, so they can be changed followed by a service reload rather than a rebuild.

- Dashboard password: change `dashboard.DRIFTGUARD_DASHBOARD_PASSWORD`, then reload `dashboard`.
- Admin token: change `api.DRIFTGUARD_ADMIN_TOKEN`, reload `api`, then reload `dashboard` so its explicit cross-service reference is refreshed.
- Bootstrap project key: change `api.DRIFTGUARD_BOOTSTRAP_PROJECT_KEY`, reload `api` so the stored SHA-256 digest is synchronized, then reload `dashboard`.
- Managed PostgreSQL/Valkey/Qdrant credentials: rotate the managed-service secret and reload consumers of its explicit reference.
- Additional tenant key: run `python -m app_api.project_keys rotate --project-id PROJECT_UUID`; persist the newly printed value before replacing callers.

During a coordinated credential rotation, dashboard readiness deliberately remains closed while its API key/admin-token pair is inconsistent.

## Recovery and dead letters

The worker writes privacy-preserving records to the Valkey list `drift_eval_dead_letter`; the record contains the run UUID, outbox event UUID, attempt count, timestamps, and error/payload fingerprints, not prompt/output text. Inspect it only from a private service shell using the configured Valkey password.

After fixing the root cause, recover a terminal job through PostgreSQL rather than directly inventing a queue message. In one transaction, and only if no evaluation exists, set its run to `queued` and its matching outbox row back to `PENDING`:

```sql
BEGIN;
UPDATE telemetry_runs
SET status = 'queued'
WHERE id = 'RUN_UUID'
  AND NOT EXISTS (SELECT 1 FROM evaluations WHERE run_id = 'RUN_UUID');

UPDATE telemetry_outbox
SET status = 'PENDING',
    retry_count = 0,
    next_attempt_at = NOW(),
    dispatch_time = NULL,
    last_error = NULL
WHERE id = 'EVENT_UUID'
  AND run_id = 'RUN_UUID';
COMMIT;
```

The API outbox poller republishes the canonical event. Verify that the run reaches `completed` before removing its DLQ record. Never requeue an already evaluated run; the unique `evaluations.run_id` constraint makes duplicate delivery a no-op.

To retry an alert whose durable route reached `FAILED`, first fix the target/transport and then reset only the intended triggered alert:

```sql
UPDATE alerts
SET route_status = 'PENDING',
    route_due_at = NOW(),
    delivery_lease_until = NULL,
    delivery_lease_token = NULL,
    delivery_attempts = 0,
    last_delivery_error = NULL
WHERE id = 'ALERT_UUID'
  AND alert_status = 'TRIGGERED'
  AND route_status = 'FAILED';
```

## Local development and verification

Use Python 3.12 and Node.js 22. Start private PostgreSQL 16, Valkey 7.2, and Qdrant 1.12 instances, then populate the values shown in [.env.example](.env.example). Do not expose their ports outside the local development machine.

```sh
python -m pip install -r requirements-dev.txt
npm ci
alembic upgrade head
python -m app_api.project_keys provision --name local
uvicorn app_api.main:app --host 127.0.0.1 --port 8000
python -m app_worker.main
npm run dev
```

Run the release gates from the repository root:

```sh
python -m ruff check .
python -m pytest -q
npm run lint
npm run build
npm audit --omit=dev --audit-level=high
```

The telemetry simulator uses the canonical endpoint and retries transient HTTP failures:

```sh
DRIFTGUARD_API_KEY="YOUR_PROJECT_KEY" \
  python telemetry_simulator.py --host 127.0.0.1 --port 8000 --mode drift-up --count 100
```

`stable` sends ordinary production telemetry close to an already seeded gold set; it does not create baselines.

## Production capacity acceptance

The worker tier is configured for two warm containers and can scale horizontally to ten; each container applies bounded concurrency and per-run fencing. That capacity envelope is not itself proof of the PRD's sustained 500 logs/s, sub-10 ms ingest, or sub-500 ms evaluation SLO on a particular Zerops service size. Before production cutover, run a sustained workload against the deployed topology and retain measurements for HTTP admission latency, queue depth and delay, worker evaluation latency, end-to-end decision latency, PostgreSQL saturation, and Qdrant latency.

DriftGuard intentionally has no implicit destructive retention. PostgreSQL telemetry/evaluations/alerts/outbox rows and Qdrant evaluation points remain auditable until an operator-approved retention, legal-hold, archive, and restore policy is implemented. At 500 logs/s the raw arrival rate is 43.2 million runs per day, so capacity and partition/retention policy are mandatory deployment decisions, not optional housekeeping.

The current daily-digest claim is deliberately all-or-nothing for one rule and UTC day, which preserves the product promise of one consolidated summary. Its real-PostgreSQL regression covers 25 alerts, not a day containing millions of anomalies. Large-volume acceptance must prove that the group claim and final transition finish inside the configured lease; otherwise production needs a materialized digest-rollup/outbox design rather than simply lengthening the lease.

## API surface

- `POST /api/v1/logs` — authenticated telemetry ingestion.
- `GET /api/v1/metrics/trends?window=24h|7d|30d` — authoritative drift and latency aggregates.
- `GET /api/v1/alerts` — tenant-scoped recent/searchable alert evidence.
- `GET|POST /api/v1/alert-rules`, `PUT /api/v1/alert-rules/{id}` — routing policy management.
- `GET /api/v1/vectors/projection` — bounded two-dimensional tenant projection; raw 384D embeddings are never returned.
- `GET /api/v1/diagnostics/pulse` — live dependency telemetry.
- `GET /api/v1/dashboard/session` — dashboard credential-pair readiness.

Except for `/api/v1/logs` and unauthenticated liveness/readiness endpoints, production/dashboard routes require the separate admin credential. Notification targets are never exposed to a browser without the authenticated dashboard proxy.
