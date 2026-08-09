# DriftGuard: A Transactionally Reliable Semantic-Drift Monitor for Production LLM Outputs

**Author:** Shorya Mishra  
**Implementation target:** Zerops PaaS  
**Report date:** 9 August 2026

## Abstract

Large language model applications can degrade semantically without producing transport-level failures. This report presents DriftGuard, a deployed monitoring system that compares production outputs with project-specific reference answers and converts semantic distance into auditable operational policy. The design combines a transactionally reliable ingestion path, asynchronous embedding, tenant-filtered cosine search, horizontally safe evaluation, durable alert delivery, and live dependency diagnostics. The implementation runs as six services on Zerops: a Next.js dashboard, FastAPI API, Python worker tier, PostgreSQL, Valkey, and Qdrant. Verification covers unit, contract, security, database-integration, build, and live deployment behavior. Results are reported conservatively: the deployed application is reachable and healthy, while sustained 500-request-per-second throughput and sub-5 ms public admission remain unverified capacity targets rather than claimed outcomes.

## 1. Problem statement

HTTP availability does not imply answer quality. A model can return `200 OK` while producing answers that are irrelevant, outdated, contradictory, or unsafe. Conventional uptime tools observe transport and infrastructure behavior but do not continuously compare answer meaning with a domain-approved reference set.

DriftGuard investigates the following engineering question:

> Can semantic drift be monitored continuously without placing embedding latency on the ingestion path, losing telemetry during broker failure, mixing tenant vectors, or falsely recording notification delivery?

The project focuses on operational reliability rather than claiming that vector similarity alone proves factual correctness. Distance is a signal for investigation and routing, not an oracle.

## 2. Design objectives

The system is designed to:

1. preserve telemetry before acknowledging ingestion;
2. keep expensive model inference off the synchronous path;
3. isolate every relational and vector operation by project;
4. remain correct under duplicate queue delivery and horizontal workers;
5. distinguish incident state from notification-delivery state;
6. expose dependency health without cascading dashboard restarts;
7. package a reproducible embedding model for network-independent runtime startup;
8. keep credentials and managed data services outside the public boundary.

## 3. System architecture

The public plane contains the dashboard and API. The private plane contains PostgreSQL, Valkey, Qdrant, and workers. Zerops provides runtime builds, generated secrets, private DNS, TLS ingress, horizontal container ranges, managed data services, and readiness/liveness orchestration.

### 3.1 Ingestion

The API authenticates a project key by SHA-256 digest, bounds the request body, applies a project-scoped Valkey rate limit, and validates the telemetry schema. One PostgreSQL transaction inserts the run and its outbox event. Publication occurs only after commit. This transactional-outbox boundary prevents a broker outage from creating acknowledged-but-untracked telemetry.

### 3.2 Asynchronous processing

The worker receives only event and run UUIDs; prompt and output text remain authoritative in PostgreSQL. A dedicated PostgreSQL session acquires a nonblocking advisory lock for the run. Ownership remains held across baseline selection, embedding, Qdrant search/upsert, and relational evaluation persistence. Duplicate deliveries therefore cannot create divergent cross-store results.

### 3.3 Vector evaluation

The encoder artifact is pinned to an immutable Hugging Face revision and loaded locally at runtime. Baseline and evaluation payloads record the model revision. Nearest-neighbor queries require exact filters for project, baseline point type, active baseline version, and model revision. A baseline manifest stores deterministic content hash and count, making identical imports idempotent and changed imports explicitly versioned.

### 3.4 Alert routing

Rules map a distance threshold to `NOTIFY`, `DIGEST`, or `MUTE`. Incident status and route status are separate. A notification is marked delivered only after provider success. Workers claim due delivery rows with `FOR UPDATE SKIP LOCKED`, expiry, and a UUID lease token. The token fences a paused owner after another worker reclaims the route.

Daily digests claim one rule and UTC-day group, report the authoritative count, and include at most 20 highest-distance evidence records. Prompt and output excerpts are normalized and bounded to 240 characters.

## 4. Failure model and mitigations

| Failure | Mitigation |
| --- | --- |
| API crashes before commit | Neither run nor outbox event exists; caller retries |
| API crashes after commit | Outbox poller finds and publishes the durable event |
| Valkey unavailable | Event remains pending with bounded backoff |
| Duplicate queue event | Per-run advisory ownership plus unique evaluation constraint |
| Worker crashes during evaluation | Run can be recovered; incomplete owner session releases its lock |
| Qdrant outage | Generation-safe circuit breaker and queue backpressure |
| Worker crashes after provider `2xx` | Stable receipt recovers most send-to-database crash windows |
| Stale delivery owner | UUID lease-token fencing rejects stale mutation |
| Dashboard dependency outage | Local liveness remains healthy; per-panel diagnostics show degradation |

Exactly-once external notification is not claimed. A provider that ignores idempotency metadata can duplicate in the interval between its `2xx` response and durable receipt persistence.

## 5. Security model

The threat model includes tenant confusion, credential leakage, server-side request forgery, DNS rebinding, unbounded payloads, stale delivery ownership, and browser-side privilege escalation.

- Administrative routes require both tenant and admin credentials.
- Dashboard credentials remain server-side and mutations require same-origin checks.
- Notification targets use provider-specific validation or an explicit hostname allowlist.
- Resolved destinations must be public unicast and are connected through the validated address while retaining TLS SNI and HTTP Host.
- PostgreSQL, Valkey, and Qdrant have no public subdomain access.
- The API receives only Qdrant's read-only key; the worker receives the write-capable key.
- Raw vectors and plaintext stored API keys are never returned to the browser.

## 6. Evaluation methodology

Verification uses four layers:

1. deterministic unit and contract tests for API, worker, routing, SSRF policy, circuit-breaker epochs, cache integrity, and baseline versioning;
2. PostgreSQL 16 integration tests for unoverridden authentication, atomic ingestion/outbox persistence, migrations, delivery mapping, concurrent claims, and lease fencing;
3. static and build gates using Ruff, Python bytecode compilation, ESLint, TypeScript, Next.js production build, and dependency audits;
4. live Zerops checks for service status, public endpoints, authentication, dependency health, generated secret references, and worker heartbeat.

## 7. Results

### 7.1 Automated verification

The settled pre-deployment tree produced 188 passing default Python tests and four opt-in PostgreSQL tests. Ruff, compilation, ESLint, TypeScript, and the optimized dashboard build passed. The Node dependency audit reported zero vulnerabilities. CI now provisions PostgreSQL 16 so the real-database tests run on every push and pull request.

### 7.2 Live Zerops verification

The deployed project reported all six services `ACTIVE`. The public dashboard liveness endpoint, authenticated dashboard, dashboard readiness endpoint, API health, and API status returned success. Infrastructure Pulse reported healthy PostgreSQL, Valkey, Qdrant, and worker state. At one observed instant, dependency latency was approximately 8.67 ms for PostgreSQL, 8.63 ms for Valkey, 10.55 ms for Qdrant, and 7.08 ms for the worker heartbeat path. These are point observations, not percentile guarantees.

### 7.3 Latency interpretation

A previous bounded local end-to-end exercise measured 54.491 ms to `202 Accepted`, 173.153 ms from ingestion to stored evaluation, and 75 ms worker compute time. This does not satisfy the aspirational sub-5 ms public-ingestion target and does not establish sustained throughput. Reporting the result prevents a local smoke test from being misrepresented as a production benchmark.

## 8. Capacity and retention

At 500 telemetry records per second, arrival volume is 43.2 million runs per day before evaluations, alerts, outbox history, indexes, and vectors. The challenge deployment is intentionally a bounded demonstration, not a certified 500-per-second data-retention tier.

The proposed production policy is:

- raw prompt/output telemetry: 30 days online;
- evaluation aggregates and alert evidence: 90 days online;
- delivered outbox rows: 7 days;
- immutable baseline versions: retain while referenced, then archive;
- legal holds: override deletion by project and date range;
- archive: encrypted object storage with tested restore sampling.

This policy requires time-based PostgreSQL partitioning and coordinated Qdrant evaluation-point expiry before high-volume launch. It is documented as an approval boundary because silently adding destructive deletion would be less responsible than retaining data in a challenge-scale deployment.

The complete-day digest algorithm also requires load proof at multi-million-alert scale. A materialized digest rollup/outbox is the recommended next design if a full-day claim cannot commit within the lease.

## 9. Reproducibility

The repository contains exact direct dependency pins, an immutable model revision, linear Alembic migrations, a 50-record baseline corpus, infrastructure import, runtime pipeline, test suite, and CI workflow. A clean Zerops import builds from the public repository, generates secrets, migrates the database, provisions the tenant, and idempotently seeds the active baseline.

Transitive Python artifacts are not hash-locked. This is a documented supply-chain improvement rather than a hidden claim of bit-for-bit build reproduction.

## 10. Limitations and future work

- Run a sustained Zerops load test with queue-delay and dependency-saturation telemetry.
- Implement approved partition, archive, legal-hold, and restore automation.
- Replace whole-day digest row claims with a materialized rollup for extreme anomaly volume.
- Exercise an authorized Slack/Discord/PagerDuty target and an SMTP digest before operational launch.
- Add browser-level dashboard security and accessibility tests to CI.
- Evaluate domain-specific encoders and calibrated thresholds against labeled incident outcomes.

## 11. AI-use disclosure

OpenAI Codex assisted with code generation, debugging, tests, security review, deployment operations, and documentation. Shorya Mishra supplied the problem definition and requirements, directed the product and release process, controlled credentials and deployment, reviewed the system, and owns the project. The implementation is public so judges can examine both the engineering decisions and the resulting code.

## 12. Conclusion

DriftGuard demonstrates that semantic monitoring can be treated as a distributed-systems reliability problem rather than a dashboard-only feature. The deployed system preserves ingestion atomically, isolates tenant vectors, fences duplicate work, records delivery truthfully, and exposes its own infrastructure state. Its challenge acceptance requirements are met by a live Zerops deployment and public source. High-volume SLO certification remains explicitly separated from the verified result.
