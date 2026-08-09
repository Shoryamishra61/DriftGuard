# DriftGuard Social Launch Package

## Primary post

I built **DriftGuard** for the Zerops Challenge: a live reliability monitor that catches semantic drift in production LLM answers—even when the upstream API still returns `200 OK`.

Every telemetry event is committed with a PostgreSQL transactional outbox, queued through Valkey, embedded by horizontally scaled workers with a pinned MiniLM model, and compared against versioned gold baselines in Qdrant. DriftGuard then routes the result through immediate NOTIFY, consolidated DIGEST, or auditable MUTE policy.

Zerops runs the complete six-service system:

- Next.js dashboard and FastAPI API on public TLS ingress
- Python workers on the private network
- PostgreSQL 16 HA for authoritative state
- Valkey for the queue, rate limits, cache, receipts, and heartbeats
- Qdrant for tenant-isolated vector search
- Generated secrets, private DNS, autoscaling, cached builds, readiness, and liveness from `zerops.yaml`

The part I care about most: the system is honest under failure. An alert is not marked notified until delivery succeeds, duplicate jobs are fenced across PostgreSQL and Qdrant, and Infrastructure Pulse shows queue depth plus live database/cache/vector latency.

Live: https://dashboard-141-3000.sea1.zerops.app  
Source: https://github.com/Shoryamishra61/DriftGuard

Built with AI assistance from OpenAI Codex; I directed the product, architecture, security decisions, deployment, and verification.

@WeMakeDevs @zeropsio

#ZeropsChallenge #BuildInPublic #LLMOps #AIEngineering #DistributedSystems

## Short variant

Production LLMs can drift while every request still returns `200 OK`.

I built **DriftGuard**: a live semantic-drift and reliability monitor running as six services on Zerops—Next.js, FastAPI, Python workers, PostgreSQL HA, Valkey, and Qdrant.

Transactional ingestion. Tenant-safe vectors. Durable NOTIFY/DIGEST/MUTE routing. Live Infrastructure Pulse.

Demo: https://dashboard-141-3000.sea1.zerops.app  
Code: https://github.com/Shoryamishra61/DriftGuard

@WeMakeDevs @zeropsio #ZeropsChallenge #LLMOps

## 70-second video script

**0–7 seconds — problem**  
“An LLM can return 200 OK while the meaning of its answers quietly degrades. DriftGuard makes that failure observable.”

**7–20 seconds — live dashboard**  
Open the authenticated live URL. Show drift trend, evaluation latency, active alerts, and the vector projection.

**20–32 seconds — Infrastructure Pulse**  
Show healthy PostgreSQL, Valkey queue depth, Qdrant latency/vector count, and worker heartbeat. Say: “These are live Zerops services, not mock dashboard data.”

**32–45 seconds — ingest and result**  
Submit one telemetry event. Refresh or wait for polling. Show the evaluation and matched baseline evidence.

**45–58 seconds — architecture**  
Show the README architecture diagram or Zerops project. Name the six services and point out that only dashboard and API are public.

**58–70 seconds — reliability and close**  
Show the transactional outbox and delivery status briefly. Say: “DriftGuard keeps ingestion durable, vector search tenant-safe, and notifications honest under failure. It is live on Zerops and the source is public.”

## Recording checklist

- Use a 1080p recording with the browser zoomed to make service cards readable.
- Hide bookmarks, email, Zerops tokens, API keys, passwords, webhook targets, and browser autofill.
- Preload one normal and one visibly drifted evaluation so the dashboard has a clear story.
- Keep the cursor movement deliberate; do not scroll through raw secrets or runtime environment pages.
- End on the live URL and repository URL for at least three seconds.
- Add captions because many viewers watch without sound.
