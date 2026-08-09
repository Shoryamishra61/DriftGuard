# DriftGuard social launch package

The challenge requires a public post containing the project name, a short explanation, a short working-product video, the live deployment, a brief account of Zerops usage, and tags for `@WeMakeDevs` and `@zeropsio`.

No wording can guarantee reach or a prize. This package instead uses defensible communication principles:

- **Information gap:** open with the counterintuitive failure—`200 OK` can still be semantically wrong.
- **Loss salience:** make the invisible production risk concrete before introducing the product.
- **Cognitive fluency:** one idea per paragraph, short sentences, and named evidence.
- **Specificity:** six services and actual failure controls are more credible than “production-ready.”
- **Narrative closure:** move from silent failure, to live detection, to an invitation to inspect the system.
- **Epistemic honesty:** disclose AI assistance and measured limits; trust is part of a reliability product.

## Recommended X thread

Attach the 60–70 second video to post 1. Publish the remaining numbered posts as replies.

### 1/7 — Hook

`200 OK` does not mean an LLM answer is still right.

The service can stay online while its meaning quietly drifts.

I built **DriftGuard** to make that invisible failure observable—in real time, on Zerops. 🎥

### 2/7 — Problem

Uptime monitors ask: “Did the model respond?”

DriftGuard asks: “Did the response remain semantically aligned with what production considers correct?”

It compares live outputs with versioned gold baselines and turns meaningful deviations into incidents.

### 3/7 — Product proof

The dashboard connects model quality with operational evidence:

• semantic-distance trends
• nearest-baseline evidence
• vector topology
• NOTIFY / DIGEST / MUTE policy
• live PostgreSQL, Valkey, Qdrant, and worker health

### 4/7 — Zerops story

Zerops runs the complete six-service system:

Next.js + FastAPI + Python workers + PostgreSQL HA + Valkey + Qdrant.

Only the dashboard and API are public. Everything else communicates through the private Zerops network, with independent builds and readiness gates.

### 5/7 — Engineering idea

The design principle was simple: uncertainty must not become data loss.

Telemetry and its outbox event commit atomically. Duplicate workers are fenced. Baselines are versioned. An alert is marked notified only after delivery succeeds.

Reliability is recorded, not assumed.

### 6/7 — Honest evidence

I also tried to break it.

The bounded 500 RPS test exposed a real capacity limit, so I published the failed target and exact percentiles instead of hiding them.

A reliability tool should be honest about its own reliability.

### 7/7 — Close

**Live, public judge view:**
https://dashboard-141-3000.sea1.zerops.app

**Source and research notes:**
https://github.com/Shoryamishra61/DriftGuard

Built with disclosed OpenAI Codex assistance and my product, architecture, deployment, and review decisions.

@WeMakeDevs @zeropsio

#ZeropsChallenge #LLMOps #DistributedSystems

## Recommended LinkedIn post

**A production LLM can return `200 OK` and still be wrong in the way that matters: meaning.**

That is the failure I built **DriftGuard** to observe for The Zerops Challenge.

DriftGuard compares production answers with versioned gold baselines, measures their semantic distance, and turns meaningful deviations into immediate notifications, consolidated digests, or auditable muted incidents.

The interesting part is not only the embedding model. It is the reliability boundary around it:

- telemetry and its queue event commit atomically in PostgreSQL;
- Valkey carries asynchronous work without becoming the source of truth;
- workers are fenced across PostgreSQL and Qdrant;
- baseline searches are isolated by project, baseline version, and model revision;
- delivery state changes only after a notification succeeds;
- Infrastructure Pulse shows whether PostgreSQL, Valkey, Qdrant, and workers are healthy.

Zerops runs the full six-service architecture: Next.js dashboard, FastAPI API, Python workers, PostgreSQL HA, Valkey, and Qdrant. It provides private networking, managed data services, independent build pipelines, autoscaling, readiness gates, secrets, and public TLS ingress.

I also ran a bounded 500 RPS experiment. It exposed a capacity ceiling rather than proving the target, so the repository publishes the failed result and exact measurements. That honesty is deliberate: a reliability product should not manufacture certainty about itself.

The live deployment is public and read-only. Its guided tour walks through real infrastructure and persisted semantic evidence.

🎥 Attach the short demo video here.

Live: https://dashboard-141-3000.sea1.zerops.app
Source: https://github.com/Shoryamishra61/DriftGuard

OpenAI Codex assisted with implementation, testing, review, and deployment. I defined the problem, directed the architecture and trade-offs, controlled deployment, and reviewed the submitted system.

@WeMakeDevs @zeropsio

#ZeropsChallenge #LLMOps #AIEngineering #DistributedSystems #BuildInPublic

## 65-second video sequence

Do not narrate a slide deck. Show the live URL continuously except for the six-service Zerops view.

| Time | Visual | Exact narration |
| --- | --- | --- |
| 0–6s | Open on the drift incident, then reveal the dashboard | “An LLM can return 200 OK while the meaning of its answer quietly fails.” |
| 6–13s | Hero and four metrics | “DriftGuard turns that silent failure into a measurable production signal.” |
| 13–23s | Drift chart and vector topology | “Each answer is compared with a versioned project baseline using a pinned semantic model and tenant-filtered vector search.” |
| 23–34s | Infrastructure Pulse | “These are live Zerops checks—not mock cards—for PostgreSQL, Valkey, Qdrant, the task queue, and workers.” |
| 34–44s | Expand incident evidence | “Here is the prompt, degraded output, nearest baseline, drift distance, and durable routing result.” |
| 44–55s | Zerops project with six services | “Zerops builds and operates six services. Only the dashboard and API are public; the data plane stays on the private network.” |
| 55–65s | Return to dashboard; show live and source URLs | “DriftGuard makes semantic reliability observable—and makes its own limits visible too. Try the live read-only tour and inspect the source.” |

## Recording and publishing checklist

- [ ] Record at 1080p with browser zoom high enough to read cards on a phone.
- [ ] Begin with the failure, not a logo animation or biography.
- [ ] Keep every password, API key, webhook destination, token, email, and browser autofill hidden.
- [ ] Show the real browser address and the Zerops service list.
- [ ] Add burned-in captions; many viewers watch without sound.
- [ ] Use one clean pointer movement per sentence and avoid frantic scrolling.
- [ ] End on the live URL and repository for at least three seconds.
- [ ] Attach the video directly to the social post instead of linking only to an external player.
- [ ] Tag `@WeMakeDevs` and `@zeropsio` exactly.
- [ ] Reply thoughtfully to early comments with architecture or failure-testing details; do not spam tags or engagement bait.
