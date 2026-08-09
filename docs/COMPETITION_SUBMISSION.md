# Zerops Challenge Submission Checklist

This checklist maps the published challenge requirements to verifiable DriftGuard evidence. A checked item means evidence exists; operator-only actions remain unchecked until the relevant link is submitted.

## Finished-product requirements

- [x] **Working product:** authenticated telemetry ingestion, asynchronous semantic evaluation, alert policy, vector projection, and Infrastructure Pulse are implemented.
- [x] **Live URL:** <https://dashboard-141-3000.sea1.zerops.app>
- [x] **Meaningful Zerops use:** six Zerops services, private networking, managed PostgreSQL/Valkey/Qdrant, generated secrets, build caches, autoscaling, readiness, liveness, and public TLS ingress.
- [x] **Public source:** <https://github.com/Shoryamishra61/DriftGuard>
- [x] **Deployment remains running:** all six services were verified `ACTIVE`; the project must remain funded and online through judging.
- [x] **Solo project:** repository and Zerops project are owned by `Shoryamishra61`.
- [x] **AI tools disclosed:** OpenAI Codex usage is disclosed in the README, technical report, and submission text below.
- [x] **No reference dumps in submission:** temporary PRD/SRS/prompt files were removed from the public tree.

## Build-post requirements

- [x] Project name appears in the prepared post.
- [x] Short product explanation is prepared.
- [x] Live deployment link is prepared.
- [x] Zerops architecture explanation is prepared.
- [x] `@WeMakeDevs` and `@zeropsio` are included.
- [x] A concise 60–75 second recording script and shot list are prepared.
- [ ] Record and attach the working-product video from the live dashboard.
- [ ] Publish the post from the entrant's own social account.
- [ ] Paste the published post URL into the challenge form.

The final three actions require the entrant's social account and cannot be truthfully completed by repository automation.

## Submission-form package

**Project name**  
DriftGuard

**One-line description**  
DriftGuard detects semantic degradation in production LLM answers and routes reliable, auditable alerts from a six-service Zerops deployment.

**Repository**  
<https://github.com/Shoryamishra61/DriftGuard>

**Live deployment**  
<https://dashboard-141-3000.sea1.zerops.app>

**Demo access**  
Username: `driftguard`. Copy the current `DRIFTGUARD_DASHBOARD_PASSWORD` secret from the Zerops `dashboard` service into the private challenge form. Never publish the password in a post, video, issue, or repository.

**How Zerops is used**  
Zerops builds and runs the Next.js dashboard, FastAPI ingestion API, and horizontally scaled Python workers. PostgreSQL 16 HA provides authoritative state and a transactional outbox; Valkey provides the private task queue, rate limits, caches, receipts, and heartbeat; Qdrant provides tenant-filtered vector search. Zerops private DNS carries internal traffic, generated secret references wire credentials, readiness gates prevent partial rollouts, and public subdomains expose only the dashboard and API.

**AI-use disclosure**  
OpenAI Codex assisted with implementation, debugging, tests, security review, deployment operations, and documentation. I defined the product requirements and competition goal, directed architecture and release decisions, supplied and controlled credentials, reviewed the implementation, and understand the system I am submitting.

## Judge walkthrough

1. Open the live dashboard using the privately supplied credentials.
2. Show Infrastructure Pulse with healthy PostgreSQL, Valkey, Qdrant, worker, and queue depth.
3. Select 24-hour, 7-day, and 30-day telemetry windows.
4. Show the Qdrant-derived baseline/evaluation projection.
5. Create or edit a `MUTE` demonstration rule without exposing an external webhook.
6. Submit one telemetry sample through the simulator or API.
7. Show the resulting evaluation and incident evidence.
8. Open the public repository and point to `zerops.yaml`, the outbox transaction, per-run fencing, and delivery lease token.

## Operational launch gates

These are not challenge eligibility requirements and must not be misrepresented as completed production-scale certification:

- [x] Bounded 500 requests/second test executed and published honestly (target failed).
- [ ] Sustained 500 requests/second accepted without errors on final Zerops sizes.
- [ ] Sub-5 ms public or application `202` admission latency (measured target failed).
- [x] Automated 30/90/7-day retention and project/date legal holds implemented.
- [ ] Composite-key time partition/archive tier required before 500-RPS approval.
- [ ] Multi-million-alert daily digest lease proof or materialized rollup.
- [ ] Authorized live NOTIFY provider and SMTP DIGEST delivery tests.

The application is a live, working challenge submission. The unchecked items define a responsible high-volume enterprise launch boundary.
