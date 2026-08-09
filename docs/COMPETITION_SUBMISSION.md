# Zerops Challenge submission package

Copy the fields below into the official form. The live deployment is publicly readable and requires no credentials.

## Required fields

### Project title

**DriftGuard — Semantic Reliability for Production LLMs**

### Project description

An LLM endpoint can remain online while the meaning of its answers quietly degrades. DriftGuard makes that failure observable.

DriftGuard is a live semantic-drift and reliability monitor deployed as six services on Zerops. A FastAPI service durably admits production telemetry through a PostgreSQL transactional outbox. Valkey queues evaluation work; Python workers encode responses with a revision-pinned MiniLM model; Qdrant compares them with versioned, tenant-isolated gold baselines. Threshold breaches become immediate NOTIFY, consolidated DIGEST, or auditable MUTE incidents.

The Next.js dashboard connects model quality with system health: semantic trends, evidence, vector topology, and a live Infrastructure Pulse for PostgreSQL, Valkey, Qdrant, and workers. The public judging deployment is server-enforced read-only, needs no credentials, and includes a guided tour over real persisted evidence.

Zerops provides the deployment topology, managed data services, private networking and DNS, generated secrets, independent build pipelines, autoscaling, health/readiness gates, and public TLS ingress. Only the dashboard and API are public.

### Repository (source code)

<https://github.com/Shoryamishra61/DriftGuard>

### Live deployment on Zerops

<https://dashboard-141-3000.sea1.zerops.app>

### Social post

Paste the URL of the published post containing the short product video. Do not submit a draft or repository link in this field.

## AI-use disclosure

OpenAI Codex assisted with implementation, debugging, testing, security review, deployment operations, and documentation. I defined the problem and product requirements, directed the architecture and reliability trade-offs, controlled platform credentials and deployment, reviewed the implementation, and understand the code and operational design I am submitting.

## Eligibility evidence

- [x] Solo project with one public source repository.
- [x] Working product reachable through a live URL without credentials.
- [x] Six-service architecture: frontend, API, worker, PostgreSQL, Valkey, and Qdrant.
- [x] Zerops materially builds, connects, scales, gates, and operates the system.
- [x] Only dashboard and API are public; managed data services remain private.
- [x] Public commit history and release-verification workflow.
- [x] AI assistance disclosed; original human role stated.
- [x] README explains the problem, method, evidence, limitations, and reproduction.
- [ ] Record and attach the short working-product video.
- [ ] Publish the social post from the entrant's account with `@WeMakeDevs` and `@zeropsio`.
- [ ] Paste that public post URL into the form.
- [ ] Keep the Zerops project funded and online through judging.

## Recommended 60-second judge path

1. Open the live URL; no login is required.
2. Start **Guided demo** and let the tour advance automatically.
3. Inspect the live Infrastructure Pulse and empty queue.
4. Compare baseline and evaluation points in Vector Topology.
5. Open the persisted incident and inspect its prompt, output, nearest baseline, drift distance, and routing state.
6. Open the repository and inspect `zerops.yaml`, the API outbox transaction, worker fencing, and migration chain.

## Honest operational boundary

These are not eligibility requirements and must not be presented as completed:

- [x] A bounded 500-requests/second experiment was executed and published.
- [ ] The deployment did not sustain 500 accepted requests/second.
- [ ] The deployment did not meet the sub-5-ms admission target.
- [x] Automated retention, durable vector cleanup, and legal holds are implemented.
- [ ] A composite-key partition/archive tier is still required before sustained 500-RPS approval.
- [ ] Multi-million-alert digest leasing still requires load proof or a materialized rollup.
- [ ] Real external NOTIFY and DIGEST delivery requires an authorized test destination.

The project is a live and substantive challenge entry. The unchecked scale items define the boundary between this verified deployment and a future enterprise capacity claim.
