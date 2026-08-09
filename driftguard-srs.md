# Software Requirements Specification (SRS)
## Project Name: DriftGuard
### Production LLM-Output Drift & Reliability Monitor on Zerops

---

## 1. System Overview & Core Algorithms

### 1.1 Project Purpose
**DriftGuard** is a highly available, multi-service developer tool designed to monitor the semantic drift, reliability, and factual integrity of production LLM outputs over time [350]. The platform acts as an automated reliability firewall, stashing gold-standard baseline evaluation datasets and incoming production logs, comparing their semantic profiles, and triggering sophisticated alert-routing logic (Notify, Digest, or Mute) when drift is detected [350, 351]. 

The primary business logic of DriftGuard is designed to port a peer-reviewed academic research pipeline (**DriftShield**, utilizing a BioBERT backbone) into an enterprise-grade SaaS environment [349, 350].

### 1.2 Core Algorithm: Semantic Drift Distance
Semantic drift is computed by comparing incoming production LLM output text strings against pre-established gold-standard baselines representing acceptable model behavior [350, 351]. 

The system utilizes a 3-step vector comparison pipeline:
1. **Embedding Generation:** Incoming text logs are processed via a local HuggingFace `sentence-transformers` pipeline using the `all-MiniLM-L6-v2` model (384-dimensional dense vectors) or a specialized BioBERT model [20, 23, 350].
2. **Vector Space Stashing:** Vectors are stashed in a managed **Qdrant Vector Database** [12, 13, 351].
3. **Drift Metric Calculation:** Semantic drift distance is calculated as the **Cosine Distance** ($D_{cos}$) between the incoming vector $\vec{u}$ and the nearest-neighbor baseline vector $\vec{v}$ in the gold-standard collection [25, 275, 351]:
   
   $$D_{cos}(u, v) = 1 - \frac{\vec{u} \cdot \vec{v}}{\|\vec{u}\| \|\vec{v}\|}$$

If $D_{cos}(u, v)$ exceeds a user-defined threshold $\tau_{drift}$ (stashed in PostgreSQL), the event is flagged as an active drift anomaly, triggering notification workflows [9, 350].

---

## 2. Distributed Microservices Topology

DriftGuard is architected as a decoupled, event-driven multi-service platform running on a dedicated private network [204, 312].

```
               [ Public Internet ]
                        │
                        ▼
                 +──────────────+
                 | L7 Balancer  | (Zerops Managed Ingress & SSL)
                 +──────┬───────+
                        │
         ┌──────────────┴──────────────┐
         │ (HTTP/HTTPS)                │ (HTTP/HTTPS)
         ▼                             ▼
+─────────────────+           +─────────────────+
|    frontend     |           |       api       |
| (Next.js UI)    |           | (FastAPI Ingest)|
+─────────────────+           +────────┬────────+
                                       │
                     ┌─────────────────┼─────────────────┐
                     │                 │                 │
                     ▼                 ▼                 ▼
              +─────────────+   +─────────────+   +─────────────+
              |    cache    |   |     db      |   |   storage   |
              |  (Valkey)   |   | (PostgreSQL)|   | (S3 Object) |
              +──────┬──────+   +──────┬──────+   +─────────────+
                     │                 │
                     └────────┬────────┘
                              │ (Private Network)
                              ▼
                      +─────────────────+
                      |     worker      | (Processes Embeddings &
                      | (Python Worker) |  Nearest-Neighbor Searches)
                      +────────┬────────+
                               │
                               ▼
                      +─────────────────+
                      |     qdrant      | (Managed Vector Database
                      |  (Vector DB)    |  exposing REST & gRPC)
                      +─────────────────+
```

### 2.1 Private VXLAN Network Architecture
All services reside within a Zerops-managed virtual private network (VXLAN) [386]. Internal service-to-service communication occurs securely over unencrypted HTTP/TCP protocols using service hostnames as DNS targets [251, 256]. **No internal databases or caches are exposed to the public internet** [213].

### 2.2 Network & Communication Protocol Matrix
| Hostname | Service Role | Internal Port | Protocol | Exposed Publicly? |
| :--- | :--- | :--- | :--- | :--- |
| `frontend` | Next.js Dashboard UI | `3000` | HTTP / TCP | Yes (`.zerops.app` / Custom Domain) [112, 279] |
| `api` | FastAPI Ingestion Engine | `8000` | HTTP / TCP | Yes (`.zerops.app` / Custom Domain) [251] |
| `cache` | Valkey Broker / Queue | `6379` | Redis / TCP | No (Private Network Only) [131] |
| `db` | PostgreSQL Metadata Store | `5432` | SQL / TCP | No (Private Network Only) [116, 254] |
| `worker` | Python Background Processor | N/A | Daemon Loop | No (No listening ports) |
| `qdrant` | Qdrant Vector Search Engine | `6333` (REST), `6334` (gRPC) | HTTP / gRPC | No (Private Network Only) [261] |
| `storage` | Object Storage Ingest Archive | `443` | S3 API / HTTPS| No (Private Network Only, presigned URLs provided) [7] |

---

## 3. Zerops Environment Variables & Reference Blueprint

To achieve strict credential isolation (satisfying advanced security standards), DriftGuard implements the default **Zerops `service` Isolation Mode** [127]. Environment variables are not globally leaked; instead, services must explicitly reference credentials generated by target databases and caches using Zerops' cross-service syntax [127, 129].

### 3.1 Automatic System Variable Extraction Blueprint
When databases, caches, and storage systems are provisioned, Zerops generates credentials based on the destination hostname [113, 253]:
- Hostname `db` $\rightarrow$ Variables: `${db_hostname}`, `${db_user}`, `${db_password}`, `${db_connectionString}` [113].
- Hostname `cache` $\rightarrow$ Variables: `${cache_hostname}`, `${cache_password}` [131].
- Hostname `qdrant` $\rightarrow$ Variables: `${qdrant_hostname}`, `${qdrant_password}` [351].
- Hostname `storage` $\rightarrow$ Variables: `${storage_hostname}`, `${storage_accessKeyId}`, `${storage_secretAccessKey}`, `${storage_bucketName}` [2].

### 3.2 Service Environment Mapping (`envVariables` block)

#### Ingestion API (`api` service)
```yaml
run:
  envVariables:
    PORT: 8000
    DATABASE_URL: ${db_connectionString}
    VALKEY_HOST: ${cache_hostname}
    VALKEY_PORT: 6379
    VALKEY_PASSWORD: ${cache_password}
```

#### Python Background Worker (`worker` service)
```yaml
run:
  envVariables:
    DATABASE_URL: ${db_connectionString}
    VALKEY_HOST: ${cache_hostname}
    VALKEY_PORT: 6379
    VALKEY_PASSWORD: ${cache_password}
    QDRANT_HOST: ${qdrant_hostname}
    QDRANT_PORT: 6333
    QDRANT_API_KEY: ${qdrant_password}
    S3_ENDPOINT: ${storage_hostname}
    S3_ACCESS_KEY: ${storage_accessKeyId}
    S3_SECRET_KEY: ${storage_secretAccessKey}
    S3_BUCKET: ${storage_bucketName}
```

---

## 4. Data Schemas

To prevent split-brain issues and database performance degradation, DriftGuard stashes relational, transactional metadata in **PostgreSQL** and dense high-dimensional vectors in **Qdrant** [313, 351].

### 4.1 PostgreSQL Schema Design (SQL DDL)

```sql
-- PostgreSQL Migrations File

-- Table 1: LLM Log Ingestion Tracker
CREATE TABLE logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id VARCHAR(100) NOT NULL,
    prompt_text TEXT NOT NULL,
    output_text TEXT NOT NULL,
    metadata JSONB,
    status VARCHAR(50) DEFAULT 'queued', -- queued, processing, completed, failed
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- Table 2: Evaluation Runs (Grouping related logs)
CREATE TABLE evaluations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    log_id UUID REFERENCES logs(id) ON DELETE CASCADE,
    drift_distance DOUBLE PRECISION,
    matched_baseline_id VARCHAR(100),
    is_anomaly BOOLEAN DEFAULT FALSE,
    evaluated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- Table 3: Alert Configurations & Routing Rules
CREATE TABLE alert_rules (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    threshold DOUBLE PRECISION NOT NULL DEFAULT 0.15, -- Max allowable Cosine Distance
    action_type VARCHAR(50) NOT NULL, -- NOTIFY, DIGEST, MUTE
    notification_target VARCHAR(255) NOT NULL, -- Email, Slack webhook
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- Table 4: Incident Alarm Alerts Log
CREATE TABLE alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    evaluation_id UUID REFERENCES evaluations(id) ON DELETE CASCADE,
    rule_id INTEGER REFERENCES alert_rules(id) ON DELETE CASCADE,
    alert_status VARCHAR(50) DEFAULT 'triggered', -- triggered, resolved, snoozed
    notified_at TIMESTAMPTZ
);

-- Table 5: Transactional Outbox for Resilient Job Queueing
CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- pending, processed, failed
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

-- Indexes for performance
CREATE INDEX idx_logs_session_id ON logs(session_id);
CREATE INDEX idx_evaluations_log_id ON evaluations(log_id);
CREATE INDEX idx_logs_created_at ON logs(created_at DESC);
```

### 4.2 Qdrant Vector Collection Structure
The Qdrant collection must be created with a dense vector parameter matching the output size of the chosen BioBERT or MiniLM sentence transformer [23, 263]:
- **Collection Name:** `drift_baselines` (Contains gold-standard reference points) [263, 351].
- **Vector Parameters:** Size = `384` (for `all-MiniLM-L6-v2`), Distance Metric = `Cosine` [263, 351].
- **Collection Payload Layout:**
```json
{
  "id": "uuid-v4-string",
  "vector": [0.015, -0.045, 0.123, "..."], 
  "payload": {
    "baseline_set": "v1.2-gold-standard",
    "prompt_context": "medical_triage_clinical_advice",
    "expected_topics": ["symptoms", "dosage", "contraindications"],
    "created_at": "2026-08-08T11:30:16Z"
  }
}
```

---

## 5. Decoupled Asynchronous Processing Design

DriftGuard leverages the **Outbox Pattern** to ensure transactional reliability when logs are ingested [315, 332]. Ingesting logs must never block user traffic or risk data loss under high volume [320].

```
[Inbound Log API] 
       │
       ▼ (Atomically write in single DB Transaction)
+──────────────────────────────────────────────+
|   PostgreSQL 'logs' & 'outbox' Tables        |
+──────────────────────────────────────────────+
       │
       ▼ (Resilient Poller/Trigger Thread)
[Valkey Task Queue (Redis List `tasks`)] 
       │
       ▼ (BLPOP block-blocking consume loop)
[Python Background Worker Runtime]
       │
       ├───► 1. Pull output string & generate 384-d embeddings via local Model [23].
       ├───► 2. Execute vector nearest-neighbor search in Qdrant [272].
       ├───► 3. Compare returned distance with thresholds in alert_rules [352].
       └───► 4. Populate PostgreSQL evaluations & trigger alerts [352].
```

### 5.1 Outbox Poller Trigger Loop (API Engine)
The Ingestion API exposes a high-throughput endpoint `/api/v1/logs` [352]. When called, it writes the log to the `logs` table and a corresponding job creation record to the `outbox` table in a **single database transaction** [315, 332]:

```python
# API Transaction Pseudocode
async def ingest_log(log_payload, db_session, valkey_client):
    async with db_session.transaction():
        # 1. Insert Log Metadata
        log_record = await db_session.insert(logs_table, log_payload)
        
        # 2. Insert Outbox Event
        outbox_payload = {"log_id": log_record.id, "output_text": log_payload.output_text}
        outbox_record = await db_session.insert(outbox_table, {"event_type": "LOG_INGESTED", "payload": outbox_payload})
    
    # 3. Non-blocking fire-and-forget push to Valkey Queue
    await valkey_client.rpush("tasks", json.dumps({"log_id": log_record.id}))
    # Update outbox status asynchronously on success
    await db_session.execute(update(outbox_table).where(id=outbox_record.id).values(status="processed"))
```

### 5.2 Python Worker Consumption Loop (`BLPOP` pattern) [20]
The worker service maintains a long-lived blocking pop loop (`BLPOP`) against Valkey, preventing CPU polling overhead [20, 32]:

```python
# Worker Loop Pseudocode
import valkey
import psycopg2
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# System initialization
vk = valkey.Valkey(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD)
qdrant_client = QdrantClient(url=f"http://{QDRANT_HOST}:{QDRANT_PORT}", api_key=QDRANT_API_KEY)
model = SentenceTransformer('all-MiniLM-L6-v2') # Heavy local NLP model loaded into container memory [20]

def main_loop():
    while True:
        # Blocking list pop with 10-second timeout
        job_data = vk.blpop("tasks", timeout=10)
        if not job_data:
            continue
            
        _, payload_str = job_data
        job = json.loads(payload_str)
        log_id = job["log_id"]
        
        # 1. Retrieve log details from Postgres
        log_text = fetch_log_text_from_postgres(log_id)
        
        # 2. Compute 384-dimensional dense vectors
        embedding = model.encode(log_text).tolist()
        
        # 3. Query Qdrant for nearest neighbor
        search_results = qdrant_client.search(
            collection_name="drift_baselines",
            query_vector=embedding,
            limit=1
        )
        
        # 4. Process evaluation distance
        if search_results:
            match = search_results[0]
            distance = 1.0 - match.score # Convert similarity score to Cosine Distance
            baseline_id = match.id
            
            # Check thresholds in database and create evaluations
            process_alert_thresholds(log_id, baseline_id, distance)
```

---

## 6. Production-Ready `zerops.yaml` Specification

This `zerops.yaml` provides the complete build, run, and environment deployment manifest for all services of the DriftGuard project on Zerops [112]. It is fully optimized with readiness checks to eliminate start-up race conditions [112, 320, 398].

```yaml
# zerops.yaml - Multi-Service Orchestration Manifest [391]
zerops:
  # Next.js Frontend UI Dashboard Service [9, 112]
  - setup: frontend
    build:
      base: nodejs@20 # Setup runtime environment base [40, 112]
      buildCommands:
        - npm ci
        - npm run build
      deployFiles:
        - .next
        - public
        - package.json
        - node_modules
      cache:
        - node_modules # Cache packages for speed [40]
    run:
      base: nodejs@20
      ports:
        - port: 3000
          httpSupport: true # Informs load-balancer to map public HTTP routing [112, 400]
      envVariables:
        NEXT_PUBLIC_API_URL: http://api:8000 # Directs private network HTTP target [251, 254]
      start: npm run start
      healthCheck:
        httpGet:
          port: 3000
          path: /api/health # Ensure frontend app endpoint returns 200 OK [398]

  # FastAPI High-Throughput Ingestion API Service [9, 112]
  - setup: api
    build:
      base: python@3.12
      buildCommands:
        - pip install -r requirements.txt
      deployFiles:
        - app
        - requirements.txt
    run:
      base: python@3.12
      ports:
        - port: 8000
          httpSupport: true
      envVariables:
        PORT: 8000
        # In service isolation mode, explicitly fetch from target host DB [112, 127]
        DATABASE_URL: ${db_connectionString}
        VALKEY_HOST: ${cache_hostname}
        VALKEY_PORT: 6379
        VALKEY_PASSWORD: ${cache_password}
      start: uvicorn app.main:app --host 0.0.0.0 --port 8000
      healthCheck:
        httpGet:
          port: 8000
          path: /status # FastAPI health routing [112]

  # Heavy Background Python Processing Worker [9, 112]
  - setup: worker
    build:
      base: python@3.12
      buildCommands:
        - pip install -r requirements.txt
      deployFiles:
        - worker
        - requirements.txt
    run:
      base: python@3.12
      envVariables:
        DATABASE_URL: ${db_connectionString}
        VALKEY_HOST: ${cache_hostname}
        VALKEY_PORT: 6379
        VALKEY_PASSWORD: ${cache_password}
        QDRANT_HOST: ${qdrant_hostname}
        QDRANT_PORT: 6333
        QDRANT_API_KEY: ${qdrant_password}
        S3_ENDPOINT: ${storage_hostname}
        S3_ACCESS_KEY: ${storage_accessKeyId}
        S3_SECRET_KEY: ${storage_secretAccessKey}
        S3_BUCKET: ${storage_bucketName}
      # The worker has no HTTP/TCP listening ports, it runs a loop
      start: python -m worker.main
      healthCheck:
        exec:
          # Verify worker daemon process is actively listening and alive
          command: ps aux | grep "python -m worker.main" | grep -v "grep" [401]
```

---

## 7. "Infrastructure Pulse" UI Specifications

To score maximum points under the **"Use of Zerops"** judging criterion, DriftGuard embeds observability directly into the product experience [312, 320]. A dedicated page titled **"Infrastructure Pulse"** displays the dynamic operational health of your microservices network, providing visual proof to the judges of your robust multi-service backend [320, 331].

### 7.1 Backend Diagnostic Endpoint (`GET /api/v1/diagnostics/pulse`)
The FastAPI application provides a centralized diagnostics endpoint that performs quick, non-blocking check operations across all private attachments [320, 332]:

```python
# app/api/diagnostics.py
from fastapi import APIRouter
import time
import valkey
import psycopg2
from qdrant_client import QdrantClient

router = APIRouter()

@router.get("/diagnostics/pulse")
async def get_infrastructure_pulse():
    pulse_report = {
        "timestamp": time.time(),
        "services": {}
    }
    
    # 1. Check Valkey Message Queue [320, 332]
    try:
        t0 = time.perf_counter()
        vk = valkey.Valkey(host=VALKEY_HOST, port=6379, password=VALKEY_PASSWORD, socket_timeout=1)
        queue_len = vk.llen("tasks") # Current backlogged tasks [320, 332]
        latency = (time.perf_counter() - t0) * 1000
        pulse_report["services"]["cache"] = {
            "status": "HEALTHY",
            "latency_ms": round(latency, 2),
            "queue_depth": queue_len
        }
    except Exception as e:
        pulse_report["services"]["cache"] = {"status": "UNHEALTHY", "error": str(e)}

    # 2. Check PostgreSQL Connection [320]
    try:
        t0 = time.perf_counter()
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=1)
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM logs;")
            total_logs = cursor.fetchone()[0]
        conn.close()
        latency = (time.perf_counter() - t0) * 1000
        pulse_report["services"]["postgres_db"] = {
            "status": "HEALTHY",
            "latency_ms": round(latency, 2),
            "total_records": total_logs
        }
    except Exception as e:
        pulse_report["services"]["postgres_db"] = {"status": "UNHEALTHY", "error": str(e)}

    # 3. Check Qdrant Vector Cluster [320, 332]
    try:
        t0 = time.perf_counter()
        qd = QdrantClient(url=f"http://{QDRANT_HOST}:{QDRANT_PORT}", api_key=QDRANT_API_KEY, timeout=1)
        collection_info = qd.get_collection("drift_baselines")
        latency = (time.perf_counter() - t0) * 1000
        pulse_report["services"]["qdrant_vectors"] = {
            "status": "HEALTHY",
            "latency_ms": round(latency, 2),
            "total_vectors": collection_info.vectors_count
        }
    except Exception as e:
        pulse_report["services"]["qdrant_vectors"] = {"status": "UNHEALTHY", "error": str(e)}

    return pulse_report
```

### 7.2 Frontend Dashboard Implementation Requirement
- **UI Element:** Render a dashboard view containing grid cards for **Valkey Queue Depth**, **Qdrant Vector Count**, and **Postgres Log Count** [332].
- **Dynamic Updates:** Automatically poll the `/diagnostics/pulse` endpoint every 3000ms using SWR or React Query.
- **Visual Indication:** Display colored indicators (Green for HEALTHY, Pulsing Red for UNHEALTHY) alongside latency milliseconds charts. This exposes the "load-bearing" nature of your Zerops setup immediately to the judges [312, 320, 325].

---

## 8. End-to-End Verification & Failure Mode Recovery

### 8.1 4-Step Verification Workflow
Your agentic IDE must implement and verify the system using these four strict test phases:
1. **Infrastructure Deploy Sanity Check:** Ensure all 6 Zerops services are in green `ACTIVE` state. Curl the `api/status` endpoint to verify database connection initialization [352].
2. **Gold Baseline Injection:** Push a set of 50 prompt-response vectors to the `drift_baselines` collection in Qdrant [352].
3. **Drift Detection Ingestion Test:** Post a payload to `/api/v1/logs` containing a highly anomalous output string. 
4. **Asynchronous Verification Check:** Query PostgreSQL to confirm that:
   - A task record was properly processed [363].
   - An evaluation entry was successfully created with a valid calculated drift distance [352].
   - An active alarm alert record was stashed in the `alerts` table [352].

### 8.2 Failure Mode Recovery Matrix

| Failure Mode | Root Cause | Detection Signal | Automated Mitigation & Remediation |
| :--- | :--- | :--- | :--- |
| **Worker Container OOM** [323] | Loading a massive document or large batch payload directly into worker container memory. | Worker container state transitions to `FAILED` with exit code 137. | 1. Implement strict incoming payload limits (max 50KB string per API call).<br>2. Stream NLP embedding computation in small transactional batches inside the background worker instead of parsing everything into RAM at once [323]. |
| **Outbox Desync** [323] | PostgreSQL successfully processes transaction but API worker misses publishing to Valkey. | Outbox status remains stuck in `pending` status for >60 seconds. | Implement a lightweight Python script (`cron` task) in the API running every 60 seconds to scan for stale pending outbox logs, republishing them to the Valkey list queue [323]. |
| **Valkey Connection Dropout** | Temporary high-load network pressure on cache container. | API logs show continuous Valkey socket/network timeout exceptions. | Implement retries with exponential backoff on both the FastAPI publisher client and the Python worker consumer loop [173]. |
| **Qdrant Timeout / Cold-Start** [323] | Managed Qdrant vector database experiences high initial query latency under heavy concurrency. | Evaluations endpoint times out with a 504 Gateway error. | 1. Implement a circuit breaker on vector searches.<br>2. Optimize vector payloads using Qdrant payload indexing to accelerate search times [275]. |
