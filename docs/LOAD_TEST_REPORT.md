# DriftGuard Zerops Load Acceptance Report

## Abstract

This report records a bounded production acceptance test against the public DriftGuard API deployed on Zerops. The objective was to test the product requirement of 500 authenticated telemetry admissions per second for 60 seconds and a sub-5 millisecond admission path. The deployment remained recoverable, but neither target was met. Results are retained without adjustment so challenge judges and future operators can distinguish a working live product from an unproven enterprise-capacity claim.

## Method

| Parameter | Value |
| --- | --- |
| Date | 9 August 2026 |
| Target | `POST /api/v1/logs` on the public Zerops API |
| Offered load | 500 requests/second for 60 seconds |
| Attempted requests | 30,000 |
| Authentication | Real project API key supplied only through process environment |
| Client | Rate-paced asynchronous HTTP/2 runner outside Zerops |
| Concurrency ceiling | 250 |
| Payload labeling | `metadata.source=load-acceptance` |
| Server measurement | ASGI `Server-Timing: app;dur=...` |

The runner is reproducible with [scripts/load_test.py](../scripts/load_test.py). Public wall time includes internet transit, load-balancer queuing, and application work. `Server-Timing` measures the API's own request lifecycle and is reported separately.

## Results

| Metric | Result |
| --- | ---: |
| Accepted (`202`) | 24,209 |
| Acceptance ratio | 80.70% |
| Errors | 5,791 |
| Effective completed throughput | 115.259 requests/second |
| Public wall p50 | 2,340.221 ms |
| Public wall p95 | 3,742.807 ms |
| Public wall p99 | 4,172.703 ms |
| Application p50 | 327.255 ms |
| Application p95 | 906.720 ms |
| Application p99 | 1,183.154 ms |
| HTTP 502 | 7 |
| Client local-protocol failures | 5,052 |
| Client remote-protocol failures | 732 |

### Acceptance decision

- Sustained 500 requests/second: **failed**.
- Sub-5 ms application admission: **failed**.
- Sub-5 ms public admission: **failed**.
- Data integrity/recovery: **passed**; the services recovered and labeled test data was removed safely.

The result is not solely a server benchmark because the external generator also saturated and emitted protocol failures. That limitation cannot reverse the acceptance decision: application p95 was 906.720 ms under the offered load, far above the target.

## Recovery evidence

After the test, workers were stopped cleanly and the admin-only maintenance endpoint removed 25,272 explicitly labeled acceptance runs in three bounded transactions. The endpoint atomically preserved unrelated Valkey messages and committed Qdrant point deletion to the PostgreSQL retention outbox. Workers were restarted, then reported:

- Valkey queue depth: `0`;
- worker status: `healthy`;
- PostgreSQL status: `healthy`;
- Qdrant status: `healthy`;
- Qdrant point count: `54`.

## Capacity decision

The challenge deployment remains suitable for live judging and bounded demonstrations. It is not approved for 500 requests/second. A future capacity release requires a generator inside the Zerops private network, dedicated rather than shared CPU, database/pool profiling, a measured batching strategy, queue-delay SLOs, and a repeat test with zero admission errors.
