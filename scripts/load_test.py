"""Rate-paced DriftGuard ingestion acceptance test with bounded concurrency."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import time
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any

import httpx


@dataclass(frozen=True, slots=True)
class LoadResult:
    target_rps: int
    duration_seconds: int
    attempted: int
    accepted: int
    errors: int
    achieved_rps: float
    wall_p50_ms: float | None
    wall_p95_ms: float | None
    wall_p99_ms: float | None
    app_p50_ms: float | None
    app_p95_ms: float | None
    app_p99_ms: float | None
    status_counts: dict[str, int]


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(percentile_value * len(ordered)) - 1))
    return round(ordered[index], 3)


def server_timing_ms(value: str | None) -> float | None:
    if not value:
        return None
    for metric in value.split(","):
        name, *parameters = metric.strip().split(";")
        if name != "app":
            continue
        for parameter in parameters:
            key, separator, raw = parameter.partition("=")
            if key.strip() == "dur" and separator:
                try:
                    parsed = float(raw)
                except ValueError:
                    return None
                return parsed if math.isfinite(parsed) and parsed >= 0 else None
    return None


async def run_load(
    *,
    url: str,
    api_key: str,
    rate: int,
    duration: int,
    concurrency: int,
) -> LoadResult:
    total = rate * duration
    queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=concurrency * 2)
    wall_latencies: list[float] = []
    app_latencies: list[float] = []
    statuses: Counter[str] = Counter()
    started = time.perf_counter()
    timeout = httpx.Timeout(connect=10, read=30, write=30, pool=30)
    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
        keepalive_expiry=30,
    )

    async with httpx.AsyncClient(
        headers={"X-API-Key": api_key},
        timeout=timeout,
        limits=limits,
        trust_env=False,
        http2=True,
    ) as client:

        async def worker() -> None:
            while True:
                sequence = await queue.get()
                try:
                    if sequence is None:
                        return
                    payload: dict[str, Any] = {
                        "session_id": f"acceptance-{sequence % 1000}",
                        "prompt_text": "Summarize the verified DriftGuard deployment state.",
                        "output_text": (
                            "DriftGuard is active on Zerops with healthy private dependencies. "
                            f"Acceptance sequence {sequence}."
                        ),
                        "metadata": {"source": "load-acceptance", "sequence": sequence},
                    }
                    request_started = time.perf_counter()
                    try:
                        response = await client.post(url, json=payload)
                    except httpx.HTTPError as exc:
                        statuses[f"error:{type(exc).__name__}"] += 1
                        continue
                    wall_latencies.append((time.perf_counter() - request_started) * 1000)
                    statuses[str(response.status_code)] += 1
                    measured = server_timing_ms(response.headers.get("server-timing"))
                    if measured is not None:
                        app_latencies.append(measured)
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        for sequence in range(total):
            due = started + (sequence / rate)
            delay = due - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            await queue.put(sequence)
        await queue.join()
        for _worker in workers:
            await queue.put(None)
        await asyncio.gather(*workers)

    elapsed = time.perf_counter() - started
    accepted = statuses.get("202", 0)
    return LoadResult(
        target_rps=rate,
        duration_seconds=duration,
        attempted=total,
        accepted=accepted,
        errors=total - accepted,
        achieved_rps=round(total / elapsed, 3),
        wall_p50_ms=percentile(wall_latencies, 0.50),
        wall_p95_ms=percentile(wall_latencies, 0.95),
        wall_p99_ms=percentile(wall_latencies, 0.99),
        app_p50_ms=percentile(app_latencies, 0.50),
        app_p95_ms=percentile(app_latencies, 0.95),
        app_p99_ms=percentile(app_latencies, 0.99),
        status_counts=dict(sorted(statuses.items())),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--rate", type=int, default=500)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=250)
    parser.add_argument("--output")
    args = parser.parse_args()
    if not 1 <= args.rate <= 5000:
        parser.error("--rate must be between 1 and 5000")
    if not 1 <= args.duration <= 600:
        parser.error("--duration must be between 1 and 600 seconds")
    if not 1 <= args.concurrency <= 2000:
        parser.error("--concurrency must be between 1 and 2000")
    return args


def main() -> int:
    args = parse_args()
    api_key = os.environ.get("DRIFTGUARD_LOAD_TEST_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("DRIFTGUARD_LOAD_TEST_API_KEY is required")
    result = asyncio.run(
        run_load(
            url=args.url,
            api_key=api_key,
            rate=args.rate,
            duration=args.duration,
            concurrency=args.concurrency,
        )
    )
    serialized = json.dumps(asdict(result), indent=2, sort_keys=True)
    print(serialized)
    if args.output:
        with open(args.output, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized + "\n")
    return 0 if result.errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
