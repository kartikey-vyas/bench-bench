from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bench_harness.config import WorkloadConfig
from bench_harness.metrics import RequestMeasurement, aggregate_summary
from bench_harness.sse import SseDecoder


async def run_one_request(
    client: Any, config: WorkloadConfig, worker_index: int, sequence: int
) -> RequestMeasurement:
    started = time.perf_counter()
    first_event_at: float | None = None
    previous_event_at: float | None = None
    last_event_at: float | None = None
    max_gap_ms = 0.0
    chunks = 0
    content_bytes = 0
    saw_done = False

    def observe_event() -> None:
        nonlocal first_event_at, previous_event_at, last_event_at, max_gap_ms
        now = time.perf_counter()
        if first_event_at is None:
            first_event_at = now
        if previous_event_at is not None:
            max_gap_ms = max(max_gap_ms, (now - previous_event_at) * 1000.0)
        previous_event_at = now
        last_event_at = now

    def measurement(ok: bool) -> RequestMeasurement:
        first_chunk_ms = (
            (first_event_at - started) * 1000.0 if first_event_at is not None else 0.0
        )
        stream_ms = (
            (last_event_at - first_event_at) * 1000.0
            if first_event_at is not None and last_event_at is not None
            else 0.0
        )
        return RequestMeasurement(
            ok=ok,
            latency_ms=(time.perf_counter() - started) * 1000.0,
            first_chunk_ms=first_chunk_ms,
            chunks=chunks,
            bytes=content_bytes,
            max_gap_ms=max_gap_ms,
            stream_ms=stream_ms,
        )

    try:
        payload = config.request_payload(worker_index, sequence, "python")
        async with client.stream("POST", config.endpoint, json=payload) as response:
            if response.status_code != 200:
                await response.aread()
                return measurement(ok=False)

            decoder = SseDecoder()
            async for text in response.aiter_text():
                for event in decoder.feed(text):
                    observe_event()
                    if event == "[DONE]":
                        saw_done = True
                        continue
                    event_payload = json.loads(event)
                    content = event_payload["choices"][0]["delta"].get("content") or ""
                    if content:
                        chunks += 1
                        content_bytes += len(content.encode("utf-8"))
    except Exception:
        return measurement(ok=False)

    return measurement(ok=saw_done)


async def run_for(
    client: Any, config: WorkloadConfig, seconds: float
) -> tuple[list[RequestMeasurement], float]:
    measurements: list[RequestMeasurement] = []
    started = time.perf_counter()
    deadline = started + seconds

    async def worker(worker_index: int) -> None:
        sequence = 0
        while time.perf_counter() < deadline:
            measurements.append(await run_one_request(client, config, worker_index, sequence))
            sequence += 1

    await asyncio.gather(*(worker(index) for index in range(config.concurrency)))
    return measurements, (time.perf_counter() - started) * 1000.0


async def run_benchmark(config: WorkloadConfig, output_dir: Path | None = None) -> dict[str, Any]:
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError(
            "Python client requires httpx. Install project dependencies with `uv sync`."
        ) from exc

    started_at = datetime.now(timezone.utc)
    timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=None)
    # The default pool caps at 100 connections, which would silently serialize
    # higher concurrencies; size the pool to the workload.
    limits = httpx.Limits(
        max_connections=config.concurrency, max_keepalive_connections=config.concurrency
    )

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        if config.warmup_seconds > 0:
            await run_for(client, config, config.warmup_seconds)
        measurements, duration_ms = await run_for(client, config, config.duration_seconds)

    result = {
        "language": "python",
        "implementation": "asyncio-httpx",
        "started_at": started_at.isoformat(),
        "config": config.result_config(),
        "summary": aggregate_summary(
            measurements,
            duration_ms,
            expected_chunks=config.chunks_per_response,
            events_per_second=config.events_per_second,
            concurrency=config.concurrency,
            ttfc_ms=config.ttfc_ms,
        ),
    }

    destination = output_dir or Path(config.output_dir) / "python"
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


async def async_main() -> None:
    parser = argparse.ArgumentParser(description="Run the Python streaming benchmark client.")
    parser.add_argument("--config", default="config/workload.smoke.json", help="Path to workload JSON.")
    parser.add_argument("--output-dir", default=None, help="Directory for summary.json.")
    args = parser.parse_args()

    config = WorkloadConfig.from_path(args.config)
    result = await run_benchmark(config, Path(args.output_dir) if args.output_dir else None)
    summary = result["summary"]
    print(
        "python "
        f"requests/s={summary['requests_per_second']:.2f} "
        f"chunks/s={summary['chunks_per_second']:.2f} "
        f"efficiency={summary['efficiency']:.3f} "
        f"failures={summary['failed_requests']} "
        f"incomplete={summary['incomplete_requests']}"
    )


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
