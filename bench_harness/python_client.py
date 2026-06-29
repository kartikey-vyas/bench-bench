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


async def run_one_request(client: Any, config: WorkloadConfig, request_index: int) -> RequestMeasurement:
    started = time.perf_counter()
    first_chunk_ms = 0.0
    chunks = 0
    content_bytes = 0
    saw_done = False

    try:
        async with client.stream("POST", config.endpoint, json=config.request_payload(request_index, "python")) as response:
            if response.status_code != 200:
                await response.aread()
                return RequestMeasurement(
                    ok=False,
                    latency_ms=(time.perf_counter() - started) * 1000.0,
                    first_chunk_ms=0.0,
                    chunks=0,
                    bytes=0,
                )

            decoder = SseDecoder()
            async for text in response.aiter_text():
                for event in decoder.feed(text):
                    if event == "[DONE]":
                        saw_done = True
                        continue

                    if chunks == 0:
                        first_chunk_ms = (time.perf_counter() - started) * 1000.0

                    payload = json.loads(event)
                    content = payload["choices"][0]["delta"].get("content", "")
                    chunks += 1
                    content_bytes += len(content.encode("utf-8"))
    except Exception:
        return RequestMeasurement(
            ok=False,
            latency_ms=(time.perf_counter() - started) * 1000.0,
            first_chunk_ms=first_chunk_ms,
            chunks=chunks,
            bytes=content_bytes,
        )

    return RequestMeasurement(
        ok=saw_done,
        latency_ms=(time.perf_counter() - started) * 1000.0,
        first_chunk_ms=first_chunk_ms,
        chunks=chunks,
        bytes=content_bytes,
    )


async def run_many(config: WorkloadConfig, total_requests: int, client: Any) -> list[RequestMeasurement]:
    semaphore = asyncio.Semaphore(config.concurrency)

    async def guarded_request(request_index: int) -> RequestMeasurement:
        async with semaphore:
            return await run_one_request(client, config, request_index)

    return await asyncio.gather(*(guarded_request(index) for index in range(total_requests)))


async def run_benchmark(config: WorkloadConfig, output_dir: Path | None = None) -> dict[str, Any]:
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError("Python client requires httpx. Install project dependencies with `uv sync`.") from exc

    started_at = datetime.now(timezone.utc)
    timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=None)

    async with httpx.AsyncClient(timeout=timeout) as client:
        if config.warmup_requests:
            await run_many(config, config.warmup_requests, client)

        measured_start = time.perf_counter()
        measurements = await run_many(config, config.total_requests, client)
        duration_ms = (time.perf_counter() - measured_start) * 1000.0

    result = {
        "language": "python",
        "implementation": "asyncio-httpx",
        "started_at": started_at.isoformat(),
        "config": config.result_config(),
        "summary": aggregate_summary(measurements, duration_ms),
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
        f"failures={summary['failed_requests']}"
    )


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
