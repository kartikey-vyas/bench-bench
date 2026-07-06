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
from bench_harness.python_client import run_for

LANGUAGE = "python"
IMPLEMENTATION = "asyncio-openai-sdk"
OUTPUT_SUBDIR = "python-openai"


async def run_one_request(
    client: Any, config: WorkloadConfig, worker_index: int, sequence: int
) -> RequestMeasurement:
    """One stream through the official OpenAI SDK — the production setup.

    The SDK SSE-decodes and pydantic-validates every chunk before yielding it,
    so timestamps here land AFTER the SDK's per-chunk work. That inflation is
    the thing this variant exists to measure. The SDK also swallows [DONE], so
    `ok` means the stream ended cleanly; completeness is still enforced by the
    aggregator via the chunk count.
    """
    started = time.perf_counter()
    first_event_at: float | None = None
    previous_event_at: float | None = None
    last_event_at: float | None = None
    max_gap_ms = 0.0
    chunks = 0
    content_bytes = 0

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

    payload = config.request_payload(worker_index, sequence, "python-openai")
    try:
        stream = await client.chat.completions.create(
            model=payload["model"],
            messages=payload["messages"],
            stream=True,
            extra_body={
                "chunks": payload["chunks"],
                "chunk_bytes": payload["chunk_bytes"],
                "ttfc_ms": payload["ttfc_ms"],
                "events_per_second": payload["events_per_second"],
                "request_id": payload["request_id"],
            },
        )
        try:
            async for chunk in stream:
                observe_event()
                if not chunk.choices:
                    continue
                content = chunk.choices[0].delta.content or ""
                if content:
                    chunks += 1
                    content_bytes += len(content.encode("utf-8"))
        finally:
            close = getattr(stream, "close", None)
            if close is not None:
                await close()
    except Exception:
        return measurement(ok=False)

    return measurement(ok=True)


async def run_benchmark(config: WorkloadConfig, output_dir: Path | None = None) -> dict[str, Any]:
    try:
        import httpx
        from openai import AsyncOpenAI
    except ImportError as exc:
        raise RuntimeError(
            "python-openai client requires the openai SDK. Run `uv sync`."
        ) from exc

    started_at = datetime.now(timezone.utc)
    # Same pool sizing as the minimal python client so the SDK is not
    # handicapped by its default connection limits at high concurrency.
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10.0, read=None, write=10.0, pool=None),
        limits=httpx.Limits(
            max_connections=config.concurrency,
            max_keepalive_connections=config.concurrency,
        ),
    )

    async with AsyncOpenAI(
        base_url=f"{config.base_url.rstrip('/')}/v1",
        api_key="synthetic",
        http_client=http_client,
        max_retries=0,
    ) as client:
        if config.warmup_seconds > 0:
            await run_for(client, config, config.warmup_seconds, run_one_request)
        measurements, duration_ms = await run_for(
            client, config, config.duration_seconds, run_one_request
        )

    result = {
        "language": LANGUAGE,
        "implementation": IMPLEMENTATION,
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

    destination = output_dir or Path(config.output_dir) / OUTPUT_SUBDIR
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


async def async_main() -> None:
    parser = argparse.ArgumentParser(description="Run the OpenAI-SDK streaming benchmark client.")
    parser.add_argument("--config", default="config/workload.smoke.json", help="Path to workload JSON.")
    parser.add_argument("--output-dir", default=None, help="Directory for summary.json.")
    args = parser.parse_args()

    config = WorkloadConfig.from_path(args.config)
    result = await run_benchmark(config, Path(args.output_dir) if args.output_dir else None)
    summary = result["summary"]
    print(
        "python-openai "
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
