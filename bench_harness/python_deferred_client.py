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
from bench_harness.sse import SseDecoder

LANGUAGE = "python"
IMPLEMENTATION = "asyncio-httpx-deferred"
OUTPUT_SUBDIR = "python-deferred"


class BoundaryCounter:
    """Counts SSE event boundaries (b"\\n\\n") in raw bytes using C-speed
    bytes.count, carrying a one-byte straddle flag across reads.

    Exact for this server's wire format (events are `data: {json}\\n\\n`, JSON
    keeps payload newlines escaped, so runs of more than two raw newlines
    never occur). The deferred decode after stream close re-derives the true
    event count, so even a miscount here could not corrupt correctness — only
    frame timing attribution.
    """

    def __init__(self) -> None:
        self._pending_newline = False

    def feed(self, data: bytes) -> int:
        if not data:
            return 0
        count = data.count(b"\n\n")
        if self._pending_newline and data[0:1] == b"\n":
            count += 1
        self._pending_newline = data.endswith(b"\n") and not data.endswith(b"\n\n")
        return count


async def run_one_request(
    client: Any, config: WorkloadConfig, worker_index: int, sequence: int, window_end: float
) -> RequestMeasurement:
    """One stream with all decode deferred off the hot path.

    Hot path per network read: append raw bytes, count event boundaries,
    stamp the clock. SSE decoding, JSON parsing, content counting, and
    [DONE] validation all happen after the stream closes — the measurement-
    harness strategy this variant exists to evaluate. Timing is frame-
    granular, like the drain reference.
    """
    started = time.perf_counter()
    first_event_at: float | None = None
    previous_event_at: float | None = None
    last_event_at: float | None = None
    max_gap_ms = 0.0
    counter = BoundaryCounter()
    raw_parts: list[bytes] = []
    transport_ok = True
    in_window_boundaries = 0

    def observe_frame() -> float:
        nonlocal first_event_at, previous_event_at, last_event_at, max_gap_ms
        now = time.perf_counter()
        if first_event_at is None:
            first_event_at = now
        if previous_event_at is not None:
            max_gap_ms = max(max_gap_ms, (now - previous_event_at) * 1000.0)
        previous_event_at = now
        last_event_at = now
        return now

    try:
        payload = config.request_payload(worker_index, sequence, "python-deferred")
        async with client.stream("POST", config.endpoint, json=payload) as response:
            if response.status_code != 200:
                await response.aread()
                transport_ok = False
            else:
                async for data in response.aiter_bytes():
                    raw_parts.append(data)
                    boundary_count = counter.feed(data)
                    if boundary_count > 0:
                        now = observe_frame()
                        if now <= window_end:
                            in_window_boundaries += boundary_count
    except Exception:
        transport_ok = False

    latency_ms = (time.perf_counter() - started) * 1000.0

    # ---- off the hot path: full decode + validation --------------------
    chunks = 0
    content_bytes = 0
    saw_done = False
    if raw_parts:
        decoder = SseDecoder()
        for event in decoder.feed(b"".join(raw_parts).decode("utf-8", errors="replace")):
            if event == "[DONE]":
                saw_done = True
                continue
            try:
                event_payload = json.loads(event)
            except ValueError:
                continue
            content = event_payload["choices"][0]["delta"].get("content") or ""
            if content:
                chunks += 1
                content_bytes += len(content.encode("utf-8"))

    # Frame-granular: we can't timestamp individual content chunks here (the
    # decode is deferred), only SSE event boundaries as they arrive on the
    # wire. in_window_boundaries counts boundaries (role + content + finish +
    # [DONE], whichever arrived by window_end); min() with the true content
    # count clips to a window_chunks estimate that is exact when the stream
    # completed inside the window (boundaries == chunks + 3, min = chunks)
    # and overcounts by at most 1 (the role event boundary) out of ~512 when
    # the window clips mid-stream.
    window_chunks = min(chunks, in_window_boundaries)

    first_chunk_ms = (first_event_at - started) * 1000.0 if first_event_at is not None else 0.0
    stream_ms = (
        (last_event_at - first_event_at) * 1000.0
        if first_event_at is not None and last_event_at is not None
        else 0.0
    )
    return RequestMeasurement(
        ok=transport_ok and saw_done,
        latency_ms=latency_ms,
        first_chunk_ms=first_chunk_ms,
        chunks=chunks,
        bytes=content_bytes,
        window_chunks=window_chunks,
        max_gap_ms=max_gap_ms,
        stream_ms=stream_ms,
    )


async def run_benchmark(config: WorkloadConfig, output_dir: Path | None = None) -> dict[str, Any]:
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError(
            "python-deferred client requires httpx. Run `uv sync`."
        ) from exc

    started_at = datetime.now(timezone.utc)
    timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=None)
    limits = httpx.Limits(
        max_connections=config.concurrency, max_keepalive_connections=config.concurrency
    )

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
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
            duration_window_seconds=config.duration_seconds,
        ),
    }

    destination = output_dir or Path(config.output_dir) / OUTPUT_SUBDIR
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


async def async_main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the deferred-decode Python streaming benchmark client."
    )
    parser.add_argument("--config", default="config/workload.smoke.json", help="Path to workload JSON.")
    parser.add_argument("--output-dir", default=None, help="Directory for summary.json.")
    args = parser.parse_args()

    config = WorkloadConfig.from_path(args.config)
    result = await run_benchmark(config, Path(args.output_dir) if args.output_dir else None)
    summary = result["summary"]
    print(
        "python-deferred "
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
