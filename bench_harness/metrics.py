from __future__ import annotations

from dataclasses import dataclass
from math import ceil


@dataclass(frozen=True)
class RequestMeasurement:
    ok: bool
    latency_ms: float
    first_chunk_ms: float
    chunks: int
    bytes: int
    max_gap_ms: float
    stream_ms: float


def percentile(values: list[float], rank: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, ceil(rank * len(ordered)) - 1))
    return ordered[index]


def mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def aggregate_summary(
    measurements: list[RequestMeasurement],
    duration_ms: float,
    expected_chunks: int,
    events_per_second: int,
    concurrency: int,
    ttfc_ms: int,
) -> dict[str, float | int]:
    successful = [m for m in measurements if m.ok and m.chunks == expected_chunks]
    incomplete = [m for m in measurements if m.ok and m.chunks != expected_chunks]
    failed = [m for m in measurements if not m.ok]

    latencies = [m.latency_ms for m in successful]
    first_chunks = [m.first_chunk_ms for m in successful]
    max_gaps = [m.max_gap_ms for m in successful]
    total_chunks = sum(m.chunks for m in successful)
    total_bytes = sum(m.bytes for m in successful)

    duration_seconds = duration_ms / 1000.0 if duration_ms > 0 else 0.0
    chunks_per_second = total_chunks / duration_seconds if duration_seconds else 0.0

    ideal_stream_ms = (
        (expected_chunks - 1) / events_per_second * 1000.0
        if events_per_second > 0 and expected_chunks > 1
        else 0.0
    )
    stretches = (
        [m.stream_ms / ideal_stream_ms for m in successful] if ideal_stream_ms > 0 else []
    )
    if events_per_second > 0:
        ideal_request_seconds = ttfc_ms / 1000.0 + (expected_chunks - 1) / events_per_second
        ideal_events_per_second = (
            concurrency * expected_chunks / ideal_request_seconds
            if ideal_request_seconds > 0
            else 0.0
        )
    else:
        ideal_events_per_second = 0.0
    efficiency = (
        chunks_per_second / ideal_events_per_second if ideal_events_per_second > 0 else 0.0
    )

    return {
        "duration_ms": duration_ms,
        "successful_requests": len(successful),
        "incomplete_requests": len(incomplete),
        "failed_requests": len(failed),
        "total_chunks": total_chunks,
        "total_bytes": total_bytes,
        "requests_per_second": len(successful) / duration_seconds if duration_seconds else 0.0,
        "chunks_per_second": chunks_per_second,
        "mean_request_latency_ms": mean(latencies),
        "p50_request_latency_ms": percentile(latencies, 0.50),
        "p95_request_latency_ms": percentile(latencies, 0.95),
        "p99_request_latency_ms": percentile(latencies, 0.99),
        "mean_time_to_first_chunk_ms": mean(first_chunks),
        "p50_time_to_first_chunk_ms": percentile(first_chunks, 0.50),
        "p95_time_to_first_chunk_ms": percentile(first_chunks, 0.95),
        "p99_time_to_first_chunk_ms": percentile(first_chunks, 0.99),
        "p50_max_gap_ms": percentile(max_gaps, 0.50),
        "p95_max_gap_ms": percentile(max_gaps, 0.95),
        "p99_max_gap_ms": percentile(max_gaps, 0.99),
        "max_max_gap_ms": max(max_gaps) if max_gaps else 0.0,
        "p50_stream_stretch": percentile(stretches, 0.50),
        "p95_stream_stretch": percentile(stretches, 0.95),
        "p99_stream_stretch": percentile(stretches, 0.99),
        "ideal_events_per_second": ideal_events_per_second,
        "efficiency": efficiency,
    }
