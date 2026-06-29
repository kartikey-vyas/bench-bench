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


def aggregate_summary(measurements: list[RequestMeasurement], duration_ms: float) -> dict[str, float | int]:
    successful = [measurement for measurement in measurements if measurement.ok]
    failed = [measurement for measurement in measurements if not measurement.ok]
    latencies = [measurement.latency_ms for measurement in successful]
    first_chunks = [measurement.first_chunk_ms for measurement in successful]
    total_chunks = sum(measurement.chunks for measurement in successful)
    total_bytes = sum(measurement.bytes for measurement in successful)
    duration_seconds = duration_ms / 1000.0 if duration_ms > 0 else 0.0

    return {
        "duration_ms": duration_ms,
        "successful_requests": len(successful),
        "failed_requests": len(failed),
        "total_chunks": total_chunks,
        "total_bytes": total_bytes,
        "requests_per_second": len(successful) / duration_seconds if duration_seconds else 0.0,
        "chunks_per_second": total_chunks / duration_seconds if duration_seconds else 0.0,
        "mean_request_latency_ms": mean(latencies),
        "p50_request_latency_ms": percentile(latencies, 0.50),
        "p95_request_latency_ms": percentile(latencies, 0.95),
        "p99_request_latency_ms": percentile(latencies, 0.99),
        "mean_time_to_first_chunk_ms": mean(first_chunks),
        "p50_time_to_first_chunk_ms": percentile(first_chunks, 0.50),
        "p95_time_to_first_chunk_ms": percentile(first_chunks, 0.95),
        "p99_time_to_first_chunk_ms": percentile(first_chunks, 0.99),
        "per_chunk_overhead_ms": duration_ms / total_chunks if total_chunks else 0.0,
    }
