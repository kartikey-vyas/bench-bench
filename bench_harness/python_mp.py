"""Multiprocess wrapper for the Python client variants.

Mirrors the production deployment shape: N worker processes (default 12),
each running its own asyncio event loop over a slice of the total
concurrency. Each child reuses the exact single-process collection logic
(`collect_measurements`) of its variant, so the only difference between
`python-openai` and `python-openai-mp` is the process fan-out — which is
precisely the variable the comparison isolates.

Aggregation is sound under the window-clipped counting contract: every
child clips chunk counts to its own measured window and the parent sums
them over the configured window duration, so per-child start skew (process
spawn, ~hundreds of ms against a 60s window) cannot dilute the aggregate.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bench_harness.config import WorkloadConfig
from bench_harness.metrics import RequestMeasurement, aggregate_summary

DEFAULT_PROCESSES = 12

VARIANTS = ("openai", "deferred")


def split_concurrency(total: int, processes: int) -> list[int]:
    """Distribute total concurrency across processes as evenly as possible,
    never spawning a process with zero workers."""
    processes = max(1, min(processes, total))
    base, extra = divmod(total, processes)
    return [base + (1 if index < extra else 0) for index in range(processes)]


def _collector(variant: str):
    if variant == "openai":
        from bench_harness.python_openai_client import collect_measurements
    elif variant == "deferred":
        from bench_harness.python_deferred_client import collect_measurements
    else:
        raise ValueError(f"unknown variant {variant!r}; expected one of {VARIANTS}")
    return collect_measurements


def _run_child(variant: str, config_data: dict, slice_concurrency: int) -> list[RequestMeasurement]:
    """Executed in a spawned worker process: run the variant's single-process
    collection over this child's concurrency slice and ship the raw
    measurements back to the parent (frozen dataclasses pickle cleanly)."""
    config = WorkloadConfig(**{**config_data, "concurrency": slice_concurrency})
    collect = _collector(variant)
    measurements, _duration_ms = asyncio.run(collect(config))
    return measurements


async def run_benchmark(
    config: WorkloadConfig,
    output_dir: Path | None = None,
    variant: str = "openai",
    processes: int = DEFAULT_PROCESSES,
) -> dict[str, Any]:
    _collector(variant)  # validate the variant before spawning anything
    started_at = datetime.now(timezone.utc)
    slices = split_concurrency(config.concurrency, processes)
    config_data = asdict(config)

    measured_start = time.perf_counter()
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=len(slices), mp_context=context) as pool:
        futures = [
            pool.submit(_run_child, variant, config_data, slice_concurrency)
            for slice_concurrency in slices
        ]
        measurements = [m for future in futures for m in future.result()]
    duration_ms = (time.perf_counter() - measured_start) * 1000.0

    implementation_base = {
        "openai": "asyncio-openai-sdk",
        "deferred": "asyncio-httpx-deferred",
    }[variant]
    result = {
        "language": "python",
        "implementation": f"{implementation_base}-mp{len(slices)}",
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

    destination = output_dir or Path(config.output_dir) / f"python-{variant}-mp"
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


async def async_main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a Python client variant across multiple worker processes."
    )
    parser.add_argument("--variant", choices=VARIANTS, required=True,
                        help="Which single-process client to fan out.")
    parser.add_argument("--processes", type=int, default=DEFAULT_PROCESSES,
                        help="Worker process count (default: %(default)s).")
    parser.add_argument("--config", default="config/workload.smoke.json", help="Path to workload JSON.")
    parser.add_argument("--output-dir", default=None, help="Directory for summary.json.")
    args = parser.parse_args()

    config = WorkloadConfig.from_path(args.config)
    result = await run_benchmark(
        config,
        Path(args.output_dir) if args.output_dir else None,
        variant=args.variant,
        processes=args.processes,
    )
    summary = result["summary"]
    print(
        f"python-{args.variant}-mp "
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
