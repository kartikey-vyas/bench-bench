# LLM Harness Overhead Benchmark

This repo is a local benchmark demo for comparing Python, Go, and Rust harness overhead against an OpenAI-style streaming API. It does not call a real model. A synthetic Rust server emits deterministic SSE chat completion chunks so the clients mostly measure request scheduling, SSE parsing, JSON parsing, and aggregation overhead.

## Layout

- `server-rust/`: Rust Axum synthetic OpenAI-style streaming server.
- `bench_harness/`: Python config, SSE parser, metrics, and async `httpx` client.
- `go-client/`: Go benchmark client using the standard `net/http` stack.
- `rust-client/`: Rust benchmark client using Tokio with selectable `reqwest` and lower-level Hyper paths.
- `config/`: shared workload and sweep JSON files.
- `scripts/`: smoke runner, concurrency sweep runner, result comparison table, and report generators.

## Prerequisites

- Python 3.12+
- `uv` for Python dependency setup, or another way to install `httpx`
- Go 1.22+
- Rust stable with `cargo`

## Setup

```bash
uv sync
```

Or install the Python dependency manually:

```bash
python3 -m pip install httpx
```

The Python client depends on `httpx`, which is only installed in the project
virtualenv, not the system `python3`. Always run the smoke and sweep runners
with the project's Python (`uv run python scripts/run_sweep.py ...` or
`.venv/bin/python scripts/run_sweep.py ...`) — running them with a bare
`python3` will fail as soon as the Python client is invoked.

## Tests

```bash
make test-python
make test-go
make test-rust
```

The Python tests cover the shared SSE parser, workload config loader, and metric aggregation. The Go and Rust tests cover their parser and percentile logic, plus Rust server event generation.

## Smoke Run

Run the full local smoke benchmark (use the project venv's Python so the
Python client can import `httpx`):

```bash
uv run python scripts/run_smoke.py --config config/workload.smoke.json
# or: .venv/bin/python scripts/run_smoke.py --config config/workload.smoke.json
```

The smoke runner starts the Rust synthetic server on `127.0.0.1:8080`, runs available clients, writes timestamped summaries under `results/`, and prints a comparison table.

Run clients manually:

```bash
python3 -m bench_harness.python_client --config config/workload.smoke.json --output-dir results/manual/python
cd go-client && go run . --config ../config/workload.smoke.json --output-dir ../results/manual/go
cargo run --manifest-path rust-client/Cargo.toml --release -- --config config/workload.smoke.json --output-dir results/manual/rust-reqwest --client reqwest
cargo run --manifest-path rust-client/Cargo.toml --release -- --config config/workload.smoke.json --output-dir results/manual/rust-hyper --client hyper
```

Compare existing results:

```bash
python3 scripts/compare_results.py results
```

Generate a static HTML report from the newest run:

```bash
uv run python scripts/generate_report.py
open reports/latest/index.html
```

Generate a report from a specific run:

```bash
uv run python scripts/generate_report.py results/20260629T141233Z --output reports/latest/index.html
open reports/latest/index.html
```

The report includes throughput charts, latency distribution charts, an efficiency/speedup table, and benchmark caveats. Generated reports are ignored by git by default. For a concurrency-vs-efficiency view across many cells, see the "Concurrency Sweep" section below.

## Workloads

- `config/workload.smoke.json`: tiny correctness-oriented workload.
- `config/workload.default.json`: higher-throughput local workload for comparison runs.

Workload fields:

```json
{
  "base_url": "http://127.0.0.1:8080",
  "duration_seconds": 20.0,
  "warmup_seconds": 3.0,
  "concurrency": 64,
  "chunks_per_response": 512,
  "chunk_bytes": 8,
  "ttfc_ms": 200,
  "events_per_second": 500,
  "output_dir": "results"
}
```

- `duration_seconds` / `warmup_seconds`: each client runs a fixed-duration closed loop at the given `concurrency`; the first `warmup_seconds` of measurements are discarded before the timed window starts.
- `ttfc_ms`: server-side delay before it emits the first SSE event of a response (simulates model "time to first token").
- `events_per_second`: server pacing rate for chunks after the first one. `0` means the server streams as fast as it can (no pacing) — used by the `max` tier in `config/sweep.default.json`.
- Every response is one SSE stream: a role chunk, then `chunks_per_response` content chunks of `chunk_bytes` bytes each, then a finish chunk, then a `[DONE]` sentinel.

## Result Shape

Each client writes `summary.json`:

```json
{
  "language": "rust",
  "implementation": "reqwest-tokio",
  "started_at": "2026-06-29T00:00:00Z",
  "config": {
    "base_url": "http://127.0.0.1:8080",
    "duration_seconds": 20.0,
    "warmup_seconds": 3.0,
    "concurrency": 64,
    "chunks_per_response": 512,
    "chunk_bytes": 8,
    "ttfc_ms": 200,
    "events_per_second": 500,
    "output_dir": "results"
  },
  "summary": {
    "duration_ms": 20000.4,
    "successful_requests": 5142,
    "incomplete_requests": 0,
    "failed_requests": 0,
    "total_chunks": 2632704,
    "total_bytes": 21061632,
    "requests_per_second": 257.1,
    "chunks_per_second": 25471.0,
    "mean_request_latency_ms": 245.9,
    "p50_request_latency_ms": 244.1,
    "p95_request_latency_ms": 251.8,
    "p99_request_latency_ms": 260.3,
    "mean_time_to_first_chunk_ms": 200.4,
    "p50_time_to_first_chunk_ms": 200.1,
    "p95_time_to_first_chunk_ms": 201.9,
    "p99_time_to_first_chunk_ms": 204.7,
    "p50_max_gap_ms": 2.1,
    "p95_max_gap_ms": 2.6,
    "p99_max_gap_ms": 3.4,
    "max_max_gap_ms": 4.0,
    "p50_stream_stretch": 0.97,
    "p95_stream_stretch": 1.01,
    "p99_stream_stretch": 1.05,
    "ideal_events_per_second": 26811.6,
    "efficiency": 0.95
  }
}
```

Notes on the less obvious `summary` fields:

- `incomplete_requests`: requests that finished (no transport error) but delivered fewer than `chunks_per_response` content chunks — excluded from the latency/gap/stretch stats, and always distinct from `failed_requests` (transport/HTTP errors).
- `p50/p95/p99/max_max_gap_ms`: percentiles of each request's largest inter-event gap, i.e. how "bursty" event delivery was within a stream.
- `p50/p95/p99_stream_stretch`: each request's wall-clock stream duration divided by the ideal duration implied by `events_per_second`; `1.0` means the stream was paced exactly on schedule, `>1.0` means it ran slower than scheduled.
- `ideal_events_per_second` / `efficiency`: the achievable closed-loop ideal throughput at this `concurrency` (accounting for `ttfc_ms` dead time before pacing starts) and the fraction of it the client actually achieved. `efficiency` near `1.0` means the client is keeping up with the server's schedule; a low value points to client-side overhead rather than the server.

This normalized shape is intended to support a simple visualization layer later without changing benchmark clients.

## Concurrency Sweep

Run the full tier × concurrency sweep (builds all binaries, starts the server,
runs every client per cell, records server schedule-slip stats and CPU):

```bash
make sweep          # full sweep, hours — tune config/sweep.default.json
make sweep-smoke    # 2-minute end-to-end sanity sweep
make sweep-report   # writes reports/sweep/index.html from the newest run
```

The Makefile auto-selects `.venv/bin/python` when present, falling back to
`python3` otherwise; if you invoke `scripts/run_sweep.py` directly (bypassing
`make`), use the project venv's Python as noted in Setup, or the sweep will
fail fast with a preflight error when the `python` client is configured.

Per (tier, client), concurrency escalation stops when failures exceed
`stop_failure_fraction`, mean efficiency drops below `stop_efficiency_below`,
or mean p95 TTFC excess exceeds `stop_ttfc_excess_p95_ms` — the stopping
concurrency is that client's knee for the tier, listed in the report and in
`results/<run>/sweep.json`.

The `drain` client reads raw bytes without SSE/JSON parsing: it calibrates the
ceiling. If drain holds efficiency ≈ 1.0 at a concurrency where a real client
does not, the gap is client overhead, not the server. Cross-check
`server_stats.json` (schedule slip) and `cpu.json` per cell before attributing
a knee to the client — on one machine, a saturated client can starve the server.
