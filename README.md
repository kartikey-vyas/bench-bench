# LLM Harness Overhead Benchmark

This repo is a local benchmark demo for comparing Python, Go, and Rust harness overhead against an OpenAI-style streaming API. It does not call a real model. A synthetic Rust server emits deterministic SSE chat completion chunks so the clients mostly measure request scheduling, SSE parsing, JSON parsing, and aggregation overhead.

## Layout

- `server-rust/`: Rust Axum synthetic OpenAI-style streaming server.
- `bench_harness/`: Python config, SSE parser, metrics, and async `httpx` client.
- `go-client/`: Go benchmark client using the standard `net/http` stack.
- `rust-client/`: Rust benchmark client using Tokio and `reqwest`.
- `config/`: shared workload JSON files.
- `scripts/`: smoke runner and result comparison table.

## Prerequisites

- Python 3.12+
- `uv` for Python dependency setup, or another way to install `httpx`
- Go 1.22+
- Rust stable with `cargo`

At implementation time in this environment, `cargo`, `rustc`, and `go` were not available on `PATH`, so Python unit tests were run locally and Go/Rust verification commands were recorded as toolchain-blocked.

## Setup

```bash
uv sync
```

Or install the Python dependency manually:

```bash
python3 -m pip install httpx
```

## Tests

```bash
make test-python
make test-go
make test-rust
```

The Python tests cover the shared SSE parser, workload config loader, and metric aggregation. The Go and Rust tests cover their parser and percentile logic, plus Rust server event generation.

## Smoke Run

Run the full local smoke benchmark:

```bash
python3 scripts/run_smoke.py --config config/workload.smoke.json
```

The smoke runner starts the Rust synthetic server on `127.0.0.1:8080`, runs available clients, writes timestamped summaries under `results/`, and prints a comparison table.

Run clients manually:

```bash
python3 -m bench_harness.python_client --config config/workload.smoke.json --output-dir results/manual/python
cd go-client && go run . --config ../config/workload.smoke.json --output-dir ../results/manual/go
cargo run --manifest-path rust-client/Cargo.toml --release -- --config config/workload.smoke.json --output-dir results/manual/rust
```

Compare existing results:

```bash
python3 scripts/compare_results.py results
```

## Workloads

- `config/workload.smoke.json`: tiny correctness-oriented workload.
- `config/workload.default.json`: higher-throughput local workload for comparison runs.

Workload fields:

```json
{
  "base_url": "http://127.0.0.1:8080",
  "total_requests": 10000,
  "concurrency": 256,
  "chunks_per_response": 64,
  "chunk_bytes": 32,
  "delay_us": 0,
  "warmup_requests": 500,
  "output_dir": "results"
}
```

## Result Shape

Each client writes `summary.json`:

```json
{
  "language": "rust",
  "implementation": "reqwest-tokio",
  "started_at": "2026-06-29T00:00:00Z",
  "config": {
    "total_requests": 10000,
    "concurrency": 256,
    "chunks_per_response": 64,
    "chunk_bytes": 32,
    "delay_us": 0
  },
  "summary": {
    "duration_ms": 1234.5,
    "successful_requests": 10000,
    "failed_requests": 0,
    "total_chunks": 640000,
    "total_bytes": 20480000,
    "requests_per_second": 8100.4,
    "chunks_per_second": 518425.6,
    "p95_request_latency_ms": 41.2
  }
}
```

This normalized shape is intended to support a simple visualization layer later without changing benchmark clients.
