# LLM Harness Overhead Benchmark

This repo measures how faithfully seven streaming clients — the official OpenAI Python SDK (`python-openai`), a minimal hand-rolled Python/httpx client (`python`), a raw-byte-then-decode Python variant (`python-deferred`), Go/`net-http` (`go`), Rust/reqwest (`rust-reqwest`), Rust/hyper (`rust-hyper`), and a parse-free reference (`drain`) — represent a paced synthetic OpenAI-style SSE server. A Rust Axum server emits deterministic, precisely-timed chat-completion chunks (no real model call), so each client's reported throughput and latency reflect its own request scheduling, SSE parsing, and aggregation overhead rather than model inference. The goal is to find where, across a concurrency × token-rate grid, each client's measurements start to distort what the server actually delivered — the point where the measurement instrument itself becomes the bottleneck. `bench_harness.sweep` walks that grid per client with stop rules, and `bench_harness.sweep_report` renders efficiency-vs-concurrency curves showing each client's knee.

**Operating this benchmark? Start with [docs/HANDOFF.md](docs/HANDOFF.md)** — production context, interpretation rules, and the dedicated-machine runbook.

## Layout

- `server-rust/`: Rust Axum synthetic OpenAI-style streaming server.
- `bench_harness/`: Python config, SSE parser, metrics, sweep runner/report, and the Python client variants — `python` (minimal httpx, inline decode), `python-openai` (official OpenAI SDK, the production-style baseline; pacing fields ride in `extra_body`), `python-deferred` (raw-byte hot path with per-event timestamps, full decode after the stream closes — the measurement-harness strategy), plus `python-openai-mp` / `python-deferred-mp` (`python_mp.py`: the same stacks fanned across 12 worker processes, mirroring production multiprocessing).
- `go-client/`: Go benchmark client using the standard `net/http` stack.
- `rust-client/`: Rust benchmark client using Tokio with selectable `reqwest`, lower-level Hyper, and `drain` (parse-free reference) paths.
- `config/`: shared workload and sweep JSON files.
- `scripts/`: smoke runner, result comparison table, static report generators, and thin back-compat shims for the sweep runner/report (the real implementations live in `bench_harness/`).

## Prerequisites

- Python 3.12+
- `uv` for Python dependency setup, or another way to install `httpx`
- Go 1.22+
- Rust stable with `cargo`

## Setup

One-shot toolchain bootstrap (installs rustup, Go ≥ 1.22, uv, the Python
venv, and taskset on Linux — idempotent, only installs what's missing;
detects macOS vs Linux):

```bash
make setup
```

Or just create the Python venv if the toolchains are already present:

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

The Python tests cover the shared SSE parser, workload/sweep config loaders, and metric aggregation. The Go and Rust tests cover their parser and percentile logic, plus Rust server event generation.

## Concurrency Sweep

This is the main event: a tier × concurrency sweep across all seven clients that finds each client's efficiency knee.

Run the full sweep (builds all binaries, starts the server, runs every client
per cell, records server schedule-slip stats and CPU):

```bash
make sweep CONFIG=config/sweep.experiment.json  # THE canonical comparison (see HANDOFF)
make sweep                                    # config/sweep.default.json
make sweep CONFIG=config/sweep.linux.json     # any sweep profile
make sweep-smoke                              # ~1min end-to-end sanity sweep
make sweep-report                             # report from the newest run
make sweep-report RUNS="results/<a> results/<b>"   # merge several runs
```

The same commands are installed into the venv by `uv sync` as console
scripts, for use without make:

```bash
uv run bench-sweep --config config/sweep.linux.json
uv run bench-sweep-report results/<a> results/<b>
```

`scripts/run_sweep.py` and `scripts/generate_sweep_report.py` still work as
thin compatibility shims around `bench_harness.sweep` and
`bench_harness.sweep_report` — use whichever entry point is convenient.

### Sweep config field reference

Sweep configs are JSON in `config/` (e.g. `config/sweep.default.json`), validated on load by `SweepConfig.from_path` / `SweepConfig.validate`:

| Field | Type | Meaning |
| --- | --- | --- |
| `tiers[].name` | string | Free-form label for a pacing profile; groups cells in the report (never merge runs whose tier names don't mean the same thing). |
| `tiers[].events_per_second` | int ≥ 0 | Server pacing rate for content chunks in this tier; `0` = unpaced/max-speed. |
| `tiers[].ttfc_ms` | int ≥ 0 | Server-side delay before the first SSE event, per tier. |
| `concurrencies` | list of int, ascending, positive | The rungs of the sweep ladder; escalated in order until a client's stop rule trips. |
| `clients` | list of string | Which clients to run this sweep; must be a subset of the known clients (`python`, `python-deferred`, `python-openai`, `go`, `rust-reqwest`, `rust-hyper`, `drain`). |
| `duration_seconds` | float > 0 | Timed measurement window per cell, after warmup. |
| `warmup_seconds` | float ≥ 0 | Discarded ramp-up time before the timed window starts. |
| `repeats` | int ≥ 1 | Repeats per (tier, concurrency, client) cell; aggregated in the report. |
| `cooldown_seconds` | float ≥ 0 | Pause between concurrency rungs. |
| `chunks_per_response` | int > 0 | Content chunks per streamed response. |
| `chunk_bytes` | int > 0 | Bytes per content chunk. |
| `stop_efficiency_below` | float | Escalation stops for a client/tier once mean efficiency drops below this. |
| `stop_ttfc_excess_p95_ms` | float | Escalation stops once mean p95 TTFC exceeds `ttfc_ms` by more than this. |
| `stop_failure_fraction` | float in [0, 1] | Escalation stops once the failed+incomplete fraction exceeds this. |
| `server_worker_threads` | int > 0 or `null` | Caps the server's tokio runtime (`--worker-threads`); `null` = one worker per core. |
| `server_cpus` | string or `null` | `taskset -c` core list for the server process (Linux only). |
| `client_cpus` | string or `null` | `taskset -c` core list for every client process (Linux only); keep disjoint from `server_cpus`. |

### CPU allocation (dedicated-machine runs)

`server_worker_threads` / `server_cpus` / `client_cpus` control CPU placement so the server and the
client under test don't compete for cores:

```json
{
  "server_worker_threads": 8,
  "server_cpus": "0-7",
  "client_cpus": "8-15"
}
```

`server_cpus` / `client_cpus`: Linux only — on macOS (no `taskset`) the sweep
warns once and runs unpinned. Match these to your machine's topology and
keep the two sets disjoint.

`config/sweep.linux.json` is a ready profile for a dedicated Linux box: the
fine-grained ladder (1–1024 with dense rungs from 64 up), 3 repeats, 15s
windows, and an 8-core server / 8-core client split — adjust the core lists
to the actual core count before running.

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

- Closed-loop cells quantize at low event rates: with long streams and short
  windows each worker completes only a few requests, and the tail (waiting
  for the last in-flight request) slightly deflates aggregate events/sec.
  Prefer longer `duration_seconds` for low-rate tiers.

## Reports

Two report generators, both static HTML/SVG with no JS frameworks:

- `bench_harness.sweep_report` (`make sweep-report`) — the efficiency-vs-concurrency sweep report described above; output defaults to `reports/sweep/index.html`.
- `scripts/generate_report.py` — a single-run report comparing whichever clients wrote summaries into one result directory:

```bash
uv run python scripts/generate_report.py
open reports/latest/index.html
```

```bash
uv run python scripts/generate_report.py results/20260629T141233Z --output reports/latest/index.html
open reports/latest/index.html
```

Defaults to the newest run under `results/` if no directory is given. The report includes throughput charts, latency distribution charts, an efficiency/speedup table, and benchmark caveats. Generated reports are ignored by git by default.

## Workloads and Result Shape

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

This normalized shape is the cross-language contract — see `docs/CONTRACTS.md` for the full wire protocol and aggregation rules.

## Development utilities

Ad hoc single-machine runs, useful while developing a client or debugging a specific cell, rather than the full sweep above.

### Smoke run

Run the full local smoke benchmark (use the project venv's Python so the
Python client can import `httpx`):

```bash
uv run python scripts/run_smoke.py --config config/workload.smoke.json
# or: .venv/bin/python scripts/run_smoke.py --config config/workload.smoke.json
```

The smoke runner starts the Rust synthetic server on `127.0.0.1:8080`, runs available clients, writes timestamped summaries under `results/`, asserts every client's `summary.json` shares the identical schema, and prints a comparison table.

### Run clients manually

```bash
python3 -m bench_harness.python_client --config config/workload.smoke.json --output-dir results/manual/python
cd go-client && go run . --config ../config/workload.smoke.json --output-dir ../results/manual/go
cargo run --manifest-path rust-client/Cargo.toml --release -- --config config/workload.smoke.json --output-dir results/manual/rust-reqwest --client reqwest
cargo run --manifest-path rust-client/Cargo.toml --release -- --config config/workload.smoke.json --output-dir results/manual/rust-hyper --client hyper
```

### Compare existing results

```bash
python3 scripts/compare_results.py results
```
