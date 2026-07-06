# LLM Harness Overhead Benchmark

This repo measures client-side overhead in streaming LLM benchmarks. It does not call a real model: a Rust synthetic server emits deterministic OpenAI-style SSE chat-completion chunks on a controlled schedule, and each client reports what it observed.

The question is: **at what concurrency does the measurement client stop faithfully representing what the server delivered?** If a real client falls behind while the parse-free reference keeps up, the measurement instrument has become the bottleneck.

## Method

- **Server:** Rust/Axum endpoint compatible with `POST /v1/chat/completions`, with benchmark fields for content chunk count, chunk size, TTFC, and per-stream event rate.
- **Pacing:** events are scheduled against absolute deadlines. A slow client may receive coalesced bursts, but the server does not intentionally stretch the stream.
- **Clients:** closed-loop workers run for a fixed measurement window. Each worker starts a new stream after its previous stream completes.
- **Sweep:** `bench_harness.sweep` walks tier x concurrency x repeat x client cells, captures server slip and CPU stats, and stops escalation once a client clearly falls behind.
- **Report:** `bench_harness.sweep_report` renders static HTML/SVG efficiency-vs-concurrency curves.

Efficiency is normalized against the achievable closed-loop ideal for the workload:

```text
efficiency = observed chunks/sec / achievable closed-loop chunks/sec
```

Near `1.0` means the client kept up with the server schedule. A low value means the client observed fewer chunks than an ideal closed-loop client could have observed for the same concurrency, TTFC, and pacing rate.

## Clients

Known clients are defined in `bench_harness/clients.py`.

| Name | Purpose |
| --- | --- |
| `drain` | Rust/hyper raw-byte drain; parse-free transport reference. |
| `python-openai` | Official OpenAI Python SDK, single process. |
| `python-openai-mp` | Official OpenAI Python SDK across 12 worker processes. |
| `python` | Minimal Python/httpx async SSE parser with inline decode. |
| `python-deferred` | Python/httpx raw-byte hot path, decode after stream close. |
| `python-deferred-mp` | Deferred Python client across 12 worker processes. |
| `go` | Go `net/http` client. |
| `rust-reqwest` | Rust/Tokio client using reqwest. |
| `rust-hyper` | Rust/Tokio client using lower-level Hyper. |

The canonical experiment uses a focused subset: `drain`, `python-openai`, `python-openai-mp`, `python-deferred`, `python-deferred-mp`, `go`, and `rust-hyper`.

## Layout

| Path | Contents |
| --- | --- |
| `server-rust/` | Synthetic OpenAI-style streaming server. |
| `bench_harness/` | Python config, metrics, parser, sweep runner/report, and Python clients. |
| `go-client/` | Go client. |
| `rust-client/` | Rust `reqwest`, `hyper`, and `drain` clients. |
| `config/` | Workload and sweep JSON profiles. |
| `scripts/` | Setup, smoke, comparison, report, and compatibility scripts. |
| `docs/HANDOFF.md` | Operational handoff and dedicated-machine runbook context. |
| `docs/CONTRACTS.md` | Wire protocol and summary-schema source of truth. |

## Setup

Prerequisites: Python 3.12+, `uv`, Go 1.22+, Rust stable, and `taskset` on Linux if using CPU pinning.

```bash
make setup   # installs missing toolchains and syncs the Python venv
uv sync      # Python deps only, if toolchains already exist
```

Run Python entry points through `uv run ...` or `.venv/bin/python ...`; a bare `python3` may not have `httpx`, `openai`, or `rich` installed.

## Quick Start

```bash
make test
make sweep-smoke
make sweep CONFIG=config/sweep.experiment.json
make sweep-report
```

`make sweep-report` reads the newest run by default and writes `reports/sweep/index.html`. Merge explicit run directories with:

```bash
make sweep-report RUNS="results/<run-a> results/<run-b>"
```

## Canonical Experiment

`config/sweep.experiment.json` is the primary comparison profile:

| Setting | Value |
| --- | --- |
| Tier | `eps250`: 250 content events/sec per stream, 200ms server TTFC |
| Concurrency | `64, 128, 256, 384, 512, 768, 1024` |
| Window | 60s measured, 5s warmup |
| Repeats | 2 |
| Clients | `drain`, OpenAI SDK single/mp, deferred Python single/mp, Go, Rust Hyper |

Run it with:

```bash
make sweep CONFIG=config/sweep.experiment.json
```

On a dedicated Linux box, set disjoint server/client core lists before running:

```json
{
  "server_worker_threads": 8,
  "server_cpus": "0-7",
  "client_cpus": "8-15"
}
```

CPU pinning uses `taskset`; on macOS the sweep warns once and runs unpinned.

## Sweep Commands

```bash
make sweep CONFIG=config/sweep.default.json
make sweep CONFIG=config/sweep.linux.json
make sweep-smoke
make sweep-report
uv run bench-sweep --config config/sweep.default.json
uv run bench-sweep-report results/<run-a> results/<run-b>
```

`scripts/run_sweep.py` and `scripts/generate_sweep_report.py` remain compatibility shims. Interactive sweeps show rich progress; pipes/CI use plain logs. Override with `--display rich|plain|auto`. Each cell captures client output in `<cell>/client.log`.

## Interpreting Results

Use `drain` as the transport reference.

- Client efficiency drops while `drain` stays near `1.0`: likely client overhead.
- `drain` drops too: server, OS, or machine saturation; do not attribute that point to a client.
- p95 TTFC excess indicates admission delay or event-loop scheduling delay before the first parsed event.
- stream stretch greater than `1.0` means streams arrived slower than the configured pacing schedule.
- failures or incomplete requests at high concurrency may be environmental; inspect cell logs.

Always cross-check `server_stats.json` and `cpu.json` before drawing conclusions. Generated findings should live in HTML reports, not this README.

## Sweep Config Reference

Sweep configs are JSON files under `config/`.

| Field | Meaning |
| --- | --- |
| `tiers[].name` | Report grouping label; do not merge runs whose tier names mean different things. |
| `tiers[].events_per_second` | Content pacing rate per stream; `0` means unpaced/max-speed. |
| `tiers[].ttfc_ms` | Server delay before the first SSE event. |
| `concurrencies` | Ascending concurrency ladder. |
| `clients` | Client names from `bench_harness/clients.py`. |
| `duration_seconds` / `warmup_seconds` | Measured window and discarded ramp-up time. |
| `repeats` / `cooldown_seconds` | Repeats per cell and pause between rungs. |
| `chunks_per_response` / `chunk_bytes` | Stream size. |
| `stop_efficiency_below` | Stop escalating when mean efficiency falls below this value. |
| `stop_ttfc_excess_p95_ms` | Stop escalating when p95 TTFC exceeds configured TTFC by more than this. |
| `stop_failure_fraction` | Stop escalating when failed plus incomplete requests exceed this fraction. |
| `server_worker_threads` | Optional Tokio worker-thread cap for the server. |
| `server_cpus` / `client_cpus` | Optional Linux `taskset -c` core lists. |

## Workload And Result Contract

Each stream is requested with OpenAI-style chat-completion JSON plus pacing fields:

```json
{
  "model": "synthetic",
  "messages": [{"role": "user", "content": "benchmark"}],
  "stream": true,
  "chunks": 512,
  "chunk_bytes": 8,
  "ttfc_ms": 200,
  "events_per_second": 500,
  "request_id": "python-3-17"
}
```

The server emits one role event, `chunks` content events, one finish event, then `data: [DONE]`. With `events_per_second: 0`, all events are due immediately.

Single-cell workload configs use:

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

Each client writes `summary.json` with the same envelope: `language`, `implementation`, `started_at`, `config`, and `summary`. The summary contains request counts, chunk/byte totals, request latency percentiles, TTFC percentiles, max inter-event gap percentiles, stream-stretch percentiles, closed-loop ideal throughput, and efficiency.

Important aggregation rules:

- `successful_requests` are complete streams; `incomplete_requests` finished but delivered fewer chunks than expected; `failed_requests` are transport or HTTP failures.
- latency, TTFC, gap, and stretch percentiles are computed over successful complete requests.
- `chunks_per_second` is clipped to the measured window so late straggler completion cannot dilute the aggregate.
- `efficiency` is defined for paced tiers; unpaced tiers report `0.0` for ideal throughput and efficiency.

`docs/CONTRACTS.md` is the source of truth when the wire protocol or summary schema changes.

## Reports

```bash
make sweep-report
uv run bench-sweep-report results/<run-a> results/<run-b>
uv run python scripts/generate_report.py results/<run> --output reports/latest/index.html
```

The sweep report renders efficiency-vs-concurrency charts grouped by tier. `scripts/generate_report.py` is for ad hoc single-run comparisons. Generated `results/` and `reports/` output is ignored by git.

## Development Utilities

```bash
uv run python scripts/run_smoke.py --config config/workload.smoke.json
uv run python -m bench_harness.python_client --config config/workload.smoke.json --output-dir results/manual/python
cd go-client && go run . --config ../config/workload.smoke.json --output-dir ../results/manual/go
cargo run --manifest-path rust-client/Cargo.toml --release -- --config config/workload.smoke.json --output-dir results/manual/rust-reqwest --client reqwest
cargo run --manifest-path rust-client/Cargo.toml --release -- --config config/workload.smoke.json --output-dir results/manual/rust-hyper --client hyper
uv run python scripts/compare_results.py results
```
