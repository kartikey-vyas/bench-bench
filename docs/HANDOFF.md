# Handoff: paced streaming benchmark — dedicated-Linux run

Last updated: 2026-07-06. State: all suites green, 9-client smoke sweep green end-to-end on macOS.

## THE canonical experiment (run this first)

`config/sweep.experiment.json` — the agreed primary comparison:

- Tier: **250 events/s per stream, 200ms TTFC** (eps250)
- Concurrency ladder: **64, 128, 256, 384, 512, 768, 1024**
- **60s measured windows**, 5s warmup, 2 repeats
- Candidates: `python-openai` (production baseline, single process),
  `python-openai-mp` (same, 12 worker processes — production deployment
  shape), `python-deferred` (the fix hypothesis, single process),
  `python-deferred-mp` (fix + 12 processes), `go`, `rust-hyper`, `drain`
  (theoretical max)

```bash
make sweep CONFIG=config/sweep.experiment.json   # ≈ 2h ceiling, less with stops
```

The four python variants form a 2×2 (SDK vs deferred × single vs
multiprocess): it separates "how much does deferral buy within one
process" from "how much does process fan-out buy each stack". On Linux,
add core pinning fields per the CPU-allocation section before running.

## Why this exists (production context)

Artificial Analysis runs an LLM-provider benchmarking client in production:
Python + the official OpenAI SDK, async event loops, multiprocessing. At high
concurrency it bottlenecks — measured throughput lower and TTFT higher than
the providers actually deliver — i.e. the measurement instrument distorts the
measurement. Hypothesis under test: move all decode work off the hot path
(timestamp raw events on arrival, decode after stream close) so streams are
timed with minimal overhead.

Goals:
1. Repeatable benchmark that detects client-side bottlenecking.
2. Compare the production setup against alternatives — see the client ladder.
3. Readable report showing where each setup bottlenecks per (concurrency ×
   tokens/sec) tier.

The client ladder is a causal chain (each gap isolates one variable):
`python-openai` (official SDK, inline pydantic decode — the production
baseline) → `python` (minimal hand-rolled inline decode — SDK overhead) →
`python-deferred` (raw-byte hot path, decode after close — the proposed fix)
→ `go` / `rust-*` (compiled inline — runtime ceiling) → `drain` (parse-free —
transport ceiling / theoretical max). The `-mp` variants (`python-openai-mp`,
`python-deferred-mp`, via `bench_harness/python_mp.py`) fan the same stacks
across 12 worker processes, each with its own event loop over a slice of the
total concurrency — the production deployment shape; window-clipped counting
makes the cross-process aggregation exact. NOTE: for this use case the client
is a measurement instrument, not an application simulator — deferring decode
is legitimate; the benchmark quantifies what each setup's timing distortion
is. The key results to extract: does python-deferred track drain where
python-openai collapses, and how far does 12-way multiprocessing move each
python knee?

## What this repo does

A Rust Axum server streams synthetic OpenAI-style SSE chat completions with exactly controlled timing (`ttfc_ms` delay before the first event, `events_per_second` per-request rate, deadline-based schedule with catch-up bursts). Seven clients (python-openai/official SDK, python/httpx, python-deferred/raw-bytes-then-decode, go/net-http, rust-reqwest, rust-hyper, and `drain` — a parse-free reference that only counts bytes) run closed-loop workers for fixed wall-clock windows. The OpenAI SDK client passes the server's pacing fields via `extra_body`, so the SDK speaks to the synthetic server unmodified. `scripts/run_sweep.py` walks tier × concurrency × repeat × client cells with stop rules; `scripts/generate_sweep_report.py` renders efficiency-vs-concurrency charts. Goal: find the concurrency at which each client stops faithfully representing the server ("knee").

Contracts (wire protocol, 25-key summary schema, aggregation rules, amended efficiency ideal): `docs/CONTRACTS.md` is the source of truth. Full design history and per-task rationale: `docs/superpowers/plans/2026-07-03-paced-streaming-sweep.md`. Efficiency = observed events/s ÷ achievable closed-loop ideal = `concurrency × chunks / (ttfc + (chunks−1)/rate)`.

## Interpretation rules (memorize these before reading results)

1. Client line sags while drain ≈ 1.0 → client overhead (real knee).
2. Drain sags too → server/OS ceiling, not a client result.
3. Low efficiency BUT p95 stream stretch ≈ 1.0 AND small TTFC excess AND zero failures → previously the **window-dilution artifact** (one straggling worker stretches the closed-loop measurement window while the rest idle); this is now fixed at the root by window-clipped counting (see known-issue history below), so it should essentially never fire on fresh runs. The report's dilution flag ("window dilution") is retained as a tripwire — if it still fires on a new run, treat that as a bug report, not an expected artifact. Flagged cells in OLD result trees (pre-fix runs, e.g. `results/20260703*`) should still be discounted per the original reasoning; they were not re-run.
4. Cross-check `server_stats.json` (schedule slip = server's own lateness vs its timetable, measured against the oldest event in each batch) and `cpu.json` per cell.

## Findings so far (M5 MacBook, shared server+client)

- All clients ≈ 0.999 up to c=16 at every tested rate.
- python: TTFC-tail knee at c=64 (p95 excess 240–290ms, both tiers — asyncio admission queueing); single-core parse collapse at eps500/c=256 (efficiency 0.22, ~24k events/s ceiling, CPU pinned at 100% of one core).
- go and both rust clients: statistically indistinguishable below box saturation. Do NOT conclude "Go > Rust" from existing data — every sub-0.99 rust cell below c=1024 is a flagged dilution artifact, and rust-hyper's eps100/c1024 rung is missing due to a false stop.
- Box ceiling (shared MacBook): ~330k events/s aggregate at eps500/c1024 — drain collapses, server CPU 480–820%, slip clean (generator on time; delivery path saturated).
- Result trees: `results/20260703T065449Z` (eps500 ladder), `results/20260703T071614Z` (python+drain at c256), `results/20260703T072701Z` (eps100 ladder). Merged report: `reports/sweep/index.html`.

## Known open issues (priority order)

1. **FIXED: window-dilution artifact.** Root fix landed: clients now count only chunks received inside the measured window (`window_chunks` per request) and divide by the configured `duration_seconds` itself rather than the stretched actual duration, so one straggling worker can no longer dilute the aggregate. Applied across all six real clients + drain + aggregation + tests; frame-granular clients (drain, python-deferred) approximate the clipped count to within one event. Stop rules, which previously acted on diluted numbers mid-sweep, benefit automatically since they consume the same summary. The report's dilution flag is retained as a tripwire (should essentially never fire now — see interpretation rule 3).
2. `CpuSampler` uses `ps -o %cpu` (decaying average on macOS; on Linux it's total-lifetime average) — treat CPU numbers as indicative. A proper interval sampler (delta of utime/stime from /proc) would be better on Linux.
3. Workers are phase-locked (all start at t=0 with identical cycle lengths), so connect bursts recur in lockstep — python's TTFC knee partially reflects synchronized arrivals. Optional: stagger worker start by i×(cycle/N).
4. Minor deferred review findings: upper-bound validation tests are missing (e.g. no test asserts a rejection at the server's documented `chunks`/`chunk_bytes`/`ttfc_ms`/`events_per_second` maxima); negative-path config tests are sparse outside the sweep config validator. The report's series-label layout has a latent clamp edge once a merged report carries 20+ series (labels may overlap) — not yet hit in practice, not yet tested.

## Runbook: dedicated Linux machine

Prereqs: Rust stable, Go 1.22+, Python 3.12+, `uv`, `taskset` (util-linux — present on virtually every distro). `make setup` bootstraps all of them (idempotent, macOS/Linux aware; distro Go older than 1.22 aborts with tarball instructions).

```bash
make setup                                 # or: uv sync, if toolchains exist
# EDIT config/sweep.linux.json first:
#   server_cpus / client_cpus: disjoint core lists matching the machine
#     (server 8 cores is plenty; give clients the rest; avoid SMT siblings
#      crossing the two sets if possible)
#   server_worker_threads: = number of server cores
make sweep CONFIG=config/sweep.linux.json      # or: uv run bench-sweep --config …
make sweep-report                              # or: uv run bench-sweep-report <dirs…>
# (sweep code lives in bench_harness/sweep.py + sweep_report.py;
#  scripts/run_sweep.py and scripts/generate_sweep_report.py are compat shims)
```

Notes:
- The runner builds all binaries, starts the server itself (port 8080), raises RLIMIT_NOFILE, writes `results/<UTC>/…` incrementally (`sweep.json` survives crashes/ctrl-C), and prints `[N/total]` progress with ETA.
- The python client MUST run under the venv interpreter (the runner fails fast with exit 2 if httpx is missing — that is the designed behavior, not a bug).
- Profile ceiling: 12 rungs × 5 tiers × 3 repeats × 7 clients ≈ 1260 runs, ~5.5h+ ceiling for `sweep.linux.json`; stop rules prune. `make sweep-smoke` (~30s) is the sanity gate after any change.
- Cooldowns are 5s; at high rungs watch for TIME_WAIT/ephemeral-port pressure if failures appear at c≥768 (failures at high rungs only = environment — cross-check per interpretation rules).
- Interesting numbers to extract: drain's curve on isolated server cores = the Rust server's true delivery ceiling; whether go vs rust separate at 384–1024 once dilution and contention are gone.

## Per-core capacity + core-scaling series (optional second experiment)

`config/sweep.percore.json` pins every client to ONE core (`client_cpus: "8"`),
turning the knee into a clean per-core capacity number: `knee_concurrency ×
rate ≈ faithful events/s per core`. Drain on the same single core is the
parse-free ceiling, so drain-minus-client = the cost of SSE+JSON parsing on
that runtime. On Linux, taskset affinity propagates correctly into GOMAXPROCS
and tokio's worker count (both use sched_getaffinity), so no per-client flags
are needed.

To measure core scaling, clone the config per allocation and encode the core
count in the tier NAMES (tier names are free labels; the report groups by
them, so a merged report shows e.g. `eps500-1core` and `eps500-4core` as
separate sections instead of averaging them together — never merge runs whose
tier names don't encode the difference):

- 1 core: `client_cpus: "8"`, tiers `*-1core`
- 2 cores: `client_cpus: "8-9"`, tiers `*-2core`
- 4 cores: `client_cpus: "8-11"`, tiers `*-4core`
- 8 cores: `client_cpus: "8-15"`, tiers `*-8core`

Then: `.venv/bin/python scripts/generate_sweep_report.py results/<run1core> results/<run4core> …`
Expected shape: Go/Rust knees scale ~linearly with cores; Python stays flat
(GIL) — that flatness is itself the finding, not an error. Keep the ladder
denser and lower than the full profile (1-core knees land well under c=512
at eps500; raise the ladder for the paced 100 eps tier or multi-core runs).

## Conventions

- Commits go straight to `main`, imperative subject ("Add …", "Fix …").
- Tests: `make test` (python via unittest, go test, two cargo suites). Keep the summary schema byte-identical across all clients — it is the cross-language contract.
- Sweep/workload configs are JSON in `config/`; never hardcode workload parameters in clients.
