# LLM Harness Overhead Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable local benchmark demo comparing Python, Go, and Rust client overhead against a synthetic OpenAI-style streaming API.

**Architecture:** A Rust Axum server emits deterministic OpenAI-style SSE chat completion chunks. Python, Go, and Rust clients load the same JSON workload, issue concurrent streaming requests, parse SSE and JSON chunks, aggregate latency/throughput metrics, and write normalized `summary.json` files. Python orchestration scripts run smoke workloads and compare generated summaries.

**Tech Stack:** Python 3.12 with `httpx` for the Python client, Go standard library for the Go client, Rust with Tokio/Axum for the server and Tokio/reqwest for the Rust client, JSON config and result files.

---

## File Structure

- Create `.gitignore`: ignore local envs, build products, and benchmark results.
- Modify `README.md`: describe prerequisites, architecture, and run commands.
- Modify `pyproject.toml`: add Python dependencies and package metadata.
- Modify `main.py`: provide a small pointer CLI for the repo.
- Create `config/workload.smoke.json`: tiny workload for correctness smoke runs.
- Create `config/workload.default.json`: higher-throughput local workload.
- Create `bench_harness/config.py`: shared Python config loader.
- Create `bench_harness/sse.py`: incremental SSE parser used by Python client and tests.
- Create `bench_harness/metrics.py`: percentile and summary aggregation helpers.
- Create `bench_harness/python_client.py`: Python async streaming benchmark harness.
- Create `bench_harness/__init__.py`: package marker.
- Create `tests/test_sse.py`: Python SSE parser tests.
- Create `tests/test_metrics.py`: Python metric aggregation tests.
- Create `server-rust/Cargo.toml`: Rust synthetic server crate.
- Create `server-rust/src/lib.rs`: server request validation and SSE event generation.
- Create `server-rust/src/main.rs`: Axum HTTP entrypoint.
- Create `server-rust/tests/streaming.rs`: server unit/integration-style tests.
- Create `go-client/go.mod`: Go benchmark client module.
- Create `go-client/main.go`: Go benchmark harness.
- Create `go-client/main_test.go`: Go parser and metric tests.
- Create `rust-client/Cargo.toml`: Rust benchmark client crate.
- Create `rust-client/src/lib.rs`: Rust config, SSE parsing, metric aggregation, client runner.
- Create `rust-client/src/main.rs`: Rust CLI entrypoint.
- Create `rust-client/tests/parser.rs`: Rust parser tests.
- Create `scripts/run_smoke.py`: builds/runs server and each client when toolchains are available.
- Create `scripts/compare_results.py`: prints a compact comparison table from summaries.
- Create `Makefile`: common test, build, smoke, and compare commands.

## Task 1: Python Parser And Metrics Tests

**Files:**
- Create: `tests/test_sse.py`
- Create: `tests/test_metrics.py`
- Create: `bench_harness/__init__.py`
- Create: `bench_harness/sse.py`
- Create: `bench_harness/metrics.py`

- [ ] **Step 1: Write failing Python SSE tests**

```python
import unittest

from bench_harness.sse import SseDecoder


class SseDecoderTests(unittest.TestCase):
    def test_decoder_returns_complete_data_events_and_buffers_partial_lines(self):
        decoder = SseDecoder()

        self.assertEqual(decoder.feed("data: {\"a\":"), [])
        self.assertEqual(decoder.feed("1}\n\ndata: [DONE]\n\n"), ['{"a":1}', "[DONE]"])

    def test_decoder_ignores_comments_and_blank_events(self):
        decoder = SseDecoder()

        self.assertEqual(decoder.feed(": keepalive\n\ndata: hello\n\n\n"), ["hello"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Write failing Python metric tests**

```python
import unittest

from bench_harness.metrics import RequestMeasurement, aggregate_summary, percentile


class MetricsTests(unittest.TestCase):
    def test_percentile_uses_nearest_rank(self):
        self.assertEqual(percentile([10.0, 20.0, 30.0, 40.0], 0.50), 20.0)
        self.assertEqual(percentile([10.0, 20.0, 30.0, 40.0], 0.95), 40.0)

    def test_aggregate_summary_computes_rates_and_latency_percentiles(self):
        measurements = [
            RequestMeasurement(ok=True, latency_ms=10.0, first_chunk_ms=2.0, chunks=4, bytes=16),
            RequestMeasurement(ok=True, latency_ms=30.0, first_chunk_ms=4.0, chunks=4, bytes=16),
            RequestMeasurement(ok=False, latency_ms=50.0, first_chunk_ms=0.0, chunks=0, bytes=0),
        ]

        summary = aggregate_summary(measurements, duration_ms=20.0)

        self.assertEqual(summary["successful_requests"], 2)
        self.assertEqual(summary["failed_requests"], 1)
        self.assertEqual(summary["total_chunks"], 8)
        self.assertEqual(summary["total_bytes"], 32)
        self.assertEqual(summary["requests_per_second"], 100.0)
        self.assertEqual(summary["chunks_per_second"], 400.0)
        self.assertEqual(summary["mean_request_latency_ms"], 20.0)
        self.assertEqual(summary["p50_request_latency_ms"], 10.0)
        self.assertEqual(summary["p95_request_latency_ms"], 30.0)
        self.assertEqual(summary["mean_time_to_first_chunk_ms"], 3.0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Verify Python tests fail before implementation**

Run: `python3 -m unittest discover -s tests -v`

Expected: FAIL because `bench_harness.sse` and `bench_harness.metrics` do not exist yet.

- [ ] **Step 4: Implement parser and metric helpers**

Implement `SseDecoder.feed()` to buffer partial text, split complete SSE events on blank lines, ignore comments, and return `data:` payloads. Implement `RequestMeasurement`, nearest-rank `percentile`, and `aggregate_summary`.

- [ ] **Step 5: Verify Python tests pass**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS for parser and metrics tests.

## Task 2: Rust Synthetic Server

**Files:**
- Create: `server-rust/Cargo.toml`
- Create: `server-rust/src/lib.rs`
- Create: `server-rust/src/main.rs`
- Create: `server-rust/tests/streaming.rs`

- [ ] **Step 1: Write Rust server tests first**

Tests cover:

```rust
use server_rust::{build_sse_events, ChatRequest};

#[test]
fn builds_configured_number_of_openai_style_events() {
    let request = ChatRequest {
        model: Some("synthetic".to_string()),
        messages: vec![],
        stream: true,
        chunks: Some(2),
        chunk_bytes: Some(4),
        delay_us: Some(0),
        request_id: Some("req-1".to_string()),
    };

    let events = build_sse_events(&request).unwrap();

    assert_eq!(events.len(), 3);
    assert!(events[0].starts_with("data: {"));
    assert!(events[0].contains("\"object\":\"chat.completion.chunk\""));
    assert!(events[0].contains("\"content\":\"xxxx\""));
    assert_eq!(events[2], "data: [DONE]\n\n");
}
```

- [ ] **Step 2: Verify Rust server tests fail before implementation**

Run: `cargo test --manifest-path server-rust/Cargo.toml`

Expected: FAIL because server crate code does not exist yet. If `cargo` is unavailable, record that verification is blocked by missing Rust toolchain.

- [ ] **Step 3: Implement server crate**

Implement `ChatRequest`, request validation, deterministic chunk content generation, `build_sse_events()`, `GET /health`, and `POST /v1/chat/completions`.

- [ ] **Step 4: Verify Rust server tests pass**

Run: `cargo test --manifest-path server-rust/Cargo.toml`

Expected: PASS when Rust toolchain is installed.

## Task 3: Python Benchmark Client

**Files:**
- Create: `config/workload.smoke.json`
- Create: `config/workload.default.json`
- Create: `bench_harness/config.py`
- Create: `bench_harness/python_client.py`
- Modify: `pyproject.toml`
- Modify: `main.py`

- [ ] **Step 1: Write config loader test first**

```python
import unittest
from pathlib import Path

from bench_harness.config import WorkloadConfig


class WorkloadConfigTests(unittest.TestCase):
    def test_workload_config_loads_json(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "workload.json"
            config_path.write_text(
                '{"base_url":"http://127.0.0.1:8080","total_requests":3,'
                '"concurrency":2,"chunks_per_response":4,"chunk_bytes":8,'
                '"delay_us":0,"warmup_requests":1,"output_dir":"results"}'
            )

            config = WorkloadConfig.from_path(config_path)

        self.assertEqual(config.total_requests, 3)
        self.assertEqual(config.request_payload(7)["chunks"], 4)
        self.assertEqual(config.request_payload(7)["request_id"], "python-7")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Verify config test fails before implementation**

Run: `python3 -m unittest discover -s tests -v`

Expected: FAIL because `bench_harness.config` does not exist yet.

- [ ] **Step 3: Implement config loader and Python client**

Implement `WorkloadConfig`, lazy `httpx` import inside the async runner, bounded concurrency with `asyncio.Semaphore`, SSE parsing via `SseDecoder`, JSON parsing for each data event, time-to-first-chunk measurement, result summary writing, and CLI args `--config` and `--output-dir`.

- [ ] **Step 4: Verify Python unit tests pass**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS.

## Task 4: Go Benchmark Client

**Files:**
- Create: `go-client/go.mod`
- Create: `go-client/main.go`
- Create: `go-client/main_test.go`

- [ ] **Step 1: Write Go tests first**

Tests cover parsing `data:` events across blank-line-delimited SSE payloads and nearest-rank percentile behavior.

- [ ] **Step 2: Verify Go tests fail before implementation**

Run: `cd go-client && go test ./...`

Expected: FAIL before `main.go` exists. If `go` is unavailable, record that verification is blocked by missing Go toolchain.

- [ ] **Step 3: Implement Go client**

Implement config loading, request payload construction, bounded worker concurrency, SSE line parsing with `bufio.Scanner`, JSON chunk parsing, per-request measurements, aggregate summary, `summary.json` output, and CLI flags `--config` and `--output-dir`.

- [ ] **Step 4: Verify Go tests pass**

Run: `cd go-client && go test ./...`

Expected: PASS when Go toolchain is installed.

## Task 5: Rust Benchmark Client

**Files:**
- Create: `rust-client/Cargo.toml`
- Create: `rust-client/src/lib.rs`
- Create: `rust-client/src/main.rs`
- Create: `rust-client/tests/parser.rs`

- [ ] **Step 1: Write Rust client parser and metric tests first**

Tests cover SSE decoding with partial chunks and nearest-rank percentile behavior.

- [ ] **Step 2: Verify Rust client tests fail before implementation**

Run: `cargo test --manifest-path rust-client/Cargo.toml`

Expected: FAIL before client code exists. If `cargo` is unavailable, record that verification is blocked by missing Rust toolchain.

- [ ] **Step 3: Implement Rust client**

Implement config loading, reqwest streaming requests, bounded concurrency with Tokio semaphore, SSE parser, JSON chunk parsing, measurements, aggregate summary, `summary.json` output, and CLI args `--config` and `--output-dir`.

- [ ] **Step 4: Verify Rust client tests pass**

Run: `cargo test --manifest-path rust-client/Cargo.toml`

Expected: PASS when Rust toolchain is installed.

## Task 6: Orchestration, Documentation, And Comparison Output

**Files:**
- Create: `.gitignore`
- Create: `scripts/run_smoke.py`
- Create: `scripts/compare_results.py`
- Create: `Makefile`
- Modify: `README.md`

- [ ] **Step 1: Write scripts after component CLIs exist**

Implement `run_smoke.py` to check for `cargo`, `go`, and Python dependencies, start the Rust server, run available clients, and put outputs under `results/<timestamp>/<language>/summary.json`. Implement `compare_results.py` to find summary JSON files and print language, requests/sec, chunks/sec, p95 request latency, and failures.

- [ ] **Step 2: Add Makefile commands**

Create targets:

```make
test-python:
	python3 -m unittest discover -s tests -v

test-go:
	cd go-client && go test ./...

test-rust:
	cargo test --manifest-path server-rust/Cargo.toml
	cargo test --manifest-path rust-client/Cargo.toml

smoke:
	python3 scripts/run_smoke.py --config config/workload.smoke.json

compare:
	python3 scripts/compare_results.py results
```

- [ ] **Step 3: Document setup and known local verification limits**

README must include prerequisites, quickstart, smoke commands, direct client commands, result schema, and a note that this environment lacked `cargo` and `go` at implementation time if still true.

- [ ] **Step 4: Run available verification**

Run:

```bash
python3 -m unittest discover -s tests -v
python3 scripts/compare_results.py --help
python3 scripts/run_smoke.py --help
```

Expected: PASS/exit 0. Also run Go and Rust tests if their toolchains are available.

## Task 7: Final Smoke And Review

**Files:**
- All implementation files.

- [ ] **Step 1: Run full available test set**

Run:

```bash
python3 -m unittest discover -s tests -v
make test-go
make test-rust
```

Expected: Python passes. Go and Rust pass if toolchains are installed; otherwise commands fail with missing executable and that limitation is reported.

- [ ] **Step 2: Run smoke benchmark if toolchains are installed**

Run: `python3 scripts/run_smoke.py --config config/workload.smoke.json`

Expected: server starts, available clients run, summaries are written, comparison table prints.

- [ ] **Step 3: Inspect git diff**

Run: `git diff --stat` and `git diff --check`.

Expected: no whitespace errors and only benchmark demo files changed.

- [ ] **Step 4: Commit implementation**

Run:

```bash
git add .
git commit -m "Build local LLM harness overhead benchmark demo"
```

Expected: one implementation commit after the design commit.
