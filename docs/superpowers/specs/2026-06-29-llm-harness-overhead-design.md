# LLM Harness Overhead Benchmark Design

## Purpose

Build a runnable benchmark demo that compares Python, Go, and Rust client harness overhead for an OpenAI-style streaming LLM API workload. The demo should isolate per-request and per-chunk client-side overhead rather than measuring model inference or external network variability.

The target outcome is a credible local demonstration of why a Rust benchmark harness can sustain higher throughput and lower overhead than Go, and why both can outperform Python for high-concurrency streaming workloads.

## Scope

The project will include:

- A local synthetic OpenAI-style streaming server.
- Three benchmark clients: Python, Go, and Rust.
- A shared workload configuration format.
- Normalized result output suitable for later visualization.
- Basic documentation for running the comparison.

The initial version will not benchmark real LLM providers, local model inference, or end-to-end application behavior. Those would add variability that hides the harness-level costs this demo is meant to expose.

## Architecture

The demo uses one synthetic server and three independent clients.

```text
                    shared workload config
                             |
                             v
             +---------------+---------------+
             |                               |
             v                               v
  synthetic OpenAI-style server      benchmark clients
                                     - Python
                                     - Go
                                     - Rust

                             |
                             v
                    normalized results
                    summary JSON files
```

The synthetic server exposes `POST /v1/chat/completions`. When the request includes `"stream": true`, it returns a `text/event-stream` response shaped like OpenAI chat completion streaming: repeated `data: {...}\n\n` events followed by `data: [DONE]\n\n`.

The streamed payload is deterministic. The server does not call a model. It emits configured chunk counts and payload sizes with optional pacing so the clients can be compared under controlled conditions.

## Synthetic Server

The server should be implemented in Rust to keep server overhead low relative to the client harnesses being measured.

Stack:

- Rust
- Tokio runtime
- Axum for HTTP
- Serde for JSON

Endpoint:

- `POST /v1/chat/completions`

Supported request fields:

- `model`: accepted but ignored.
- `messages`: accepted but ignored.
- `stream`: must be `true` for the initial benchmark path.
- Optional benchmark controls in the JSON body:
  - `chunks`: number of SSE chunks per response.
  - `chunk_bytes`: approximate content bytes per chunk.
  - `delay_us`: optional delay between chunks.
  - `request_id`: optional caller-provided ID for correlation.

Response behavior:

- Return HTTP 200 with `content-type: text/event-stream`.
- Emit a first chunk immediately unless `delay_us` is configured to apply before every chunk.
- Emit deterministic JSON chunks with a small OpenAI-compatible shape.
- End each response with `data: [DONE]`.
- Avoid logging per chunk by default, because logging would distort throughput.

The server should expose a simple health endpoint, `GET /health`, so run scripts can wait for readiness.

## Client Harnesses

Each language client should implement the same behavior:

- Load the same workload config.
- Issue `N` total streaming requests with configurable concurrency.
- Parse each SSE event.
- Parse JSON payload chunks, not only raw bytes, because real harnesses usually inspect streamed events.
- Count requests, chunks, bytes, errors, and timing.
- Write normalized result files.

The client harnesses should avoid unnecessary application logic. They should be idiomatic enough for each language while keeping feature parity clear.

Python client:

- Use `asyncio`.
- Use `httpx` for async HTTP streaming.
- Measure the realistic overhead of Python async streaming, line parsing, JSON parsing, and task scheduling.

Go client:

- Use the standard `net/http` client.
- Use goroutines and a bounded concurrency mechanism.
- Parse SSE lines and JSON chunks explicitly.

Rust client:

- Use Tokio.
- Use `reqwest` for async HTTP streaming.
- Parse SSE lines and JSON chunks explicitly.

## Workload Configuration

Use one shared config file so all clients run the same workload.

Example fields:

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

The first implementation can keep this as JSON for all languages. TOML can be added later if the project grows.

## Metrics

Each client should report:

- Total wall-clock duration.
- Successful requests.
- Failed requests.
- Total chunks parsed.
- Total streamed content bytes.
- Requests per second.
- Chunks per second.
- Mean request latency.
- p50, p95, and p99 request latency.
- Mean time to first chunk.
- p50, p95, and p99 time to first chunk.
- Approximate per-chunk overhead, computed as client wall time divided by parsed chunks for zero-delay workloads.

Optional metrics if cheap and portable:

- Process RSS at end of run.
- CPU time.
- Per-request allocation counters where the language runtime exposes them easily.

The first version should not block on perfect cross-platform CPU or memory accounting. Throughput and latency are the core comparison.

## Result Format

Each client run writes one `summary.json` file:

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
    "mean_request_latency_ms": 25.1,
    "p50_request_latency_ms": 23.0,
    "p95_request_latency_ms": 41.2,
    "p99_request_latency_ms": 58.9,
    "mean_time_to_first_chunk_ms": 2.1,
    "p50_time_to_first_chunk_ms": 1.8,
    "p95_time_to_first_chunk_ms": 3.9,
    "p99_time_to_first_chunk_ms": 6.2
  }
}
```

The format is intentionally visualization-friendly. A later step can add a small charting script or dashboard without changing the harnesses.

## Run Workflow

The demo should support a simple local workflow:

1. Build the Rust synthetic server.
2. Start the server on `127.0.0.1:8080`.
3. Run the Python, Go, and Rust clients against the same config.
4. Write results under `results/<timestamp>/`.
5. Print a compact comparison table in the terminal.

A top-level script or Makefile can orchestrate this once the individual components exist.

## Error Handling

The server should:

- Return 400 for malformed JSON.
- Return 400 when `stream` is missing or false for the initial benchmark endpoint.
- Return 400 for invalid benchmark controls, such as negative chunk counts.
- Handle client disconnects without treating them as server failures.

The clients should:

- Count HTTP errors separately from stream parsing errors.
- Treat missing `[DONE]` as a failed request.
- Treat malformed SSE JSON as a failed request.
- Continue running other requests after individual failures.
- Include error counts in the summary output.

## Testing

Testing should focus on correctness of the benchmark harness before trusting performance numbers.

Server tests:

- Health endpoint returns 200.
- Streaming endpoint emits the configured number of chunks.
- Final event is `[DONE]`.
- Invalid requests return 400.

Client tests:

- SSE parser handles multiple events.
- SSE parser handles partial line boundaries.
- Result aggregation computes expected totals and percentiles.
- Failed streams are counted as failures.

End-to-end smoke test:

- Start the synthetic server.
- Run a tiny workload from each client, such as 10 requests, concurrency 2, 3 chunks per response.
- Verify all clients produce comparable totals.

Performance runs should be separate from correctness tests because machine load can make performance assertions flaky.

## Tradeoffs

Using a custom synthetic server gives tighter control than off-the-shelf mock servers and avoids pulling Python or model inference into the measurement path. The tradeoff is that the server must maintain enough OpenAI-like behavior to be credible.

Implementing clients directly in each language is more work than driving all clients through SDKs, but it measures the harness loop, streaming parser, JSON parser, and concurrency runtime more clearly.

The initial benchmark will not prove absolute superiority for every real-world LLM workload. It will demonstrate overhead differences in a controlled streaming scenario, which is the right claim for this repo.

## Open Questions Resolved

- Use local synthetic workload rather than a real LLM API.
- Use an OpenAI-style streaming endpoint rather than a custom raw stream.
- Prefer a custom synthetic server over LiteLLM, vLLM, llama.cpp, or a general-purpose mock server.
- Keep visualization as a follow-on by producing normalized result files now.
