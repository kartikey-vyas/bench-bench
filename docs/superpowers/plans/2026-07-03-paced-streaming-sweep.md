# Paced Streaming + Concurrency Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ad-hoc `delay_us` pacing with deadline-based server pacing (`ttfc_ms` + `events_per_second`), make clients duration-based with completeness/gap metrics, add a drain-only reference client and server schedule-slip stats, and add a concurrency-sweep runner plus an efficiency-vs-concurrency report that shows where each client underrepresents server performance.

**Architecture:** The Rust Axum server prebuilds one role event, one content event, and one finale (finish+`[DONE]`) per request as shared `Bytes`, then streams them against absolute deadlines (`first_due + i*interval`) with catch-up batching — a slow client gets coalesced bursts, never a stretched schedule. Clients (Python/httpx, Go/net-http, Rust/reqwest+hyper, plus a byte-drain hyper reference) run closed-loop worker pools for a fixed wall-clock duration and emit an identical `summary.json` schema. A sweep runner iterates (tier × concurrency × repeat × client) cells against one server process, records server slip stats and CPU per cell, and applies stop rules per client per tier. A report generator renders efficiency-vs-concurrency lines per tier.

**Tech Stack:** Rust (axum 0.7, tokio, hyper 1, hyper-util), Go 1.22 stdlib, Python 3.12 (httpx, stdlib `unittest`), static HTML/SVG reports (no JS frameworks).

## Global Constraints

- Python tests run with `python3 -m unittest discover -s tests -v` (repo uses `unittest`, NOT pytest). Go: `cd go-client && go test ./...`. Rust: `cargo test --manifest-path server-rust/Cargo.toml` and `cargo test --manifest-path rust-client/Cargo.toml`.
- Python is 3.12+; the only third-party Python dependency is `httpx` (do not add new Python deps — sweep runner and report use stdlib only).
- Go client uses stdlib only.
- The wire protocol (request fields and stream shape) is defined below; every client and the server must match it exactly.
- The `summary.json` schema (below) must be byte-key-identical across Python, Go, and both Rust clients and the drain client.
- `request_id` format is `{language}-{worker_index}-{sequence}`.
- All checked-in workload configs use `chunk_bytes: 8`.
- Commit after each task with the repo's imperative style (e.g. "Add deadline-based server pacing").

## Shared Contracts (referenced by every task)

### Request payload (client → server, POST /v1/chat/completions)

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

Server defaults/limits: `chunks` default 64, 1..=1_000_000; `chunk_bytes` default 32, 1..=1_048_576 (zero now rejected); `ttfc_ms` default 0, max 60_000; `events_per_second` default 0 (0 = unpaced/max-speed), max 1_000_000.

### Stream shape (request arrival = t0, all SSE `data:` events)

1. Role event `delta:{"role":"assistant","content":""}` — due at `t0 + ttfc_ms`.
2. `chunks` content events `delta:{"content":"xxx…"}` — content event `i` (0-indexed) due at `t0 + ttfc_ms + i/events_per_second`; the first coincides with the role event.
3. Finish event `delta:{}` with `"finish_reason":"stop"`, then `data: [DONE]` — both immediately after the last content event (not paced).

Total SSE events = `chunks + 3`. With `events_per_second: 0` everything is due immediately (one coalesced batch).

### Workload config JSON (replaces total_requests/warmup_requests/delay_us)

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

### Per-request measurement fields (all clients)

`ok` (transport+parse success and saw `[DONE]`; drain: read to EOF), `latency_ms`, `first_chunk_ms` (first parsed SSE event — the role event), `chunks` (content events with non-empty `delta.content` only), `bytes` (content bytes; drain: wire bytes), `max_gap_ms` (max gap between successive parsed events), `stream_ms` (first event → last event).

### summary.json `summary` keys (identical in all clients)

`duration_ms`, `successful_requests`, `incomplete_requests`, `failed_requests`, `total_chunks`, `total_bytes`, `requests_per_second`, `chunks_per_second`, `mean_request_latency_ms`, `p50_request_latency_ms`, `p95_request_latency_ms`, `p99_request_latency_ms`, `mean_time_to_first_chunk_ms`, `p50_time_to_first_chunk_ms`, `p95_time_to_first_chunk_ms`, `p99_time_to_first_chunk_ms`, `p50_max_gap_ms`, `p95_max_gap_ms`, `p99_max_gap_ms`, `max_max_gap_ms`, `p50_stream_stretch`, `p95_stream_stretch`, `p99_stream_stretch`, `ideal_events_per_second`, `efficiency`.

Aggregation rules (identical everywhere):
- successful = `ok && chunks == chunks_per_response`; incomplete = `ok && chunks != expected`; failed = `!ok`.
- Percentiles (nearest-rank, existing implementations) over successful requests only; totals over successful only.
- `ideal_stream_ms = (expected - 1) / events_per_second * 1000` when `events_per_second > 0 && expected > 1`, else stretch list is empty and stretch percentiles are 0.0. `stream_stretch = stream_ms / ideal_stream_ms` per successful request.
- **[AMENDED after Task 9 integration]** `ideal_events_per_second` must account for TTFC dead time in the closed loop: when `events_per_second > 0`, `ideal_request_seconds = ttfc_ms/1000 + (expected - 1)/events_per_second`, and `ideal_events_per_second = concurrency * expected / ideal_request_seconds` (0.0 if `ideal_request_seconds` is 0). When unpaced, 0.0. `efficiency = chunks_per_second / ideal_events_per_second` (0.0 when unpaced). The original `eps × concurrency` definition was unreachable for any client (a perfect closed-loop client idles through TTFC every request) and would have falsely triggered stop rules. Python's `aggregate_summary` gains a `ttfc_ms` parameter.
- `requests_per_second = successful / duration_seconds`; `chunks_per_second = total_chunks / duration_seconds`.
- `per_chunk_overhead_ms` is REMOVED from the schema.

### Result envelope (unchanged shape)

`{"language", "implementation", "started_at", "config", "summary"}` — config is the full workload config.

---

### Task 1: Server — deadline-paced streaming, shared-Bytes events, slip stats

**Files:**
- Modify: `server-rust/src/lib.rs` (full rewrite below)
- Modify: `server-rust/Cargo.toml` (dev-deps)
- Modify: `server-rust/tests/streaming.rs` (full rewrite below)

**Interfaces:**
- Consumes: nothing new.
- Produces: `pub struct ChatRequest` (fields `model, messages, stream, chunks, chunk_bytes, ttfc_ms: Option<u64>, events_per_second: Option<u64>, request_id`), `pub fn validate_request(&ChatRequest) -> Result<(), String>`, `pub struct StreamPlan { chunks, ttfc, interval: Option<Duration>, role_event/content_event/finale: Bytes }` with `pub fn batch(&self, include_role: bool, count: usize) -> Bytes`, `pub fn build_stream_plan(&ChatRequest) -> Result<StreamPlan, String>`, `pub struct ServerStats` with `new()/record_batch(events: u64, slip: Duration)/reset()/snapshot() -> serde_json::Value`, `pub fn app() -> Router`, `pub fn app_with_stats(Arc<ServerStats>) -> Router`. Routes: `GET /health`, `GET /stats`, `POST /stats/reset`, `POST /v1/chat/completions`. Task 2 (main.rs) and Task 10 (sweep runner, via HTTP) rely on these.

- [ ] **Step 1: Update `server-rust/Cargo.toml` dev-dependencies**

Replace the `[dev-dependencies]` section with:

```toml
[dev-dependencies]
tower = { version = "0.5", features = ["util"] }
http-body-util = "0.1"
```

- [ ] **Step 2: Write the failing tests — replace `server-rust/tests/streaming.rs` entirely**

```rust
use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;
use server_rust::{app, app_with_stats, build_stream_plan, validate_request, ChatRequest, ServerStats};
use tower::ServiceExt;

fn request(chunks: usize, chunk_bytes: usize, ttfc_ms: u64, events_per_second: u64) -> ChatRequest {
    ChatRequest {
        model: Some("synthetic".to_string()),
        messages: vec![],
        stream: true,
        chunks: Some(chunks),
        chunk_bytes: Some(chunk_bytes),
        ttfc_ms: Some(ttfc_ms),
        events_per_second: Some(events_per_second),
        request_id: Some("req-1".to_string()),
    }
}

async fn post_chat(router: axum::Router, body: &str) -> axum::response::Response {
    let request = http::Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    router.oneshot(request).await.unwrap()
}

#[test]
fn stream_plan_builds_role_content_and_finale_events() {
    let plan = build_stream_plan(&request(2, 4, 0, 0)).unwrap();
    let role = std::str::from_utf8(&plan.role_event).unwrap();
    let content = std::str::from_utf8(&plan.content_event).unwrap();
    let finale = std::str::from_utf8(&plan.finale).unwrap();

    assert!(role.contains("\"role\":\"assistant\""));
    assert!(role.starts_with("data: {"));
    assert!(content.contains("\"content\":\"xxxx\""));
    assert!(finale.contains("\"finish_reason\":\"stop\""));
    assert!(finale.ends_with("data: [DONE]\n\n"));
    assert_eq!(plan.chunks, 2);
    assert!(plan.interval.is_none());
    assert_eq!(plan.ttfc, Duration::ZERO);
}

#[test]
fn stream_plan_derives_interval_and_ttfc() {
    let plan = build_stream_plan(&request(2, 4, 200, 500)).unwrap();
    assert_eq!(plan.ttfc, Duration::from_millis(200));
    assert_eq!(plan.interval, Some(Duration::from_secs_f64(1.0 / 500.0)));
}

#[test]
fn batch_concatenates_role_and_content_events() {
    let plan = build_stream_plan(&request(4, 4, 0, 0)).unwrap();
    let batch = plan.batch(true, 2);
    assert_eq!(batch.len(), plan.role_event.len() + 2 * plan.content_event.len());
    let single = plan.batch(false, 1);
    assert_eq!(single, plan.content_event);
}

#[test]
fn rejects_non_streaming_and_zero_chunk_bytes() {
    let mut non_streaming = request(1, 1, 0, 0);
    non_streaming.stream = false;
    assert_eq!(validate_request(&non_streaming).unwrap_err(), "stream must be true");

    let zero_bytes = request(1, 0, 0, 0);
    assert_eq!(
        validate_request(&zero_bytes).unwrap_err(),
        "chunk_bytes must be greater than zero"
    );
}

#[test]
fn stats_records_batches_and_resets() {
    let stats = ServerStats::new();
    stats.record_batch(3, Duration::from_micros(400));
    stats.record_batch(1, Duration::from_millis(30));

    let snapshot = stats.snapshot();
    assert_eq!(snapshot["events_emitted"], 4);
    assert_eq!(snapshot["slip_max_ms"], 30.0);
    assert_eq!(snapshot["slip_p50_ms"], 0.5);
    assert_eq!(snapshot["slip_p95_ms"], 50.0);

    stats.reset();
    assert_eq!(stats.snapshot()["events_emitted"], 0);
}

#[tokio::test]
async fn unpaced_stream_returns_role_content_finish_done_in_order() {
    let response = post_chat(
        app(),
        r#"{"stream":true,"chunks":3,"chunk_bytes":4,"ttfc_ms":0,"events_per_second":0}"#,
    )
    .await;
    assert_eq!(response.status(), 200);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = std::str::from_utf8(&body).unwrap();
    let events: Vec<&str> = text.trim_end().split("\n\n").collect();

    assert_eq!(events.len(), 6, "role + 3 content + finish + done, got {events:?}");
    assert!(events[0].contains("\"role\":\"assistant\""));
    for event in &events[1..4] {
        assert!(event.contains("\"content\":\"xxxx\""));
    }
    assert!(events[4].contains("\"finish_reason\":\"stop\""));
    assert_eq!(events[5], "data: [DONE]");
}

#[tokio::test]
async fn paced_stream_takes_at_least_the_scheduled_duration() {
    let started = std::time::Instant::now();
    let response = post_chat(
        app(),
        r#"{"stream":true,"chunks":6,"chunk_bytes":4,"ttfc_ms":0,"events_per_second":100}"#,
    )
    .await;
    let _ = response.into_body().collect().await.unwrap();
    let elapsed = started.elapsed();
    // Last content deadline is 50ms after the first; allow scheduler tolerance.
    assert!(elapsed >= Duration::from_millis(45), "stream finished too fast: {elapsed:?}");
}

#[tokio::test]
async fn stats_endpoint_reports_and_resets_over_http() {
    let stats = Arc::new(ServerStats::new());
    let router = app_with_stats(stats.clone());

    let response = post_chat(
        router.clone(),
        r#"{"stream":true,"chunks":2,"chunk_bytes":4,"ttfc_ms":0,"events_per_second":0}"#,
    )
    .await;
    let _ = response.into_body().collect().await.unwrap();

    let stats_response = router
        .clone()
        .oneshot(http::Request::get("/stats").body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    let body = stats_response.into_body().collect().await.unwrap().to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["requests_started"], 1);
    assert_eq!(value["requests_completed"], 1);
    assert_eq!(value["events_emitted"], 2);

    let reset = router
        .clone()
        .oneshot(http::Request::post("/stats/reset").body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(reset.status(), http::StatusCode::NO_CONTENT);
}
```

Note: `http` is already a direct dependency of server-rust, so `http::Request` resolves.

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --manifest-path server-rust/Cargo.toml`
Expected: compile errors — `ChatRequest` has no `ttfc_ms`/`events_per_second`, `build_stream_plan`/`ServerStats`/`app_with_stats` not found.

- [ ] **Step 4: Replace `server-rust/src/lib.rs` entirely**

```rust
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_stream::stream;
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::{sleep_until, Instant};

const DEFAULT_CHUNKS: usize = 64;
const DEFAULT_CHUNK_BYTES: usize = 32;
const MAX_CHUNKS: usize = 1_000_000;
const MAX_CHUNK_BYTES: usize = 1_048_576;
const MAX_TTFC_MS: u64 = 60_000;
const MAX_EVENTS_PER_SECOND: u64 = 1_000_000;

/// Upper bounds (µs) of the schedule-slip histogram buckets; one overflow bucket follows.
const SLIP_BUCKET_BOUNDS_US: [u64; 11] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
];

#[derive(Debug, Clone, Deserialize)]
pub struct ChatRequest {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub messages: Vec<Value>,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub chunks: Option<usize>,
    #[serde(default)]
    pub chunk_bytes: Option<usize>,
    #[serde(default)]
    pub ttfc_ms: Option<u64>,
    #[serde(default)]
    pub events_per_second: Option<u64>,
    #[serde(default)]
    pub request_id: Option<String>,
}

/// Everything a stream needs, prebuilt once per request. All content events in a
/// request are identical, so they are serialized once and cloned (refcount bump).
#[derive(Debug, Clone)]
pub struct StreamPlan {
    pub chunks: usize,
    pub ttfc: Duration,
    /// None = unpaced (events_per_second == 0): everything is due immediately.
    pub interval: Option<Duration>,
    pub role_event: Bytes,
    pub content_event: Bytes,
    pub finale: Bytes,
}

impl StreamPlan {
    /// One coalesced body frame: optional role event plus `count` content events.
    pub fn batch(&self, include_role: bool, count: usize) -> Bytes {
        if !include_role && count == 1 {
            return self.content_event.clone();
        }
        let role_len = if include_role { self.role_event.len() } else { 0 };
        let mut buffer = BytesMut::with_capacity(role_len + count * self.content_event.len());
        if include_role {
            buffer.put_slice(&self.role_event);
        }
        for _ in 0..count {
            buffer.put_slice(&self.content_event);
        }
        buffer.freeze()
    }
}

pub fn validate_request(request: &ChatRequest) -> Result<(), String> {
    if !request.stream {
        return Err("stream must be true".to_string());
    }
    let chunks = request.chunks.unwrap_or(DEFAULT_CHUNKS);
    if chunks == 0 {
        return Err("chunks must be greater than zero".to_string());
    }
    if chunks > MAX_CHUNKS {
        return Err(format!("chunks must be <= {MAX_CHUNKS}"));
    }
    let chunk_bytes = request.chunk_bytes.unwrap_or(DEFAULT_CHUNK_BYTES);
    if chunk_bytes == 0 {
        return Err("chunk_bytes must be greater than zero".to_string());
    }
    if chunk_bytes > MAX_CHUNK_BYTES {
        return Err(format!("chunk_bytes must be <= {MAX_CHUNK_BYTES}"));
    }
    if request.ttfc_ms.unwrap_or(0) > MAX_TTFC_MS {
        return Err(format!("ttfc_ms must be <= {MAX_TTFC_MS}"));
    }
    if request.events_per_second.unwrap_or(0) > MAX_EVENTS_PER_SECOND {
        return Err(format!("events_per_second must be <= {MAX_EVENTS_PER_SECOND}"));
    }
    Ok(())
}

fn sse_event(id: &str, model: &str, delta: &Value, finish_reason: &Value) -> Result<String, String> {
    let payload = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": 0,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]
    });
    let encoded = serde_json::to_string(&payload).map_err(|error| error.to_string())?;
    Ok(format!("data: {encoded}\n\n"))
}

pub fn build_stream_plan(request: &ChatRequest) -> Result<StreamPlan, String> {
    validate_request(request)?;
    let chunks = request.chunks.unwrap_or(DEFAULT_CHUNKS);
    let chunk_bytes = request.chunk_bytes.unwrap_or(DEFAULT_CHUNK_BYTES);
    let model = request.model.as_deref().unwrap_or("synthetic");
    let id = request.request_id.as_deref().unwrap_or("chatcmpl-synthetic");
    let events_per_second = request.events_per_second.unwrap_or(0);

    let role_event = sse_event(id, model, &json!({"role": "assistant", "content": ""}), &Value::Null)?;
    let content_event = sse_event(id, model, &json!({"content": "x".repeat(chunk_bytes)}), &Value::Null)?;
    let finish_event = sse_event(id, model, &json!({}), &json!("stop"))?;
    let finale = format!("{finish_event}data: [DONE]\n\n");

    Ok(StreamPlan {
        chunks,
        ttfc: Duration::from_millis(request.ttfc_ms.unwrap_or(0)),
        interval: (events_per_second > 0)
            .then(|| Duration::from_secs_f64(1.0 / events_per_second as f64)),
        role_event: Bytes::from(role_event),
        content_event: Bytes::from(content_event),
        finale: Bytes::from(finale),
    })
}

#[derive(Debug)]
pub struct ServerStats {
    requests_started: AtomicU64,
    requests_completed: AtomicU64,
    events_emitted: AtomicU64,
    max_slip_us: AtomicU64,
    slip_buckets: [AtomicU64; SLIP_BUCKET_BOUNDS_US.len() + 1],
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            requests_started: AtomicU64::new(0),
            requests_completed: AtomicU64::new(0),
            events_emitted: AtomicU64::new(0),
            max_slip_us: AtomicU64::new(0),
            slip_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    pub fn record_request_started(&self) {
        self.requests_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_request_completed(&self) {
        self.requests_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one emitted batch: how many events it carried and how late the
    /// newest event in it was relative to its deadline.
    pub fn record_batch(&self, events: u64, slip: Duration) {
        self.events_emitted.fetch_add(events, Ordering::Relaxed);
        let slip_us = u64::try_from(slip.as_micros()).unwrap_or(u64::MAX);
        self.max_slip_us.fetch_max(slip_us, Ordering::Relaxed);
        let index = SLIP_BUCKET_BOUNDS_US
            .iter()
            .position(|bound| slip_us <= *bound)
            .unwrap_or(SLIP_BUCKET_BOUNDS_US.len());
        self.slip_buckets[index].fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset(&self) {
        self.requests_started.store(0, Ordering::Relaxed);
        self.requests_completed.store(0, Ordering::Relaxed);
        self.events_emitted.store(0, Ordering::Relaxed);
        self.max_slip_us.store(0, Ordering::Relaxed);
        for bucket in &self.slip_buckets {
            bucket.store(0, Ordering::Relaxed);
        }
    }

    fn slip_quantile_ms(&self, counts: &[u64], quantile: f64) -> f64 {
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return 0.0;
        }
        let target = ((quantile * total as f64).ceil() as u64).max(1);
        let mut cumulative = 0u64;
        for (index, count) in counts.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                // Quantiles are reported as the bucket's upper bound (conservative).
                let upper_us = SLIP_BUCKET_BOUNDS_US
                    .get(index)
                    .copied()
                    .unwrap_or_else(|| self.max_slip_us.load(Ordering::Relaxed));
                return upper_us as f64 / 1000.0;
            }
        }
        self.max_slip_us.load(Ordering::Relaxed) as f64 / 1000.0
    }

    pub fn snapshot(&self) -> Value {
        let counts: Vec<u64> = self
            .slip_buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        json!({
            "requests_started": self.requests_started.load(Ordering::Relaxed),
            "requests_completed": self.requests_completed.load(Ordering::Relaxed),
            "events_emitted": self.events_emitted.load(Ordering::Relaxed),
            "slip_p50_ms": self.slip_quantile_ms(&counts, 0.50),
            "slip_p95_ms": self.slip_quantile_ms(&counts, 0.95),
            "slip_p99_ms": self.slip_quantile_ms(&counts, 0.99),
            "slip_max_ms": self.max_slip_us.load(Ordering::Relaxed) as f64 / 1000.0,
        })
    }
}

impl Default for ServerStats {
    fn default() -> Self {
        Self::new()
    }
}

pub fn app() -> Router {
    app_with_stats(Arc::new(ServerStats::new()))
}

pub fn app_with_stats(stats: Arc<ServerStats>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/stats", get(get_stats))
        .route("/stats/reset", post(reset_stats))
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(stats)
}

async fn health() -> &'static str {
    "ok"
}

async fn get_stats(State(stats): State<Arc<ServerStats>>) -> Json<Value> {
    Json(stats.snapshot())
}

async fn reset_stats(State(stats): State<Arc<ServerStats>>) -> StatusCode {
    stats.reset();
    StatusCode::NO_CONTENT
}

async fn chat_completions(
    State(stats): State<Arc<ServerStats>>,
    Json(request): Json<ChatRequest>,
) -> Result<Response, (StatusCode, String)> {
    let plan = build_stream_plan(&request).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    stats.record_request_started();

    let response_stream = stream! {
        let start = Instant::now();
        let first_due = start + plan.ttfc;
        if !plan.ttfc.is_zero() {
            sleep_until(first_due).await;
        }

        let mut sent = 0usize;
        while sent < plan.chunks {
            // Absolute-deadline catch-up: emit every content event whose deadline
            // has passed as one coalesced frame, so a slow wakeup produces a burst
            // instead of stretching the schedule.
            let due = match plan.interval {
                None => plan.chunks,
                Some(interval) => {
                    let since_first = Instant::now().duration_since(first_due);
                    let due = (since_first.as_secs_f64() / interval.as_secs_f64()) as usize + 1;
                    due.min(plan.chunks)
                }
            };

            if due > sent {
                let batch_deadline = match plan.interval {
                    None => first_due,
                    Some(interval) => first_due + interval.mul_f64((due - 1) as f64),
                };
                let slip = Instant::now().duration_since(batch_deadline);
                stats.record_batch((due - sent) as u64, slip);
                let batch = plan.batch(sent == 0, due - sent);
                sent = due;
                yield Ok::<Bytes, Infallible>(batch);
            }

            if sent < plan.chunks {
                let interval = plan
                    .interval
                    .expect("unpaced streams emit all chunks in one batch");
                sleep_until(first_due + interval.mul_f64(sent as f64)).await;
            }
        }

        stats.record_request_completed();
        yield Ok::<Bytes, Infallible>(plan.finale.clone());
    };

    let mut response = Response::new(Body::from_stream(response_stream));
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    Ok(response)
}
```

Note: `tokio::time::Instant::duration_since` saturates to zero when the argument is later (unlike `std::time::Instant`), which is exactly what the slip computation needs.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --manifest-path server-rust/Cargo.toml`
Expected: all tests PASS (the timing test may take ~60ms).

- [ ] **Step 6: Commit**

```bash
git add server-rust
git commit -m "Add deadline-based pacing, prebuilt events, and slip stats to server"
```

### Task 2: Server binary — TCP_NODELAY accept loop

**Files:**
- Modify: `server-rust/src/main.rs` (full rewrite below)
- Modify: `server-rust/Cargo.toml` (add deps)

**Interfaces:**
- Consumes: `server_rust::app()` from Task 1.
- Produces: the `synthetic-openai-server` binary (same CLI: `--bind <addr>`), now setting `TCP_NODELAY` on every accepted socket. Tasks 9/10 spawn this binary.

Nagle + delayed ACK would distort millisecond-scale SSE pacing; `axum::serve` does not guarantee nodelay, so use the standard axum "serve with hyper" accept loop.

- [ ] **Step 1: Add dependencies to `server-rust/Cargo.toml` `[dependencies]`**

```toml
hyper = { version = "1", features = ["server", "http1"] }
hyper-util = { version = "0.1", features = ["server", "server-auto", "http1", "tokio"] }
tower = "0.5"
```

(Keep the existing `[dev-dependencies]` from Task 1 — cargo merges features.)

- [ ] **Step 2: Replace `server-rust/src/main.rs` entirely**

```rust
use std::net::SocketAddr;

use clap::Parser;
use hyper::body::Incoming;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use server_rust::app;
use tokio::net::TcpListener;
use tower::Service;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let listener = TcpListener::bind(args.bind).await?;
    println!("synthetic OpenAI-style server listening on http://{}", args.bind);
    let router = app();

    loop {
        let (socket, _remote) = listener.accept().await?;
        // Nagle + delayed ACK would distort millisecond-scale SSE pacing.
        socket.set_nodelay(true)?;
        let service = router.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(socket);
            let hyper_service = hyper::service::service_fn(
                move |request: hyper::Request<Incoming>| service.clone().call(request),
            );
            let _ = auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, hyper_service)
                .await;
        });
    }
}
```

- [ ] **Step 3: Verify it builds and serves**

Run: `cargo build --release --manifest-path server-rust/Cargo.toml`
Expected: clean build.

Run (background the server, then):
```bash
server-rust/target/release/synthetic-openai-server --bind 127.0.0.1:8099 &
SERVER_PID=$!
sleep 1
curl -s http://127.0.0.1:8099/health
curl -s http://127.0.0.1:8099/stats
curl -sN -X POST http://127.0.0.1:8099/v1/chat/completions -H 'content-type: application/json' \
  -d '{"stream":true,"chunks":3,"chunk_bytes":4,"ttfc_ms":50,"events_per_second":100}'
kill $SERVER_PID
```
Expected: `ok`; a JSON stats object; an SSE stream with role event, 3 content events, finish event, `data: [DONE]`.

- [ ] **Step 4: Commit**

```bash
git add server-rust
git commit -m "Serve with TCP_NODELAY via manual hyper accept loop"
```

---

### Task 3: Python — workload config fields

**Files:**
- Modify: `bench_harness/config.py` (full rewrite below)
- Modify: `tests/test_config.py` (full rewrite below)

**Interfaces:**
- Consumes: nothing.
- Produces: `WorkloadConfig` dataclass with fields `base_url: str, duration_seconds: float, warmup_seconds: float, concurrency: int, chunks_per_response: int, chunk_bytes: int, ttfc_ms: int, events_per_second: int, output_dir: str`; `request_payload(worker_index: int, sequence: int, language: str) -> dict`; `endpoint` property and `result_config()` unchanged. Tasks 5 and 10 rely on these names. NOTE: the checked-in `config/workload.*.json` files are rewritten in Task 9; until then `test_checked_in_*` tests must only assert on files that exist with new fields, so this task rewrites the workload configs' *tests* to check pacing fields and Task 9 rewrites the JSON — to keep the suite green in between, this task ALSO rewrites the three workload JSON files (small, no behavior).

- [ ] **Step 1: Write the failing tests — replace `tests/test_config.py` entirely**

```python
import json
import tempfile
import unittest
from pathlib import Path

from bench_harness.config import WorkloadConfig


def workload_json() -> str:
    return (
        '{"base_url":"http://127.0.0.1:8080","duration_seconds":2.0,'
        '"warmup_seconds":0.5,"concurrency":2,"chunks_per_response":4,'
        '"chunk_bytes":8,"ttfc_ms":200,"events_per_second":500,'
        '"output_dir":"results"}'
    )


class WorkloadConfigTests(unittest.TestCase):
    def test_workload_config_loads_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "workload.json"
            config_path.write_text(workload_json())

            config = WorkloadConfig.from_path(config_path)

        self.assertEqual(config.duration_seconds, 2.0)
        self.assertEqual(config.warmup_seconds, 0.5)
        payload = config.request_payload(1, 7, "python")
        self.assertEqual(payload["chunks"], 4)
        self.assertEqual(payload["ttfc_ms"], 200)
        self.assertEqual(payload["events_per_second"], 500)
        self.assertEqual(payload["request_id"], "python-1-7")

    def test_rejects_zero_chunk_bytes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "workload.json"
            config_path.write_text(workload_json().replace('"chunk_bytes":8', '"chunk_bytes":0'))
            with self.assertRaises(ValueError):
                WorkloadConfig.from_path(config_path)

    def test_checked_in_workloads_have_pacing_and_duration_fields(self):
        for path in Path("config").glob("workload.*.json"):
            with self.subTest(path=path):
                data = json.loads(path.read_text())
                self.assertIn("duration_seconds", data)
                self.assertIn("warmup_seconds", data)
                self.assertIn("ttfc_ms", data)
                self.assertIn("events_per_second", data)
                self.assertEqual(data["chunk_bytes"], 8)

    def test_checked_in_comparison_workloads_use_at_least_512_chunks(self):
        for name in ("workload.default.json", "workload.compare.json"):
            with self.subTest(name=name):
                data = json.loads((Path("config") / name).read_text())
                self.assertGreaterEqual(data["chunks_per_response"], 512)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_config -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'duration_seconds'` and missing-field assertions.

- [ ] **Step 3: Replace `bench_harness/config.py` entirely**

```python
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WorkloadConfig:
    base_url: str
    duration_seconds: float
    warmup_seconds: float
    concurrency: int
    chunks_per_response: int
    chunk_bytes: int
    ttfc_ms: int
    events_per_second: int
    output_dir: str

    @classmethod
    def from_path(cls, path: str | Path) -> "WorkloadConfig":
        data = json.loads(Path(path).read_text())
        config = cls(**data)
        config.validate()
        return config

    @property
    def endpoint(self) -> str:
        return f"{self.base_url.rstrip('/')}/v1/chat/completions"

    def validate(self) -> None:
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be > 0")
        if self.warmup_seconds < 0:
            raise ValueError("warmup_seconds must be >= 0")
        if self.concurrency <= 0:
            raise ValueError("concurrency must be > 0")
        if self.chunks_per_response <= 0:
            raise ValueError("chunks_per_response must be > 0")
        if self.chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be > 0")
        if self.ttfc_ms < 0:
            raise ValueError("ttfc_ms must be >= 0")
        if self.events_per_second < 0:
            raise ValueError("events_per_second must be >= 0")

    def request_payload(self, worker_index: int, sequence: int, language: str) -> dict[str, Any]:
        return {
            "model": "synthetic",
            "messages": [{"role": "user", "content": "benchmark"}],
            "stream": True,
            "chunks": self.chunks_per_response,
            "chunk_bytes": self.chunk_bytes,
            "ttfc_ms": self.ttfc_ms,
            "events_per_second": self.events_per_second,
            "request_id": f"{language}-{worker_index}-{sequence}",
        }

    def result_config(self) -> dict[str, Any]:
        return asdict(self)
```

- [ ] **Step 4: Rewrite the three workload JSON files**

`config/workload.smoke.json`:
```json
{
  "base_url": "http://127.0.0.1:8080",
  "duration_seconds": 2.0,
  "warmup_seconds": 0.5,
  "concurrency": 2,
  "chunks_per_response": 64,
  "chunk_bytes": 8,
  "ttfc_ms": 20,
  "events_per_second": 2000,
  "output_dir": "results"
}
```

`config/workload.default.json`:
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

`config/workload.compare.json` (max-speed tier):
```json
{
  "base_url": "http://127.0.0.1:8080",
  "duration_seconds": 15.0,
  "warmup_seconds": 3.0,
  "concurrency": 64,
  "chunks_per_response": 512,
  "chunk_bytes": 8,
  "ttfc_ms": 0,
  "events_per_second": 0,
  "output_dir": "results"
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_config -v`
Expected: PASS (4 tests). Note `tests.test_metrics` and the Python client are now broken until Tasks 4–5 — that is expected mid-plan; run only `tests.test_config` here.

- [ ] **Step 6: Commit**

```bash
git add bench_harness/config.py tests/test_config.py config/workload.smoke.json config/workload.default.json config/workload.compare.json
git commit -m "Replace request-count workload config with duration and pacing fields"
```

---

### Task 4: Python — metrics with completeness, gaps, stretch, efficiency

**Files:**
- Modify: `bench_harness/metrics.py` (full rewrite below)
- Modify: `tests/test_metrics.py` (full rewrite below)

**Interfaces:**
- Consumes: nothing.
- Produces: `RequestMeasurement(ok, latency_ms, first_chunk_ms, chunks, bytes, max_gap_ms, stream_ms)`; `aggregate_summary(measurements, duration_ms, expected_chunks: int, events_per_second: int, concurrency: int) -> dict` emitting exactly the shared summary keys; `percentile`/`mean` unchanged. Task 5 calls `aggregate_summary` with keyword args.

- [ ] **Step 1: Write the failing tests — replace `tests/test_metrics.py` entirely**

```python
import unittest

from bench_harness.metrics import RequestMeasurement, aggregate_summary, percentile


def measurement(ok=True, latency_ms=10.0, first_chunk_ms=2.0, chunks=4, bytes=32,
                max_gap_ms=1.0, stream_ms=30.0):
    return RequestMeasurement(
        ok=ok, latency_ms=latency_ms, first_chunk_ms=first_chunk_ms,
        chunks=chunks, bytes=bytes, max_gap_ms=max_gap_ms, stream_ms=stream_ms,
    )


class MetricsTests(unittest.TestCase):
    def test_percentile_uses_nearest_rank(self):
        self.assertEqual(percentile([10.0, 20.0, 30.0, 40.0], 0.50), 20.0)
        self.assertEqual(percentile([10.0, 20.0, 30.0, 40.0], 0.95), 40.0)

    def test_aggregate_summary_classifies_and_computes_efficiency(self):
        measurements = [
            measurement(chunks=4, stream_ms=30.0, max_gap_ms=12.0),
            measurement(chunks=3),               # incomplete: ok but wrong count
            measurement(ok=False, chunks=0),     # failed
        ]

        summary = aggregate_summary(
            measurements, duration_ms=1000.0,
            expected_chunks=4, events_per_second=100, concurrency=2,
        )

        self.assertEqual(summary["successful_requests"], 1)
        self.assertEqual(summary["incomplete_requests"], 1)
        self.assertEqual(summary["failed_requests"], 1)
        self.assertEqual(summary["total_chunks"], 4)
        self.assertEqual(summary["chunks_per_second"], 4.0)
        # ideal = 100 eps * 2 workers = 200; efficiency = 4/200
        self.assertEqual(summary["ideal_events_per_second"], 200.0)
        self.assertAlmostEqual(summary["efficiency"], 0.02)
        # ideal stream = (4-1)/100*1000 = 30ms; stretch = 30/30 = 1.0
        self.assertAlmostEqual(summary["p50_stream_stretch"], 1.0)
        self.assertEqual(summary["p95_max_gap_ms"], 12.0)
        self.assertEqual(summary["max_max_gap_ms"], 12.0)

    def test_aggregate_summary_unpaced_has_zero_ideal_and_stretch(self):
        summary = aggregate_summary(
            [measurement()], duration_ms=1000.0,
            expected_chunks=4, events_per_second=0, concurrency=2,
        )
        self.assertEqual(summary["ideal_events_per_second"], 0.0)
        self.assertEqual(summary["efficiency"], 0.0)
        self.assertEqual(summary["p50_stream_stretch"], 0.0)
        self.assertNotIn("per_chunk_overhead_ms", summary)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_metrics -v`
Expected: FAIL — `RequestMeasurement` has no `max_gap_ms`/`stream_ms`; `aggregate_summary` signature mismatch.

- [ ] **Step 3: Replace `bench_harness/metrics.py` entirely**

```python
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
    ideal_events_per_second = float(events_per_second * concurrency)
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_metrics -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add bench_harness/metrics.py tests/test_metrics.py
git commit -m "Add completeness, gap, stretch, and efficiency metrics"
```

### Task 5: Python client — duration loop, pool limits, gap tracking

**Files:**
- Modify: `bench_harness/python_client.py` (full rewrite below)
- Create: `tests/test_python_client.py`

**Interfaces:**
- Consumes: `WorkloadConfig` (Task 3), `RequestMeasurement`/`aggregate_summary` (Task 4), `SseDecoder` (unchanged).
- Produces: `async def run_one_request(client, config, worker_index: int, sequence: int) -> RequestMeasurement`; `async def run_for(client, config, seconds: float) -> tuple[list[RequestMeasurement], float]`; `async def run_benchmark(config, output_dir=None) -> dict`. CLI unchanged (`--config`, `--output-dir`). Task 10 invokes `python3 -m bench_harness.python_client`.

- [ ] **Step 1: Write the failing tests — create `tests/test_python_client.py`**

```python
import asyncio
import unittest

from bench_harness.config import WorkloadConfig
from bench_harness.python_client import run_one_request


def sse(payload: str) -> str:
    return f"data: {payload}\n\n"


def stream_pieces(chunk_count: int, content: str) -> list[str]:
    pieces = [sse('{"choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}')]
    for _ in range(chunk_count):
        pieces.append(sse('{"choices":[{"index":0,"delta":{"content":"%s"},"finish_reason":null}]}' % content))
    pieces.append(sse('{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}'))
    pieces.append(sse("[DONE]"))
    return pieces


class FakeResponse:
    status_code = 200

    def __init__(self, pieces):
        self._pieces = pieces

    async def aiter_text(self):
        for piece in self._pieces:
            yield piece

    async def aread(self):
        return b""


class FakeStream:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *exc):
        return False


class FakeClient:
    def __init__(self, pieces):
        self._pieces = pieces

    def stream(self, method, url, json=None):
        return FakeStream(FakeResponse(self._pieces))


def make_config(chunks=3):
    return WorkloadConfig(
        base_url="http://example", duration_seconds=1.0, warmup_seconds=0.0,
        concurrency=1, chunks_per_response=chunks, chunk_bytes=4,
        ttfc_ms=0, events_per_second=0, output_dir="results",
    )


class RunOneRequestTests(unittest.TestCase):
    def test_counts_content_chunks_and_completes(self):
        pieces = stream_pieces(3, "xxxx")
        m = asyncio.run(run_one_request(FakeClient(pieces), make_config(3), 0, 0))
        self.assertTrue(m.ok)
        self.assertEqual(m.chunks, 3)          # role/finish events not counted
        self.assertEqual(m.bytes, 12)
        self.assertGreater(m.first_chunk_ms, 0.0)
        self.assertGreaterEqual(m.stream_ms, 0.0)
        self.assertGreaterEqual(m.max_gap_ms, 0.0)

    def test_missing_done_marks_not_ok(self):
        pieces = stream_pieces(3, "xxxx")[:-1]
        m = asyncio.run(run_one_request(FakeClient(pieces), make_config(3), 0, 0))
        self.assertFalse(m.ok)
        self.assertEqual(m.chunks, 3)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_python_client -v`
Expected: FAIL — `run_one_request` signature mismatch (`TypeError`) or attribute errors on the measurement.

- [ ] **Step 3: Replace `bench_harness/python_client.py` entirely**

```python
from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bench_harness.config import WorkloadConfig
from bench_harness.metrics import RequestMeasurement, aggregate_summary
from bench_harness.sse import SseDecoder


async def run_one_request(
    client: Any, config: WorkloadConfig, worker_index: int, sequence: int
) -> RequestMeasurement:
    started = time.perf_counter()
    first_event_at: float | None = None
    previous_event_at: float | None = None
    last_event_at: float | None = None
    max_gap_ms = 0.0
    chunks = 0
    content_bytes = 0
    saw_done = False

    def observe_event() -> None:
        nonlocal first_event_at, previous_event_at, last_event_at, max_gap_ms
        now = time.perf_counter()
        if first_event_at is None:
            first_event_at = now
        if previous_event_at is not None:
            max_gap_ms = max(max_gap_ms, (now - previous_event_at) * 1000.0)
        previous_event_at = now
        last_event_at = now

    def measurement(ok: bool) -> RequestMeasurement:
        first_chunk_ms = (
            (first_event_at - started) * 1000.0 if first_event_at is not None else 0.0
        )
        stream_ms = (
            (last_event_at - first_event_at) * 1000.0
            if first_event_at is not None and last_event_at is not None
            else 0.0
        )
        return RequestMeasurement(
            ok=ok,
            latency_ms=(time.perf_counter() - started) * 1000.0,
            first_chunk_ms=first_chunk_ms,
            chunks=chunks,
            bytes=content_bytes,
            max_gap_ms=max_gap_ms,
            stream_ms=stream_ms,
        )

    try:
        payload = config.request_payload(worker_index, sequence, "python")
        async with client.stream("POST", config.endpoint, json=payload) as response:
            if response.status_code != 200:
                await response.aread()
                return measurement(ok=False)

            decoder = SseDecoder()
            async for text in response.aiter_text():
                for event in decoder.feed(text):
                    observe_event()
                    if event == "[DONE]":
                        saw_done = True
                        continue
                    event_payload = json.loads(event)
                    content = event_payload["choices"][0]["delta"].get("content") or ""
                    if content:
                        chunks += 1
                        content_bytes += len(content.encode("utf-8"))
    except Exception:
        return measurement(ok=False)

    return measurement(ok=saw_done)


async def run_for(
    client: Any, config: WorkloadConfig, seconds: float
) -> tuple[list[RequestMeasurement], float]:
    measurements: list[RequestMeasurement] = []
    started = time.perf_counter()
    deadline = started + seconds

    async def worker(worker_index: int) -> None:
        sequence = 0
        while time.perf_counter() < deadline:
            measurements.append(await run_one_request(client, config, worker_index, sequence))
            sequence += 1

    await asyncio.gather(*(worker(index) for index in range(config.concurrency)))
    return measurements, (time.perf_counter() - started) * 1000.0


async def run_benchmark(config: WorkloadConfig, output_dir: Path | None = None) -> dict[str, Any]:
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError(
            "Python client requires httpx. Install project dependencies with `uv sync`."
        ) from exc

    started_at = datetime.now(timezone.utc)
    timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=None)
    # The default pool caps at 100 connections, which would silently serialize
    # higher concurrencies; size the pool to the workload.
    limits = httpx.Limits(
        max_connections=config.concurrency, max_keepalive_connections=config.concurrency
    )

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        if config.warmup_seconds > 0:
            await run_for(client, config, config.warmup_seconds)
        measurements, duration_ms = await run_for(client, config, config.duration_seconds)

    result = {
        "language": "python",
        "implementation": "asyncio-httpx",
        "started_at": started_at.isoformat(),
        "config": config.result_config(),
        "summary": aggregate_summary(
            measurements,
            duration_ms,
            expected_chunks=config.chunks_per_response,
            events_per_second=config.events_per_second,
            concurrency=config.concurrency,
        ),
    }

    destination = output_dir or Path(config.output_dir) / "python"
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


async def async_main() -> None:
    parser = argparse.ArgumentParser(description="Run the Python streaming benchmark client.")
    parser.add_argument("--config", default="config/workload.smoke.json", help="Path to workload JSON.")
    parser.add_argument("--output-dir", default=None, help="Directory for summary.json.")
    args = parser.parse_args()

    config = WorkloadConfig.from_path(args.config)
    result = await run_benchmark(config, Path(args.output_dir) if args.output_dir else None)
    summary = result["summary"]
    print(
        "python "
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
```

- [ ] **Step 4: Run the full Python suite**

Run: `python3 -m unittest discover -s tests -v`
Expected: `test_config`, `test_metrics`, `test_python_client`, `test_sse`, `test_run_smoke` PASS. `test_generate_report` still passes (its fixtures are self-contained; it is updated in Task 12).

- [ ] **Step 5: Commit**

```bash
git add bench_harness/python_client.py tests/test_python_client.py
git commit -m "Make Python client duration-based with sized pool and gap tracking"
```

---

### Task 6: Go client — transport pool, duration loop, new metrics

**Files:**
- Modify: `go-client/main.go`
- Modify: `go-client/main_test.go` (add tests; keep existing decoder/percentile tests)

**Interfaces:**
- Consumes: wire protocol + workload config + summary schema from Shared Contracts.
- Produces: the `bench-go-client` binary (same CLI flags `--config`, `--output-dir`). Task 10 builds it with `go build -o`.

- [ ] **Step 1: Write the failing test — append to `go-client/main_test.go`**

```go
func TestAggregateSummaryClassifiesAndComputesEfficiency(t *testing.T) {
	config := Config{
		Concurrency:       2,
		ChunksPerResponse: 4,
		EventsPerSecond:   100,
		DurationSeconds:   1,
	}
	measurements := []Measurement{
		{OK: true, LatencyMS: 50, FirstChunkMS: 10, Chunks: 4, Bytes: 32, MaxGapMS: 12, StreamMS: 30},
		{OK: true, LatencyMS: 60, FirstChunkMS: 12, Chunks: 3, Bytes: 24, MaxGapMS: 15, StreamMS: 28},
		{OK: false, LatencyMS: 5},
	}

	summary := aggregateSummary(measurements, 1000.0, config)

	if summary.SuccessfulRequests != 1 {
		t.Fatalf("successful = %d, want 1", summary.SuccessfulRequests)
	}
	if summary.IncompleteRequests != 1 {
		t.Fatalf("incomplete = %d, want 1", summary.IncompleteRequests)
	}
	if summary.FailedRequests != 1 {
		t.Fatalf("failed = %d, want 1", summary.FailedRequests)
	}
	// chunks/s = 4; ideal = 100 * 2 = 200; efficiency = 0.02
	if math.Abs(summary.IdealEventsPerSecond-200.0) > 1e-9 {
		t.Fatalf("ideal = %v, want 200", summary.IdealEventsPerSecond)
	}
	if math.Abs(summary.Efficiency-0.02) > 1e-9 {
		t.Fatalf("efficiency = %v, want 0.02", summary.Efficiency)
	}
	// ideal stream = (4-1)/100*1000 = 30ms; stretch = 30/30 = 1.0
	if math.Abs(summary.P50StreamStretch-1.0) > 1e-9 {
		t.Fatalf("p50 stretch = %v, want 1.0", summary.P50StreamStretch)
	}
	if summary.MaxMaxGapMS != 12.0 {
		t.Fatalf("max max gap = %v, want 12 (successful requests only)", summary.MaxMaxGapMS)
	}
}
```

Add `"math"` to the test file's imports.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd go-client && go test ./...`
Expected: compile FAIL — `Config` has no `EventsPerSecond`/`DurationSeconds`, `Measurement` has no `MaxGapMS`/`StreamMS`, `aggregateSummary` signature mismatch.

- [ ] **Step 3: Update `go-client/main.go`**

Replace the `Config`, `Measurement`, and `Summary` types with:

```go
type Config struct {
	BaseURL           string  `json:"base_url"`
	DurationSeconds   float64 `json:"duration_seconds"`
	WarmupSeconds     float64 `json:"warmup_seconds"`
	Concurrency       int     `json:"concurrency"`
	ChunksPerResponse int     `json:"chunks_per_response"`
	ChunkBytes        int     `json:"chunk_bytes"`
	TTFCMS            int     `json:"ttfc_ms"`
	EventsPerSecond   int     `json:"events_per_second"`
	OutputDir         string  `json:"output_dir"`
}

type Measurement struct {
	OK           bool
	LatencyMS    float64
	FirstChunkMS float64
	Chunks       int
	Bytes        int
	MaxGapMS     float64
	StreamMS     float64
}

type Summary struct {
	DurationMS             float64 `json:"duration_ms"`
	SuccessfulRequests     int     `json:"successful_requests"`
	IncompleteRequests     int     `json:"incomplete_requests"`
	FailedRequests         int     `json:"failed_requests"`
	TotalChunks            int     `json:"total_chunks"`
	TotalBytes             int     `json:"total_bytes"`
	RequestsPerSecond      float64 `json:"requests_per_second"`
	ChunksPerSecond        float64 `json:"chunks_per_second"`
	MeanRequestLatencyMS   float64 `json:"mean_request_latency_ms"`
	P50RequestLatencyMS    float64 `json:"p50_request_latency_ms"`
	P95RequestLatencyMS    float64 `json:"p95_request_latency_ms"`
	P99RequestLatencyMS    float64 `json:"p99_request_latency_ms"`
	MeanTimeToFirstChunkMS float64 `json:"mean_time_to_first_chunk_ms"`
	P50TimeToFirstChunkMS  float64 `json:"p50_time_to_first_chunk_ms"`
	P95TimeToFirstChunkMS  float64 `json:"p95_time_to_first_chunk_ms"`
	P99TimeToFirstChunkMS  float64 `json:"p99_time_to_first_chunk_ms"`
	P50MaxGapMS            float64 `json:"p50_max_gap_ms"`
	P95MaxGapMS            float64 `json:"p95_max_gap_ms"`
	P99MaxGapMS            float64 `json:"p99_max_gap_ms"`
	MaxMaxGapMS            float64 `json:"max_max_gap_ms"`
	P50StreamStretch       float64 `json:"p50_stream_stretch"`
	P95StreamStretch       float64 `json:"p95_stream_stretch"`
	P99StreamStretch       float64 `json:"p99_stream_stretch"`
	IdealEventsPerSecond   float64 `json:"ideal_events_per_second"`
	Efficiency             float64 `json:"efficiency"`
}
```

Replace `(config Config) validate()` with:

```go
func (config Config) validate() error {
	if config.DurationSeconds <= 0 {
		return fmt.Errorf("duration_seconds must be > 0")
	}
	if config.WarmupSeconds < 0 {
		return fmt.Errorf("warmup_seconds must be >= 0")
	}
	if config.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}
	if config.ChunksPerResponse <= 0 {
		return fmt.Errorf("chunks_per_response must be > 0")
	}
	if config.ChunkBytes <= 0 {
		return fmt.Errorf("chunk_bytes must be > 0")
	}
	if config.TTFCMS < 0 {
		return fmt.Errorf("ttfc_ms must be >= 0")
	}
	if config.EventsPerSecond < 0 {
		return fmt.Errorf("events_per_second must be >= 0")
	}
	return nil
}
```

Replace `requestPayload` with:

```go
func (config Config) requestPayload(workerIndex int, sequence int, language string) map[string]any {
	return map[string]any{
		"model":             "synthetic",
		"messages":          []map[string]string{{"role": "user", "content": "benchmark"}},
		"stream":            true,
		"chunks":            config.ChunksPerResponse,
		"chunk_bytes":       config.ChunkBytes,
		"ttfc_ms":           config.TTFCMS,
		"events_per_second": config.EventsPerSecond,
		"request_id":        fmt.Sprintf("%s-%d-%d", language, workerIndex, sequence),
	}
}
```

Replace `runOneRequest` and delete `failedMeasurement` with:

```go
func runOneRequest(client *http.Client, config Config, workerIndex int, sequence int) Measurement {
	started := time.Now()
	var firstEventAt, previousEventAt, lastEventAt time.Time
	maxGapMS := 0.0
	chunks := 0
	contentBytes := 0
	sawDone := false

	observe := func() {
		now := time.Now()
		if firstEventAt.IsZero() {
			firstEventAt = now
		}
		if !previousEventAt.IsZero() {
			gap := float64(now.Sub(previousEventAt).Microseconds()) / 1000.0
			if gap > maxGapMS {
				maxGapMS = gap
			}
		}
		previousEventAt = now
		lastEventAt = now
	}

	build := func(ok bool) Measurement {
		firstChunkMS := 0.0
		if !firstEventAt.IsZero() {
			firstChunkMS = float64(firstEventAt.Sub(started).Microseconds()) / 1000.0
		}
		streamMS := 0.0
		if !firstEventAt.IsZero() && !lastEventAt.IsZero() {
			streamMS = float64(lastEventAt.Sub(firstEventAt).Microseconds()) / 1000.0
		}
		return Measurement{
			OK:           ok,
			LatencyMS:    float64(time.Since(started).Microseconds()) / 1000.0,
			FirstChunkMS: firstChunkMS,
			Chunks:       chunks,
			Bytes:        contentBytes,
			MaxGapMS:     maxGapMS,
			StreamMS:     streamMS,
		}
	}

	body, err := json.Marshal(config.requestPayload(workerIndex, sequence, "go"))
	if err != nil {
		return build(false)
	}

	request, err := http.NewRequest(http.MethodPost, config.endpoint(), bytes.NewReader(body))
	if err != nil {
		return build(false)
	}
	request.Header.Set("content-type", "application/json")

	response, err := client.Do(request)
	if err != nil {
		return build(false)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return build(false)
	}

	reader := bufio.NewReader(response.Body)
	decoder := newSSEDecoder()

	for {
		piece, readErr := reader.ReadString('\n')
		if len(piece) > 0 {
			for _, event := range decoder.feed(piece) {
				observe()
				if event == "[DONE]" {
					sawDone = true
					continue
				}

				var payload struct {
					Choices []struct {
						Delta struct {
							Content string `json:"content"`
						} `json:"delta"`
					} `json:"choices"`
				}
				if err := json.Unmarshal([]byte(event), &payload); err != nil {
					return build(false)
				}
				if len(payload.Choices) == 0 {
					return build(false)
				}
				content := payload.Choices[0].Delta.Content
				if content != "" {
					chunks++
					contentBytes += len(content)
				}
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return build(false)
		}
	}

	return build(sawDone)
}
```

Replace `runMany` with `runFor`:

```go
func runFor(config Config, seconds float64, client *http.Client) ([]Measurement, float64) {
	if seconds <= 0 {
		return nil, 0
	}
	started := time.Now()
	deadline := started.Add(time.Duration(seconds * float64(time.Second)))

	var mu sync.Mutex
	measurements := []Measurement{}
	var wg sync.WaitGroup

	for worker := 0; worker < config.Concurrency; worker++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			for sequence := 0; time.Now().Before(deadline); sequence++ {
				measurement := runOneRequest(client, config, workerIndex, sequence)
				mu.Lock()
				measurements = append(measurements, measurement)
				mu.Unlock()
			}
		}(worker)
	}

	wg.Wait()
	return measurements, float64(time.Since(started).Microseconds()) / 1000.0
}
```

Replace `aggregateSummary` with:

```go
func aggregateSummary(measurements []Measurement, durationMS float64, config Config) Summary {
	expected := config.ChunksPerResponse
	latencies := []float64{}
	firstChunks := []float64{}
	maxGaps := []float64{}
	stretches := []float64{}
	successful := 0
	incomplete := 0
	failed := 0
	totalChunks := 0
	totalBytes := 0

	idealStreamMS := 0.0
	if config.EventsPerSecond > 0 && expected > 1 {
		idealStreamMS = float64(expected-1) / float64(config.EventsPerSecond) * 1000.0
	}

	for _, measurement := range measurements {
		if !measurement.OK {
			failed++
			continue
		}
		if measurement.Chunks != expected {
			incomplete++
			continue
		}
		successful++
		latencies = append(latencies, measurement.LatencyMS)
		firstChunks = append(firstChunks, measurement.FirstChunkMS)
		maxGaps = append(maxGaps, measurement.MaxGapMS)
		if idealStreamMS > 0 {
			stretches = append(stretches, measurement.StreamMS/idealStreamMS)
		}
		totalChunks += measurement.Chunks
		totalBytes += measurement.Bytes
	}

	durationSeconds := durationMS / 1000.0
	chunksPerSecond := 0.0
	requestsPerSecond := 0.0
	if durationSeconds > 0 {
		chunksPerSecond = float64(totalChunks) / durationSeconds
		requestsPerSecond = float64(successful) / durationSeconds
	}
	idealEventsPerSecond := float64(config.EventsPerSecond * config.Concurrency)
	efficiency := 0.0
	if idealEventsPerSecond > 0 {
		efficiency = chunksPerSecond / idealEventsPerSecond
	}
	maxMaxGap := 0.0
	for _, gap := range maxGaps {
		if gap > maxMaxGap {
			maxMaxGap = gap
		}
	}

	return Summary{
		DurationMS:             durationMS,
		SuccessfulRequests:     successful,
		IncompleteRequests:     incomplete,
		FailedRequests:         failed,
		TotalChunks:            totalChunks,
		TotalBytes:             totalBytes,
		RequestsPerSecond:      requestsPerSecond,
		ChunksPerSecond:        chunksPerSecond,
		MeanRequestLatencyMS:   mean(latencies),
		P50RequestLatencyMS:    percentile(latencies, 0.50),
		P95RequestLatencyMS:    percentile(latencies, 0.95),
		P99RequestLatencyMS:    percentile(latencies, 0.99),
		MeanTimeToFirstChunkMS: mean(firstChunks),
		P50TimeToFirstChunkMS:  percentile(firstChunks, 0.50),
		P95TimeToFirstChunkMS:  percentile(firstChunks, 0.95),
		P99TimeToFirstChunkMS:  percentile(firstChunks, 0.99),
		P50MaxGapMS:            percentile(maxGaps, 0.50),
		P95MaxGapMS:            percentile(maxGaps, 0.95),
		P99MaxGapMS:            percentile(maxGaps, 0.99),
		MaxMaxGapMS:            maxMaxGap,
		P50StreamStretch:       percentile(stretches, 0.50),
		P95StreamStretch:       percentile(stretches, 0.95),
		P99StreamStretch:       percentile(stretches, 0.99),
		IdealEventsPerSecond:   idealEventsPerSecond,
		Efficiency:             efficiency,
	}
}
```

Replace `runBenchmark` with (note the sized transport — the default transport keeps only 2 idle conns per host, forcing constant re-dials at high concurrency):

```go
func runBenchmark(config Config, outputDir string) (Result, error) {
	transport := &http.Transport{
		MaxIdleConns:        config.Concurrency,
		MaxIdleConnsPerHost: config.Concurrency,
	}
	client := &http.Client{Transport: transport}
	startedAt := time.Now().UTC()

	if config.WarmupSeconds > 0 {
		_, _ = runFor(config, config.WarmupSeconds, client)
	}

	measurements, durationMS := runFor(config, config.DurationSeconds, client)

	result := Result{
		Language:       "go",
		Implementation: "net-http-goroutines",
		StartedAt:      startedAt.Format(time.RFC3339Nano),
		Config:         config,
		Summary:        aggregateSummary(measurements, durationMS, config),
	}

	destination := outputDir
	if destination == "" {
		destination = filepath.Join(config.OutputDir, "go")
	}
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return result, err
	}

	content, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return result, err
	}
	content = append(content, '\n')
	if err := os.WriteFile(filepath.Join(destination, "summary.json"), content, 0o644); err != nil {
		return result, err
	}

	return result, nil
}
```

Update the final `fmt.Printf` in `main` to:

```go
	fmt.Printf(
		"go requests/s=%.2f chunks/s=%.2f efficiency=%.3f failures=%d incomplete=%d\n",
		result.Summary.RequestsPerSecond,
		result.Summary.ChunksPerSecond,
		result.Summary.Efficiency,
		result.Summary.FailedRequests,
		result.Summary.IncompleteRequests,
	)
```

Remove the now-unused `runMany`/`failedMeasurement` and drop `"math"` from main.go imports only if it becomes unused (percentile still uses it — keep).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd go-client && go test ./... && go vet ./...`
Expected: PASS, no vet errors.

- [ ] **Step 5: Commit**

```bash
git add go-client
git commit -m "Make Go client duration-based with sized transport and gap tracking"
```

### Task 7: Rust client — duration workers, timing helper, new metrics

**Files:**
- Modify: `rust-client/src/lib.rs` (large targeted rewrite; code below)
- Modify: `rust-client/src/main.rs` (print line)
- Modify: `rust-client/tests/parser.rs` (add tests; keep decoder/percentile tests)

**Interfaces:**
- Consumes: wire protocol + workload config + summary schema from Shared Contracts.
- Produces: `Config` with fields `base_url: String, duration_seconds: f64, warmup_seconds: f64, concurrency: usize, chunks_per_response: usize, chunk_bytes: usize, ttfc_ms: u64, events_per_second: u64, output_dir: String`; `Measurement { ok, latency_ms, first_chunk_ms, chunks, bytes, max_gap_ms, stream_ms }`; `pub fn aggregate_summary(&[Measurement], duration_ms: f64, config: &Config) -> Summary`; `pub enum AnyClient` + `pub fn build_client(ClientKind) -> Result<AnyClient, _>`; `pub async fn run_for(AnyClient, Config, seconds: f64) -> (Vec<Measurement>, f64)`; `run_benchmark_with_client` keeps its signature. Task 8 adds `ClientKind::Drain`; write `ClientKind` matches so a new variant is a compile error here, not a silent fallthrough.

- [ ] **Step 1: Write the failing tests — append to `rust-client/tests/parser.rs`**

```rust
use rust_client::{aggregate_summary, Config, Measurement};

fn test_config(concurrency: usize, chunks: usize, events_per_second: u64) -> Config {
    Config {
        base_url: "http://127.0.0.1:8080".to_string(),
        duration_seconds: 1.0,
        warmup_seconds: 0.0,
        concurrency,
        chunks_per_response: chunks,
        chunk_bytes: 8,
        ttfc_ms: 0,
        events_per_second,
        output_dir: "results".to_string(),
    }
}

fn measurement(ok: bool, chunks: usize, stream_ms: f64, max_gap_ms: f64) -> Measurement {
    Measurement {
        ok,
        latency_ms: 50.0,
        first_chunk_ms: 10.0,
        chunks,
        bytes: chunks * 8,
        max_gap_ms,
        stream_ms,
    }
}

#[test]
fn aggregate_summary_classifies_and_computes_efficiency() {
    let config = test_config(2, 4, 100);
    let measurements = vec![
        measurement(true, 4, 30.0, 12.0),
        measurement(true, 3, 28.0, 15.0), // incomplete: ok but wrong chunk count
        measurement(false, 0, 0.0, 0.0),
    ];

    let summary = aggregate_summary(&measurements, 1000.0, &config);

    assert_eq!(summary.successful_requests, 1);
    assert_eq!(summary.incomplete_requests, 1);
    assert_eq!(summary.failed_requests, 1);
    assert_eq!(summary.ideal_events_per_second, 200.0);
    assert!((summary.efficiency - 0.02).abs() < 1e-9);
    assert!((summary.p50_stream_stretch - 1.0).abs() < 1e-9);
    assert_eq!(summary.max_max_gap_ms, 12.0);
}

#[test]
fn aggregate_summary_unpaced_has_zero_ideal_and_stretch() {
    let config = test_config(2, 4, 0);
    let summary = aggregate_summary(&[measurement(true, 4, 30.0, 1.0)], 1000.0, &config);
    assert_eq!(summary.ideal_events_per_second, 0.0);
    assert_eq!(summary.efficiency, 0.0);
    assert_eq!(summary.p50_stream_stretch, 0.0);
}
```

(Keep the existing `use rust_client::{percentile, ClientKind, SseDecoder};` import line and existing tests; merge the imports.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --manifest-path rust-client/Cargo.toml`
Expected: compile FAIL — `Config` field mismatch, `Measurement` missing fields, `aggregate_summary` arity.

- [ ] **Step 3: Update `rust-client/src/lib.rs`**

3a. Replace `Config` and its impl:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub base_url: String,
    pub duration_seconds: f64,
    pub warmup_seconds: f64,
    pub concurrency: usize,
    pub chunks_per_response: usize,
    pub chunk_bytes: usize,
    pub ttfc_ms: u64,
    pub events_per_second: u64,
    pub output_dir: String,
}

impl Config {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_json::from_str(&content)?;
        config
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.duration_seconds <= 0.0 {
            return Err("duration_seconds must be > 0".to_string());
        }
        if self.warmup_seconds < 0.0 {
            return Err("warmup_seconds must be >= 0".to_string());
        }
        if self.concurrency == 0 {
            return Err("concurrency must be > 0".to_string());
        }
        if self.chunks_per_response == 0 {
            return Err("chunks_per_response must be > 0".to_string());
        }
        if self.chunk_bytes == 0 {
            return Err("chunk_bytes must be > 0".to_string());
        }
        Ok(())
    }

    pub fn endpoint(&self) -> String {
        format!(
            "{}/v1/chat/completions",
            self.base_url.trim_end_matches('/')
        )
    }

    pub fn request_payload(&self, worker_index: usize, sequence: usize, language: &str) -> Value {
        json!({
            "model": "synthetic",
            "messages": [{"role": "user", "content": "benchmark"}],
            "stream": true,
            "chunks": self.chunks_per_response,
            "chunk_bytes": self.chunk_bytes,
            "ttfc_ms": self.ttfc_ms,
            "events_per_second": self.events_per_second,
            "request_id": format!("{language}-{worker_index}-{sequence}"),
        })
    }
}
```

3b. Replace `Measurement` and `Summary`:

```rust
#[derive(Debug, Clone)]
pub struct Measurement {
    pub ok: bool,
    pub latency_ms: f64,
    pub first_chunk_ms: f64,
    pub chunks: usize,
    pub bytes: usize,
    pub max_gap_ms: f64,
    pub stream_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub duration_ms: f64,
    pub successful_requests: usize,
    pub incomplete_requests: usize,
    pub failed_requests: usize,
    pub total_chunks: usize,
    pub total_bytes: usize,
    pub requests_per_second: f64,
    pub chunks_per_second: f64,
    pub mean_request_latency_ms: f64,
    pub p50_request_latency_ms: f64,
    pub p95_request_latency_ms: f64,
    pub p99_request_latency_ms: f64,
    pub mean_time_to_first_chunk_ms: f64,
    pub p50_time_to_first_chunk_ms: f64,
    pub p95_time_to_first_chunk_ms: f64,
    pub p99_time_to_first_chunk_ms: f64,
    pub p50_max_gap_ms: f64,
    pub p95_max_gap_ms: f64,
    pub p99_max_gap_ms: f64,
    pub max_max_gap_ms: f64,
    pub p50_stream_stretch: f64,
    pub p95_stream_stretch: f64,
    pub p99_stream_stretch: f64,
    pub ideal_events_per_second: f64,
    pub efficiency: f64,
}
```

3c. Add the `StreamTiming` helper (replaces `failed_measurement`, which is deleted):

```rust
#[derive(Debug)]
struct StreamTiming {
    started: Instant,
    first_event_at: Option<Instant>,
    previous_event_at: Option<Instant>,
    last_event_at: Option<Instant>,
    max_gap_ms: f64,
}

impl StreamTiming {
    fn new(started: Instant) -> Self {
        Self {
            started,
            first_event_at: None,
            previous_event_at: None,
            last_event_at: None,
            max_gap_ms: 0.0,
        }
    }

    fn observe_event(&mut self) {
        let now = Instant::now();
        if self.first_event_at.is_none() {
            self.first_event_at = Some(now);
        }
        if let Some(previous) = self.previous_event_at {
            let gap_ms = now.duration_since(previous).as_secs_f64() * 1000.0;
            if gap_ms > self.max_gap_ms {
                self.max_gap_ms = gap_ms;
            }
        }
        self.previous_event_at = Some(now);
        self.last_event_at = Some(now);
    }

    fn measurement(&self, ok: bool, chunks: usize, bytes: usize) -> Measurement {
        let first_chunk_ms = self
            .first_event_at
            .map_or(0.0, |at| at.duration_since(self.started).as_secs_f64() * 1000.0);
        let stream_ms = match (self.first_event_at, self.last_event_at) {
            (Some(first), Some(last)) => last.duration_since(first).as_secs_f64() * 1000.0,
            _ => 0.0,
        };
        Measurement {
            ok,
            latency_ms: self.started.elapsed().as_secs_f64() * 1000.0,
            first_chunk_ms,
            chunks,
            bytes,
            max_gap_ms: self.max_gap_ms,
            stream_ms,
        }
    }
}
```

(`Instant` here is `std::time::Instant`, already imported.)

3d. Rewrite `run_one_reqwest_request` (same shape for hyper below): new signature `(client, config, worker_index, sequence)`, call `timing.observe_event()` on every decoded event, count only non-empty `delta.content`:

```rust
pub async fn run_one_reqwest_request(
    client: &reqwest::Client,
    config: &Config,
    worker_index: usize,
    sequence: usize,
) -> Measurement {
    let started = Instant::now();
    let mut timing = StreamTiming::new(started);
    let response = match client
        .post(config.endpoint())
        .json(&config.request_payload(worker_index, sequence, "rust"))
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return timing.measurement(false, 0, 0),
    };

    if !response.status().is_success() {
        let _ = response.bytes().await;
        return timing.measurement(false, 0, 0);
    }

    let mut stream = response.bytes_stream();
    let mut decoder = SseDecoder::new();
    let mut chunks = 0;
    let mut content_bytes = 0;
    let mut saw_done = false;

    while let Some(next) = stream.next().await {
        let bytes = match next {
            Ok(bytes) => bytes,
            Err(_) => return timing.measurement(false, chunks, content_bytes),
        };
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => return timing.measurement(false, chunks, content_bytes),
        };

        for event in decoder.feed(text) {
            timing.observe_event();
            if event == "[DONE]" {
                saw_done = true;
                continue;
            }
            let payload: ChunkPayload = match serde_json::from_str(&event) {
                Ok(payload) => payload,
                Err(_) => return timing.measurement(false, chunks, content_bytes),
            };
            let Some(choice) = payload.choices.first() else {
                return timing.measurement(false, chunks, content_bytes);
            };
            if !choice.delta.content.is_empty() {
                chunks += 1;
                content_bytes += choice.delta.content.len();
            }
        }
    }

    timing.measurement(saw_done, chunks, content_bytes)
}
```

3e. Rewrite `run_one_hyper_request` and `drain_hyper_body`:

```rust
pub async fn run_one_hyper_request(
    client: &HyperHttpClient,
    config: &Config,
    worker_index: usize,
    sequence: usize,
) -> Measurement {
    let started = Instant::now();
    let timing = StreamTiming::new(started);
    let body = match serde_json::to_vec(&config.request_payload(worker_index, sequence, "rust")) {
        Ok(body) => body,
        Err(_) => return timing.measurement(false, 0, 0),
    };

    let request = match Request::post(config.endpoint())
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
    {
        Ok(request) => request,
        Err(_) => return timing.measurement(false, 0, 0),
    };

    let response = match client.request(request).await {
        Ok(response) => response,
        Err(_) => return timing.measurement(false, 0, 0),
    };

    if !response.status().is_success() {
        return timing.measurement(false, 0, 0);
    }

    drain_hyper_body(timing, response.into_body()).await
}

async fn drain_hyper_body(mut timing: StreamTiming, mut body: Incoming) -> Measurement {
    let mut decoder = SseDecoder::new();
    let mut chunks = 0;
    let mut content_bytes = 0;
    let mut saw_done = false;

    while let Some(next) = body.frame().await {
        let frame = match next {
            Ok(frame) => frame,
            Err(_) => return timing.measurement(false, chunks, content_bytes),
        };
        let Some(data) = frame.data_ref() else {
            continue;
        };
        let text = match std::str::from_utf8(data) {
            Ok(text) => text,
            Err(_) => return timing.measurement(false, chunks, content_bytes),
        };

        for event in decoder.feed(text) {
            timing.observe_event();
            if event == "[DONE]" {
                saw_done = true;
                continue;
            }
            let payload: ChunkPayload = match serde_json::from_str(&event) {
                Ok(payload) => payload,
                Err(_) => return timing.measurement(false, chunks, content_bytes),
            };
            let Some(choice) = payload.choices.first() else {
                return timing.measurement(false, chunks, content_bytes);
            };
            if !choice.delta.content.is_empty() {
                chunks += 1;
                content_bytes += choice.delta.content.len();
            }
        }
    }

    timing.measurement(saw_done, chunks, content_bytes)
}
```

(Note `drain_hyper_body` now takes the `StreamTiming` by value instead of a bare `Instant`.)

3f. Replace `run_reqwest_many`/`run_hyper_many`/`run_measurements` with an `AnyClient` enum and duration loop:

```rust
#[derive(Clone)]
pub enum AnyClient {
    Reqwest(reqwest::Client),
    Hyper(HyperHttpClient),
}

pub fn build_client(kind: ClientKind) -> Result<AnyClient, Box<dyn Error + Send + Sync>> {
    Ok(match kind {
        ClientKind::Reqwest => AnyClient::Reqwest(reqwest::Client::builder().build()?),
        ClientKind::Hyper => {
            AnyClient::Hyper(HyperClient::builder(TokioExecutor::new()).build_http())
        }
    })
}

async fn run_one(
    client: &AnyClient,
    config: &Config,
    worker_index: usize,
    sequence: usize,
) -> Measurement {
    match client {
        AnyClient::Reqwest(inner) => {
            run_one_reqwest_request(inner, config, worker_index, sequence).await
        }
        AnyClient::Hyper(inner) => {
            run_one_hyper_request(inner, config, worker_index, sequence).await
        }
    }
}

pub async fn run_for(client: AnyClient, config: Config, seconds: f64) -> (Vec<Measurement>, f64) {
    let started = Instant::now();
    let deadline = started + Duration::from_secs_f64(seconds);
    let mut handles = Vec::with_capacity(config.concurrency);

    for worker_index in 0..config.concurrency {
        let client = client.clone();
        let config = config.clone();
        handles.push(tokio::spawn(async move {
            let mut measurements = Vec::new();
            let mut sequence = 0usize;
            while Instant::now() < deadline {
                measurements.push(run_one(&client, &config, worker_index, sequence).await);
                sequence += 1;
            }
            measurements
        }));
    }

    let mut all = Vec::new();
    for handle in handles {
        if let Ok(measurements) = handle.await {
            all.extend(measurements);
        }
    }
    (all, started.elapsed().as_secs_f64() * 1000.0)
}
```

Add `use std::time::Duration;` to the imports; `futures_util::stream` import can be trimmed to just `StreamExt`.

3g. Replace `run_benchmark_with_client`:

```rust
pub async fn run_benchmark_with_client(
    config: Config,
    output_dir: Option<PathBuf>,
    client_kind: ClientKind,
) -> Result<ResultEnvelope, Box<dyn Error + Send + Sync>> {
    let started_at = Utc::now().to_rfc3339();
    let client = build_client(client_kind)?;

    if config.warmup_seconds > 0.0 {
        let _ = run_for(client.clone(), config.clone(), config.warmup_seconds).await;
    }
    let (measurements, duration_ms) =
        run_for(client.clone(), config.clone(), config.duration_seconds).await;

    let result = ResultEnvelope {
        language: "rust".to_string(),
        implementation: client_kind.implementation().to_string(),
        started_at,
        config: config.clone(),
        summary: aggregate_summary(&measurements, duration_ms, &config),
    };

    let destination = output_dir
        .unwrap_or_else(|| PathBuf::from(&config.output_dir).join(client_kind.output_name()));
    tokio::fs::create_dir_all(&destination).await?;
    let content = serde_json::to_string_pretty(&result)? + "\n";
    tokio::fs::write(destination.join("summary.json"), content).await?;

    Ok(result)
}
```

3h. Replace `aggregate_summary`:

```rust
pub fn aggregate_summary(measurements: &[Measurement], duration_ms: f64, config: &Config) -> Summary {
    let expected = config.chunks_per_response;
    let mut latencies = Vec::new();
    let mut first_chunks = Vec::new();
    let mut max_gaps = Vec::new();
    let mut stretches = Vec::new();
    let mut successful_requests = 0usize;
    let mut incomplete_requests = 0usize;
    let mut failed_requests = 0usize;
    let mut total_chunks = 0usize;
    let mut total_bytes = 0usize;

    let ideal_stream_ms = if config.events_per_second > 0 && expected > 1 {
        (expected - 1) as f64 / config.events_per_second as f64 * 1000.0
    } else {
        0.0
    };

    for measurement in measurements {
        if !measurement.ok {
            failed_requests += 1;
            continue;
        }
        if measurement.chunks != expected {
            incomplete_requests += 1;
            continue;
        }
        successful_requests += 1;
        latencies.push(measurement.latency_ms);
        first_chunks.push(measurement.first_chunk_ms);
        max_gaps.push(measurement.max_gap_ms);
        if ideal_stream_ms > 0.0 {
            stretches.push(measurement.stream_ms / ideal_stream_ms);
        }
        total_chunks += measurement.chunks;
        total_bytes += measurement.bytes;
    }

    let duration_seconds = duration_ms / 1000.0;
    let (requests_per_second, chunks_per_second) = if duration_seconds > 0.0 {
        (
            successful_requests as f64 / duration_seconds,
            total_chunks as f64 / duration_seconds,
        )
    } else {
        (0.0, 0.0)
    };
    let ideal_events_per_second = (config.events_per_second as usize * config.concurrency) as f64;
    let efficiency = if ideal_events_per_second > 0.0 {
        chunks_per_second / ideal_events_per_second
    } else {
        0.0
    };

    Summary {
        duration_ms,
        successful_requests,
        incomplete_requests,
        failed_requests,
        total_chunks,
        total_bytes,
        requests_per_second,
        chunks_per_second,
        mean_request_latency_ms: mean(&latencies),
        p50_request_latency_ms: percentile(&latencies, 0.50),
        p95_request_latency_ms: percentile(&latencies, 0.95),
        p99_request_latency_ms: percentile(&latencies, 0.99),
        mean_time_to_first_chunk_ms: mean(&first_chunks),
        p50_time_to_first_chunk_ms: percentile(&first_chunks, 0.50),
        p95_time_to_first_chunk_ms: percentile(&first_chunks, 0.95),
        p99_time_to_first_chunk_ms: percentile(&first_chunks, 0.99),
        p50_max_gap_ms: percentile(&max_gaps, 0.50),
        p95_max_gap_ms: percentile(&max_gaps, 0.95),
        p99_max_gap_ms: percentile(&max_gaps, 0.99),
        max_max_gap_ms: max_gaps.iter().copied().fold(0.0, f64::max),
        p50_stream_stretch: percentile(&stretches, 0.50),
        p95_stream_stretch: percentile(&stretches, 0.95),
        p99_stream_stretch: percentile(&stretches, 0.99),
        ideal_events_per_second,
        efficiency,
    }
}
```

3i. In `rust-client/src/main.rs`, update the print line:

```rust
    println!(
        "rust/{} requests/s={:.2} chunks/s={:.2} efficiency={:.3} failures={} incomplete={}",
        result.implementation,
        result.summary.requests_per_second,
        result.summary.chunks_per_second,
        result.summary.efficiency,
        result.summary.failed_requests,
        result.summary.incomplete_requests
    );
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --manifest-path rust-client/Cargo.toml`
Expected: all tests PASS (existing decoder/percentile/ClientKind tests plus the two new aggregate tests).

- [ ] **Step 5: Commit**

```bash
git add rust-client
git commit -m "Make Rust client duration-based with timing helper and new metrics"
```

---

### Task 8: Rust drain-only reference client

**Files:**
- Modify: `rust-client/src/lib.rs` (add `EventBoundaryCounter`, `ClientKind::Drain`, `run_one_drain_request`)
- Modify: `rust-client/tests/parser.rs` (add tests)

**Interfaces:**
- Consumes: Task 7's `Config`, `StreamTiming`, `AnyClient`, `HyperHttpClient`.
- Produces: `pub struct EventBoundaryCounter` with `new()` and `feed(&mut self, bytes: &[u8]) -> usize` (count of `\n\n` boundaries, split-safe); `ClientKind::Drain` parsing from `"drain"`, `implementation() == "drain-hyper"`, `output_name() == "rust-drain"`. The drain client reads raw body bytes, never SSE-parses or JSON-parses; `chunks = total_events - 3` (role + finish + `[DONE]`), `bytes` = wire bytes. Task 9's smoke runner and Task 10's sweep runner invoke it via `--client drain`.

- [ ] **Step 1: Write the failing tests — append to `rust-client/tests/parser.rs`**

```rust
use rust_client::EventBoundaryCounter;

#[test]
fn boundary_counter_handles_splits_across_feeds() {
    let mut counter = EventBoundaryCounter::new();
    assert_eq!(counter.feed(b"data: a\n"), 0);
    assert_eq!(counter.feed(b"\ndata: b\n\n"), 2);
    assert_eq!(counter.feed(b"data: c\n\ndata: d\n\n"), 2);
}

#[test]
fn client_kind_parses_drain() {
    let drain = ClientKind::parse("drain").unwrap();
    assert_eq!(drain.implementation(), "drain-hyper");
    assert_eq!(drain.output_name(), "rust-drain");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --manifest-path rust-client/Cargo.toml`
Expected: compile FAIL — `EventBoundaryCounter` not found; `parse("drain")` returns Err.

- [ ] **Step 3: Implement in `rust-client/src/lib.rs`**

3a. Add the counter:

```rust
/// Counts SSE event boundaries ("\n\n") in a raw byte stream without
/// materializing events. Split-safe across feed() calls.
#[derive(Debug, Default)]
pub struct EventBoundaryCounter {
    pending_newline: bool,
}

impl EventBoundaryCounter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, bytes: &[u8]) -> usize {
        let mut count = 0;
        for &byte in bytes {
            if byte == b'\n' {
                if self.pending_newline {
                    count += 1;
                    self.pending_newline = false;
                } else {
                    self.pending_newline = true;
                }
            } else {
                self.pending_newline = false;
            }
        }
        count
    }
}
```

3b. Extend `ClientKind`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientKind {
    Reqwest,
    Hyper,
    Drain,
}

impl ClientKind {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "reqwest" => Ok(Self::Reqwest),
            "hyper" => Ok(Self::Hyper),
            "drain" => Ok(Self::Drain),
            other => Err(format!(
                "unsupported Rust client {other:?}; expected reqwest, hyper, or drain"
            )),
        }
    }

    pub fn implementation(self) -> &'static str {
        match self {
            Self::Reqwest => "reqwest-tokio",
            Self::Hyper => "hyper-tokio",
            Self::Drain => "drain-hyper",
        }
    }

    pub fn output_name(self) -> &'static str {
        match self {
            Self::Reqwest => "rust-reqwest",
            Self::Hyper => "rust-hyper",
            Self::Drain => "rust-drain",
        }
    }
}
```

3c. Add the drain variant to `AnyClient` and wire it through `build_client`/`run_one`:

```rust
#[derive(Clone)]
pub enum AnyClient {
    Reqwest(reqwest::Client),
    Hyper(HyperHttpClient),
    Drain(HyperHttpClient),
}
```

In `build_client`: `ClientKind::Drain => AnyClient::Drain(HyperClient::builder(TokioExecutor::new()).build_http())`.
In `run_one`: `AnyClient::Drain(inner) => run_one_drain_request(inner, config, worker_index, sequence).await`.

3d. Add the drain request (raw byte scan; a frame carrying ≥1 boundary counts as one timing observation — gaps are frame-granular by design):

```rust
pub async fn run_one_drain_request(
    client: &HyperHttpClient,
    config: &Config,
    worker_index: usize,
    sequence: usize,
) -> Measurement {
    let started = Instant::now();
    let mut timing = StreamTiming::new(started);
    let body = match serde_json::to_vec(&config.request_payload(worker_index, sequence, "rust-drain")) {
        Ok(body) => body,
        Err(_) => return timing.measurement(false, 0, 0),
    };

    let request = match Request::post(config.endpoint())
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
    {
        Ok(request) => request,
        Err(_) => return timing.measurement(false, 0, 0),
    };

    let response = match client.request(request).await {
        Ok(response) => response,
        Err(_) => return timing.measurement(false, 0, 0),
    };
    if !response.status().is_success() {
        return timing.measurement(false, 0, 0);
    }

    let mut incoming = response.into_body();
    let mut counter = EventBoundaryCounter::new();
    let mut total_events = 0usize;
    let mut wire_bytes = 0usize;

    while let Some(next) = incoming.frame().await {
        let frame = match next {
            Ok(frame) => frame,
            Err(_) => return timing.measurement(false, total_events.saturating_sub(3), wire_bytes),
        };
        let Some(data) = frame.data_ref() else {
            continue;
        };
        wire_bytes += data.len();
        let boundaries = counter.feed(data);
        if boundaries > 0 {
            timing.observe_event();
            total_events += boundaries;
        }
    }

    // role + finish + [DONE] are not content chunks.
    timing.measurement(true, total_events.saturating_sub(3), wire_bytes)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --manifest-path rust-client/Cargo.toml`
Expected: PASS, including the existing `client_kind_parses_and_labels_implementations` test (unchanged — `parse("curl")` still errs).

- [ ] **Step 5: End-to-end sanity check against the real server**

```bash
cargo build --release --manifest-path server-rust/Cargo.toml
cargo build --release --manifest-path rust-client/Cargo.toml
server-rust/target/release/synthetic-openai-server --bind 127.0.0.1:8080 &
SERVER_PID=$!
sleep 1
rust-client/target/release/rust-benchmark-client \
  --config config/workload.smoke.json --output-dir /tmp/drain-check --client drain
kill $SERVER_PID
cat /tmp/drain-check/summary.json
```

Expected: `incomplete_requests == 0` and `failed_requests == 0` — the drain client's chunk count must equal `chunks_per_response`, proving the `total_events - 3` accounting matches the real stream.

- [ ] **Step 6: Commit**

```bash
git add rust-client
git commit -m "Add drain-only reference client for server calibration"
```

### Task 9: Smoke runner — include drain client, full end-to-end verification

**Files:**
- Modify: `scripts/run_smoke.py:106` (rust client loop)

**Interfaces:**
- Consumes: all clients from Tasks 5–8, workload configs from Task 3.
- Produces: a green `python3 scripts/run_smoke.py` run covering all five clients. This is the integration gate for Tasks 1–8.

- [ ] **Step 1: Add drain to the rust client loop**

In `scripts/run_smoke.py`, change:

```python
        for client_name in ("reqwest", "hyper"):
```
to:
```python
        for client_name in ("reqwest", "hyper", "drain"):
```

- [ ] **Step 2: Run the Python suite (smoke runner test still passes)**

Run: `python3 -m unittest tests.test_run_smoke -v`
Expected: PASS.

- [ ] **Step 3: Full smoke verification**

Run: `python3 scripts/run_smoke.py --config config/workload.smoke.json`
Expected: server starts; python, go, rust-reqwest, rust-hyper, rust-drain all print a
`… requests/s=… chunks/s=… efficiency=… failures=0 incomplete=0` line; the comparison
table prints. Investigate any `incomplete>0` before proceeding — it means a client's
content-chunk counting disagrees with the server's stream shape.

Also verify pacing visually: with the smoke config (`events_per_second: 2000`, `chunks: 64`, `ttfc_ms: 20`), each request should take ≥ 51ms, so at concurrency 2 a 2s window yields roughly 50–80 requests per client, and `efficiency` should print near 1.0 for the Rust clients.

- [ ] **Step 4: Commit**

```bash
git add scripts/run_smoke.py
git commit -m "Run drain reference client in smoke benchmark"
```

---

### Task 10: Sweep runner — tiers × concurrency × repeats with stop rules

**Files:**
- Create: `scripts/run_sweep.py`
- Create: `config/sweep.default.json`
- Create: `config/sweep.smoke.json`
- Create: `tests/test_run_sweep.py`

**Interfaces:**
- Consumes: server binary (Task 2), client binaries/CLIs (Tasks 5–8), `/stats` + `/stats/reset` (Task 1).
- Produces: results tree `results/<UTC timestamp>/<tier>/c<concurrency>/r<repeat>/<client>/{summary.json, server_stats.json, cpu.json}` plus `results/<ts>/sweep.json` (`{"config", "started_at", "finished_at", "stops": {"<tier>:<client>": {"concurrency", "reason"}}, "cells": [...]}`). Task 11's report reads this tree. Pure functions `build_workload`, `rotated`, `stop_reason` are unit-tested.

- [ ] **Step 1: Write the failing tests — create `tests/test_run_sweep.py`**

```python
import json
import tempfile
import unittest
from pathlib import Path

from scripts.run_sweep import SweepConfig, SweepTier, build_workload, rotated, stop_reason


def sweep_config(**overrides):
    data = {
        "base_url": "http://127.0.0.1:8080",
        "tiers": ({"name": "eps100", "events_per_second": 100, "ttfc_ms": 200},),
        "concurrencies": (1, 4),
        "clients": ("drain", "python"),
        "duration_seconds": 2.0,
        "warmup_seconds": 0.5,
        "repeats": 2,
        "cooldown_seconds": 0.0,
        "chunks_per_response": 64,
        "chunk_bytes": 8,
        "stop_efficiency_below": 0.9,
        "stop_ttfc_excess_p95_ms": 100.0,
        "stop_failure_fraction": 0.001,
        "output_dir": "results",
    }
    data.update(overrides)
    tiers = tuple(SweepTier(**tier) for tier in data.pop("tiers"))
    return SweepConfig(tiers=tiers, **data)


def summary(successful=100, incomplete=0, failed=0, efficiency=0.99, p95_ttfc=210.0):
    return {
        "successful_requests": successful,
        "incomplete_requests": incomplete,
        "failed_requests": failed,
        "efficiency": efficiency,
        "p95_time_to_first_chunk_ms": p95_ttfc,
    }


class SweepConfigTests(unittest.TestCase):
    def test_from_path_loads_tiers_and_lists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sweep.json"
            path.write_text(json.dumps({
                "base_url": "http://127.0.0.1:8080",
                "tiers": [{"name": "max", "events_per_second": 0, "ttfc_ms": 0}],
                "concurrencies": [1, 2],
                "clients": ["python"],
                "duration_seconds": 1.0,
                "warmup_seconds": 0.0,
                "repeats": 1,
                "cooldown_seconds": 0.0,
                "chunks_per_response": 16,
                "chunk_bytes": 8,
                "stop_efficiency_below": 0.9,
                "stop_ttfc_excess_p95_ms": 100.0,
                "stop_failure_fraction": 0.001,
                "output_dir": "results",
            }))
            config = SweepConfig.from_path(path)
        self.assertEqual(config.tiers[0].name, "max")
        self.assertEqual(config.concurrencies, (1, 2))

    def test_build_workload_maps_tier_and_concurrency(self):
        sweep = sweep_config()
        workload = build_workload(sweep, sweep.tiers[0], 4)
        self.assertEqual(workload["concurrency"], 4)
        self.assertEqual(workload["events_per_second"], 100)
        self.assertEqual(workload["ttfc_ms"], 200)
        self.assertEqual(workload["duration_seconds"], 2.0)


class RotationTests(unittest.TestCase):
    def test_rotated_shifts_by_repeat(self):
        clients = ("a", "b", "c")
        self.assertEqual(rotated(clients, 0), ["a", "b", "c"])
        self.assertEqual(rotated(clients, 1), ["b", "c", "a"])
        self.assertEqual(rotated(clients, 3), ["a", "b", "c"])


class StopReasonTests(unittest.TestCase):
    def test_healthy_cell_returns_none(self):
        sweep = sweep_config()
        self.assertIsNone(stop_reason(sweep, sweep.tiers[0], [summary(), summary()]))

    def test_failure_fraction_triggers_stop(self):
        sweep = sweep_config()
        reason = stop_reason(sweep, sweep.tiers[0], [summary(failed=5)])
        self.assertIn("failure fraction", reason)

    def test_low_efficiency_triggers_stop(self):
        sweep = sweep_config()
        reason = stop_reason(sweep, sweep.tiers[0], [summary(efficiency=0.5)])
        self.assertIn("efficiency", reason)

    def test_ttfc_excess_triggers_stop(self):
        sweep = sweep_config()
        reason = stop_reason(sweep, sweep.tiers[0], [summary(p95_ttfc=400.0)])
        self.assertIn("TTFC", reason)

    def test_unpaced_tier_skips_efficiency_and_ttfc_rules(self):
        sweep = sweep_config(tiers=({"name": "max", "events_per_second": 0, "ttfc_ms": 0},))
        self.assertIsNone(
            stop_reason(sweep, sweep.tiers[0], [summary(efficiency=0.0, p95_ttfc=5000.0)])
        )

    def test_no_summaries_triggers_stop(self):
        sweep = sweep_config()
        self.assertIsNotNone(stop_reason(sweep, sweep.tiers[0], []))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_run_sweep -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.run_sweep'`.

- [ ] **Step 3: Create `scripts/run_sweep.py`**

```python
from __future__ import annotations

import argparse
import json
import resource
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class SweepTier:
    name: str
    events_per_second: int
    ttfc_ms: int


@dataclass(frozen=True)
class SweepConfig:
    base_url: str
    tiers: tuple[SweepTier, ...]
    concurrencies: tuple[int, ...]
    clients: tuple[str, ...]
    duration_seconds: float
    warmup_seconds: float
    repeats: int
    cooldown_seconds: float
    chunks_per_response: int
    chunk_bytes: int
    stop_efficiency_below: float
    stop_ttfc_excess_p95_ms: float
    stop_failure_fraction: float
    output_dir: str

    @classmethod
    def from_path(cls, path: str | Path) -> "SweepConfig":
        data = json.loads(Path(path).read_text())
        tiers = tuple(SweepTier(**tier) for tier in data.pop("tiers"))
        concurrencies = tuple(data.pop("concurrencies"))
        clients = tuple(data.pop("clients"))
        return cls(tiers=tiers, concurrencies=concurrencies, clients=clients, **data)

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["tiers"] = [asdict(tier) for tier in self.tiers]
        data["concurrencies"] = list(self.concurrencies)
        data["clients"] = list(self.clients)
        return data


def build_workload(sweep: SweepConfig, tier: SweepTier, concurrency: int) -> dict[str, Any]:
    return {
        "base_url": sweep.base_url,
        "duration_seconds": sweep.duration_seconds,
        "warmup_seconds": sweep.warmup_seconds,
        "concurrency": concurrency,
        "chunks_per_response": sweep.chunks_per_response,
        "chunk_bytes": sweep.chunk_bytes,
        "ttfc_ms": tier.ttfc_ms,
        "events_per_second": tier.events_per_second,
        "output_dir": sweep.output_dir,
    }


def rotated(items: tuple[str, ...], repeat: int) -> list[str]:
    if not items:
        return []
    shift = repeat % len(items)
    return list(items[shift:] + items[:shift])


def stop_reason(sweep: SweepConfig, tier: SweepTier, summaries: list[dict[str, Any]]) -> str | None:
    if not summaries:
        return "client produced no results"
    total = sum(
        s["successful_requests"] + s["incomplete_requests"] + s["failed_requests"]
        for s in summaries
    )
    if total == 0:
        return "no requests completed"
    bad = sum(s["incomplete_requests"] + s["failed_requests"] for s in summaries)
    failure_fraction = bad / total
    if failure_fraction > sweep.stop_failure_fraction:
        return f"failure fraction {failure_fraction:.4f} above {sweep.stop_failure_fraction}"
    if tier.events_per_second > 0:
        mean_efficiency = sum(s["efficiency"] for s in summaries) / len(summaries)
        if mean_efficiency < sweep.stop_efficiency_below:
            return f"efficiency {mean_efficiency:.3f} below {sweep.stop_efficiency_below}"
        mean_excess = (
            sum(s["p95_time_to_first_chunk_ms"] for s in summaries) / len(summaries)
            - tier.ttfc_ms
        )
        if mean_excess > sweep.stop_ttfc_excess_p95_ms:
            return f"p95 TTFC excess {mean_excess:.1f}ms above {sweep.stop_ttfc_excess_p95_ms}ms"
    return None


class CpuSampler:
    """Samples %CPU for named pids via `ps` on a background thread."""

    def __init__(self, pids: dict[str, int], interval_seconds: float = 0.5) -> None:
        self._pids = pids
        self._interval = interval_seconds
        self._samples: dict[str, list[float]] = {name: [] for name in pids}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> "CpuSampler":
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            for name, pid in self._pids.items():
                result = subprocess.run(
                    ["ps", "-p", str(pid), "-o", "%cpu="],
                    capture_output=True,
                    text=True,
                )
                value = result.stdout.strip()
                if result.returncode == 0 and value:
                    try:
                        self._samples[name].append(float(value))
                    except ValueError:
                        pass
            self._stop.wait(self._interval)

    def stop(self) -> dict[str, dict[str, float]]:
        self._stop.set()
        self._thread.join(timeout=5)
        report: dict[str, dict[str, float]] = {}
        for name, values in self._samples.items():
            if values:
                report[name] = {
                    "mean_percent": sum(values) / len(values),
                    "max_percent": max(values),
                    "samples": len(values),
                }
            else:
                report[name] = {"mean_percent": 0.0, "max_percent": 0.0, "samples": 0}
        return report


def raise_file_limit(target: int = 65536) -> None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    desired = target if hard == resource.RLIM_INFINITY else min(target, hard)
    if soft < desired:
        resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))


def http_get_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read())


def http_post(url: str) -> None:
    request = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(request, timeout=5) as response:
        response.read()


def wait_for_health(url: str, timeout_seconds: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=0.5) as response:
                if response.status == 200:
                    return
        except OSError as error:
            last_error = error
        time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy at {url}: {last_error}")


def build_binaries() -> dict[str, Path]:
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(ROOT / "server-rust" / "Cargo.toml")],
        check=True,
    )
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(ROOT / "rust-client" / "Cargo.toml")],
        check=True,
    )
    go_binary = ROOT / "go-client" / "bin" / "bench-go-client"
    go_binary.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["go", "build", "-o", str(go_binary), "."], cwd=ROOT / "go-client", check=True)
    return {
        "server": ROOT / "server-rust" / "target" / "release" / "synthetic-openai-server",
        "rust": ROOT / "rust-client" / "target" / "release" / "rust-benchmark-client",
        "go": go_binary,
    }


def client_command(name: str, binaries: dict[str, Path], config_path: Path, out_dir: Path) -> list[str]:
    if name == "python":
        return [
            sys.executable, "-m", "bench_harness.python_client",
            "--config", str(config_path), "--output-dir", str(out_dir),
        ]
    if name == "go":
        return [str(binaries["go"]), "--config", str(config_path), "--output-dir", str(out_dir)]
    rust_kinds = {"rust-reqwest": "reqwest", "rust-hyper": "hyper", "drain": "drain"}
    if name in rust_kinds:
        return [
            str(binaries["rust"]),
            "--config", str(config_path), "--output-dir", str(out_dir),
            "--client", rust_kinds[name],
        ]
    raise ValueError(f"unknown client {name!r}")


def run_cell_client(
    sweep: SweepConfig,
    binaries: dict[str, Path],
    server_pid: int,
    client_name: str,
    config_path: Path,
    out_dir: Path,
) -> dict[str, Any] | None:
    out_dir.mkdir(parents=True, exist_ok=True)
    http_post(f"{sweep.base_url}/stats/reset")

    command = client_command(client_name, binaries, config_path, out_dir)
    print("+", " ".join(command))
    process = subprocess.Popen(command, cwd=ROOT)
    sampler = CpuSampler({"server": server_pid, "client": process.pid}).start()
    returncode = process.wait()
    cpu = sampler.stop()
    server_stats = http_get_json(f"{sweep.base_url}/stats")

    (out_dir / "server_stats.json").write_text(json.dumps(server_stats, indent=2) + "\n")
    (out_dir / "cpu.json").write_text(json.dumps(cpu, indent=2) + "\n")

    if returncode != 0:
        print(f"warning: {client_name} exited {returncode}", file=sys.stderr)
        return None
    summary_path = out_dir / "summary.json"
    if not summary_path.exists():
        print(f"warning: {client_name} wrote no summary.json", file=sys.stderr)
        return None
    return json.loads(summary_path.read_text())


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a tier x concurrency benchmark sweep.")
    parser.add_argument("--config", default="config/sweep.default.json", help="Path to sweep JSON.")
    parser.add_argument("--bind", default="127.0.0.1:8080", help="Server bind address.")
    parser.add_argument("--results-dir", default="results", help="Root directory for run output.")
    args = parser.parse_args()

    sweep = SweepConfig.from_path(ROOT / args.config)
    raise_file_limit()
    binaries = build_binaries()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = ROOT / args.results_dir / timestamp
    (run_dir / "configs").mkdir(parents=True, exist_ok=True)

    server = subprocess.Popen([str(binaries["server"]), "--bind", args.bind])
    record: dict[str, Any] = {
        "config": sweep.as_dict(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "stops": {},
        "cells": [],
    }
    try:
        wait_for_health(f"{sweep.base_url}/health")

        for tier in sweep.tiers:
            active = list(sweep.clients)
            for concurrency in sweep.concurrencies:
                if not active:
                    break
                workload = build_workload(sweep, tier, concurrency)
                config_path = run_dir / "configs" / f"{tier.name}-c{concurrency}.json"
                config_path.write_text(json.dumps(workload, indent=2) + "\n")

                cell_summaries: dict[str, list[dict[str, Any]]] = {name: [] for name in active}
                for repeat in range(sweep.repeats):
                    for client_name in rotated(tuple(active), repeat):
                        out_dir = run_dir / tier.name / f"c{concurrency}" / f"r{repeat}" / client_name
                        result = run_cell_client(
                            sweep, binaries, server.pid, client_name, config_path, out_dir
                        )
                        record["cells"].append({
                            "tier": tier.name,
                            "concurrency": concurrency,
                            "repeat": repeat,
                            "client": client_name,
                            "ok": result is not None,
                        })
                        if result is not None:
                            cell_summaries[client_name].append(result["summary"])

                for client_name in list(active):
                    reason = stop_reason(sweep, tier, cell_summaries[client_name])
                    if reason:
                        active.remove(client_name)
                        record["stops"][f"{tier.name}:{client_name}"] = {
                            "concurrency": concurrency,
                            "reason": reason,
                        }
                        print(f"stop {tier.name}/{client_name} at c={concurrency}: {reason}")

                if sweep.cooldown_seconds > 0:
                    time.sleep(sweep.cooldown_seconds)
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)

    record["finished_at"] = datetime.now(timezone.utc).isoformat()
    (run_dir / "sweep.json").write_text(json.dumps(record, indent=2) + "\n")
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Create `config/sweep.default.json`**

```json
{
  "base_url": "http://127.0.0.1:8080",
  "tiers": [
    {"name": "eps100", "events_per_second": 100, "ttfc_ms": 200},
    {"name": "eps250", "events_per_second": 250, "ttfc_ms": 200},
    {"name": "eps500", "events_per_second": 500, "ttfc_ms": 200},
    {"name": "eps1000", "events_per_second": 1000, "ttfc_ms": 200},
    {"name": "max", "events_per_second": 0, "ttfc_ms": 0}
  ],
  "concurrencies": [1, 4, 16, 64, 256, 1024],
  "clients": ["drain", "python", "go", "rust-reqwest", "rust-hyper"],
  "duration_seconds": 10.0,
  "warmup_seconds": 2.0,
  "repeats": 2,
  "cooldown_seconds": 5.0,
  "chunks_per_response": 512,
  "chunk_bytes": 8,
  "stop_efficiency_below": 0.9,
  "stop_ttfc_excess_p95_ms": 100.0,
  "stop_failure_fraction": 0.001,
  "output_dir": "results"
}
```

(Worst case ~6h if nothing ever stops; stop rules cut this heavily in practice. Documented in Task 12's README update.)

- [ ] **Step 5: Create `config/sweep.smoke.json`**

```json
{
  "base_url": "http://127.0.0.1:8080",
  "tiers": [
    {"name": "eps2000", "events_per_second": 2000, "ttfc_ms": 20}
  ],
  "concurrencies": [1, 4],
  "clients": ["drain", "python", "go", "rust-reqwest", "rust-hyper"],
  "duration_seconds": 2.0,
  "warmup_seconds": 0.5,
  "repeats": 1,
  "cooldown_seconds": 0.0,
  "chunks_per_response": 64,
  "chunk_bytes": 8,
  "stop_efficiency_below": 0.5,
  "stop_ttfc_excess_p95_ms": 500.0,
  "stop_failure_fraction": 0.05,
  "output_dir": "results"
}
```

- [ ] **Step 6: Run unit tests**

Run: `python3 -m unittest tests.test_run_sweep -v`
Expected: PASS (9 tests).

- [ ] **Step 7: Run the smoke sweep end-to-end**

Run: `python3 scripts/run_sweep.py --config config/sweep.smoke.json`
Expected: binaries build; server starts; 2 concurrencies × 1 repeat × 5 clients run; a
`results/<ts>/` tree appears containing `sweep.json`, `configs/`, and
`eps2000/c1/r0/<client>/{summary.json,server_stats.json,cpu.json}` for each client;
no stops triggered. Spot-check one `server_stats.json` — `events_emitted > 0` and
`slip_p99_ms` small (single-digit ms).

- [ ] **Step 8: Commit**

```bash
git add scripts/run_sweep.py config/sweep.default.json config/sweep.smoke.json tests/test_run_sweep.py
git commit -m "Add concurrency sweep runner with stop rules, CPU and slip capture"
```

### Task 11: Sweep report — efficiency-vs-concurrency per tier

**Files:**
- Create: `scripts/generate_sweep_report.py`
- Create: `tests/test_generate_sweep_report.py`

**Interfaces:**
- Consumes: the Task 10 results tree (`<tier>/c<N>/r<M>/<client>/summary.json`, optional `server_stats.json`/`cpu.json`, optional `sweep.json`).
- Produces: `reports/sweep/index.html` (self-contained HTML+SVG, no JS). Public functions used by tests: `load_cells(run_dir) -> list[dict]`, `aggregate_cells(cells) -> dict[(tier, client, concurrency) -> dict]`, `render_report(run_dir, cells, sweep_meta) -> str`, `write_report(run_dir, output) -> Path`, `find_latest_results_dir(root)`.

Design notes (from the dataviz method): categorical colors follow the validated reference palette in fixed slot order — python `#2a78d6`/dark `#3987e5`, go `#1baf7a`/`#199e70`, rust-reqwest `#eda100`/`#c98500`, rust-hyper `#008300`/`#008300`; drain is a neutral dashed reference line (`#898781`), not a categorical slot. The aqua/yellow slots are sub-3:1 contrast on light surfaces, so every chart ships direct end-labels AND a data table (relief rule). One y-axis per chart; x is concurrency on evenly spaced doubling ticks; 2px lines, 8px markers with native `<title>` tooltips; hairline grid; legend + direct labels; light and dark palettes via CSS custom properties.

- [ ] **Step 1: Write the failing tests — create `tests/test_generate_sweep_report.py`**

```python
import json
import tempfile
import unittest
from pathlib import Path

from scripts.generate_sweep_report import aggregate_cells, load_cells, render_report, write_report


def write_cell(root: Path, tier: str, concurrency: int, repeat: int, client: str,
               efficiency: float, eps: int = 500, ttfc: int = 200) -> None:
    cell = root / tier / f"c{concurrency}" / f"r{repeat}" / client
    cell.mkdir(parents=True, exist_ok=True)
    (cell / "summary.json").write_text(json.dumps({
        "language": "x", "implementation": client, "started_at": "2026-07-03T00:00:00Z",
        "config": {
            "base_url": "http://127.0.0.1:8080", "duration_seconds": 10.0,
            "warmup_seconds": 2.0, "concurrency": concurrency,
            "chunks_per_response": 512, "chunk_bytes": 8,
            "ttfc_ms": ttfc, "events_per_second": eps, "output_dir": "results",
        },
        "summary": {
            "duration_ms": 10000.0, "successful_requests": 100, "incomplete_requests": 0,
            "failed_requests": 0, "total_chunks": 51200, "total_bytes": 409600,
            "requests_per_second": 10.0, "chunks_per_second": eps * concurrency * efficiency,
            "mean_request_latency_ms": 1200.0, "p50_request_latency_ms": 1200.0,
            "p95_request_latency_ms": 1300.0, "p99_request_latency_ms": 1400.0,
            "mean_time_to_first_chunk_ms": 205.0, "p50_time_to_first_chunk_ms": 204.0,
            "p95_time_to_first_chunk_ms": 210.0, "p99_time_to_first_chunk_ms": 220.0,
            "p50_max_gap_ms": 3.0, "p95_max_gap_ms": 5.0, "p99_max_gap_ms": 8.0,
            "max_max_gap_ms": 9.0, "p50_stream_stretch": 1.01, "p95_stream_stretch": 1.02,
            "p99_stream_stretch": 1.05, "ideal_events_per_second": float(eps * concurrency),
            "efficiency": efficiency,
        },
    }))
    (cell / "server_stats.json").write_text(json.dumps({
        "requests_started": 100, "requests_completed": 100, "events_emitted": 51200,
        "slip_p50_ms": 0.1, "slip_p95_ms": 0.5, "slip_p99_ms": 1.0, "slip_max_ms": 2.0,
    }))
    (cell / "cpu.json").write_text(json.dumps({
        "server": {"mean_percent": 40.0, "max_percent": 60.0, "samples": 10},
        "client": {"mean_percent": 80.0, "max_percent": 95.0, "samples": 10},
    }))


class SweepReportTests(unittest.TestCase):
    def test_load_cells_parses_tree_coordinates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_cell(root, "eps500", 4, 0, "python", 0.99)
            write_cell(root, "eps500", 4, 1, "python", 0.97)
            write_cell(root, "eps500", 16, 0, "go", 1.0)

            cells = load_cells(root)

        self.assertEqual(len(cells), 3)
        first = min(cells, key=lambda c: (c["concurrency"], c["repeat"]))
        self.assertEqual(first["tier"], "eps500")
        self.assertEqual(first["concurrency"], 4)
        self.assertEqual(first["client"], "python")
        self.assertIn("slip_p99_ms", first["server_stats"])

    def test_aggregate_cells_averages_repeats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_cell(root, "eps500", 4, 0, "python", 0.99)
            write_cell(root, "eps500", 4, 1, "python", 0.97)

            groups = aggregate_cells(load_cells(root))

        entry = groups[("eps500", "python", 4)]
        self.assertAlmostEqual(entry["efficiency_mean"], 0.98)
        self.assertAlmostEqual(entry["efficiency_min"], 0.97)
        self.assertAlmostEqual(entry["efficiency_max"], 0.99)
        self.assertEqual(entry["repeats"], 2)

    def test_render_and_write_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "run"
            write_cell(root, "eps500", 4, 0, "python", 0.99)
            write_cell(root, "eps500", 4, 0, "drain", 1.0)
            write_cell(root, "max", 4, 0, "go", 0.0, eps=0, ttfc=0)
            (root / "sweep.json").write_text(json.dumps({
                "stops": {"eps500:python": {"concurrency": 4, "reason": "efficiency 0.5 below 0.9"}},
            }))

            output = Path(tmpdir) / "report" / "index.html"
            written = write_report(root, output)
            html = written.read_text()

        self.assertIn("<svg", html)
        self.assertIn("python", html)
        self.assertIn("drain", html)
        self.assertIn("Delivery efficiency", html)
        self.assertIn("Observed events/sec", html)   # max tier fallback chart
        self.assertIn("efficiency 0.5 below 0.9", html)  # knee table
        self.assertIn("<table", html)                # relief rule: table view


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_generate_sweep_report -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.generate_sweep_report'`.

- [ ] **Step 3: Create `scripts/generate_sweep_report.py`**

```python
from __future__ import annotations

import argparse
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RUN_DIR_RE = re.compile(r"^\d{8}T\d{6}Z$")

# Reference dataviz palette, fixed slot order (validated for CVD separation).
# Drain is the neutral reference, not a categorical slot.
CLIENT_STYLE = {
    "python": {"light": "#2a78d6", "dark": "#3987e5", "dash": ""},
    "go": {"light": "#1baf7a", "dark": "#199e70", "dash": ""},
    "rust-reqwest": {"light": "#eda100", "dark": "#c98500", "dash": ""},
    "rust-hyper": {"light": "#008300", "dark": "#008300", "dash": ""},
    "drain": {"light": "#898781", "dark": "#898781", "dash": "6 4"},
}
CLIENT_ORDER = ["drain", "python", "go", "rust-reqwest", "rust-hyper"]


def find_latest_results_dir(root: Path) -> Path:
    candidates = [p for p in root.iterdir() if p.is_dir() and RUN_DIR_RE.match(p.name)]
    if not candidates:
        raise FileNotFoundError(f"No timestamped result directories found under {root}")
    return sorted(candidates, key=lambda p: p.name)[-1]


def load_json_if_exists(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def load_cells(run_dir: Path) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for summary_path in sorted(run_dir.glob("*/c*/r*/*/summary.json")):
        client_dir = summary_path.parent
        repeat_dir = client_dir.parent
        concurrency_dir = repeat_dir.parent
        tier_dir = concurrency_dir.parent
        data = json.loads(summary_path.read_text())
        cells.append({
            "tier": tier_dir.name,
            "concurrency": int(concurrency_dir.name[1:]),
            "repeat": int(repeat_dir.name[1:]),
            "client": client_dir.name,
            "config": data.get("config", {}),
            "summary": data["summary"],
            "server_stats": load_json_if_exists(client_dir / "server_stats.json"),
            "cpu": load_json_if_exists(client_dir / "cpu.json"),
        })
    return cells


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def aggregate_cells(cells: list[dict[str, Any]]) -> dict[tuple[str, str, int], dict[str, Any]]:
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for cell in cells:
        grouped.setdefault((cell["tier"], cell["client"], cell["concurrency"]), []).append(cell)

    aggregates: dict[tuple[str, str, int], dict[str, Any]] = {}
    for key, group in grouped.items():
        efficiencies = [c["summary"]["efficiency"] for c in group]
        config = group[0]["config"]
        ttfc_ms = float(config.get("ttfc_ms", 0))
        aggregates[key] = {
            "repeats": len(group),
            "efficiency_mean": mean(efficiencies),
            "efficiency_min": min(efficiencies),
            "efficiency_max": max(efficiencies),
            "chunks_per_second_mean": mean([c["summary"]["chunks_per_second"] for c in group]),
            "ttfc_excess_p95_mean": mean(
                [c["summary"]["p95_time_to_first_chunk_ms"] - ttfc_ms for c in group]
            ),
            "stretch_p95_mean": mean([c["summary"]["p95_stream_stretch"] for c in group]),
            "max_gap_p99_mean": mean([c["summary"]["p99_max_gap_ms"] for c in group]),
            "failed": sum(c["summary"]["failed_requests"] for c in group),
            "incomplete": sum(c["summary"]["incomplete_requests"] for c in group),
            "server_slip_p99_max": max(
                (c["server_stats"].get("slip_p99_ms", 0.0) for c in group), default=0.0
            ),
            "client_cpu_mean": mean(
                [c["cpu"].get("client", {}).get("mean_percent", 0.0) for c in group]
            ),
            "server_cpu_mean": mean(
                [c["cpu"].get("server", {}).get("mean_percent", 0.0) for c in group]
            ),
            "events_per_second": int(config.get("events_per_second", 0)),
            "ttfc_ms": ttfc_ms,
        }
    return aggregates


def escape(value: str) -> str:
    return html.escape(str(value), quote=True)


def format_number(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def clients_in(aggregates: dict, tier: str) -> list[str]:
    present = {client for (t, client, _) in aggregates if t == tier}
    ordered = [c for c in CLIENT_ORDER if c in present]
    return ordered + sorted(present - set(ordered))


def concurrencies_in(aggregates: dict, tier: str) -> list[int]:
    return sorted({c for (t, _, c) in aggregates if t == tier})


def line_chart(
    title: str,
    y_label: str,
    concurrencies: list[int],
    series: list[dict[str, Any]],
    y_max: float,
    reference_y: float | None = None,
    value_digits: int = 2,
) -> str:
    """One SVG line chart. series = [{name, points: {concurrency: (mean, lo, hi)}}]."""
    width, height = 860, 360
    left, right, top, bottom = 64, 150, 48, 40
    plot_w, plot_h = width - left - right, height - top - bottom

    def x_for(concurrency: int) -> float:
        index = concurrencies.index(concurrency)
        if len(concurrencies) == 1:
            return left + plot_w / 2
        return left + index * plot_w / (len(concurrencies) - 1)

    def y_for(value: float) -> float:
        clamped = min(max(value, 0.0), y_max)
        return top + plot_h - (clamped / y_max) * plot_h if y_max > 0 else top + plot_h

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">',
        f'<text x="0" y="20" class="svg-title">{escape(title)}</text>',
        f'<text x="0" y="38" class="svg-note">{escape(y_label)} vs concurrency</text>',
    ]
    for tick_index in range(5):
        tick_value = y_max * tick_index / 4
        y = y_for(tick_value)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="grid"></line>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" class="tick tick-y">{format_number(tick_value, value_digits)}</text>')
    for concurrency in concurrencies:
        x = x_for(concurrency)
        parts.append(f'<text x="{x:.1f}" y="{height - 16}" class="tick tick-x">{concurrency}</text>')
    if reference_y is not None and reference_y <= y_max:
        y = y_for(reference_y)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="reference"></line>')
        parts.append(f'<text x="{left + plot_w + 6}" y="{y + 4:.1f}" class="svg-note">ideal</text>')

    for entry in series:
        name = entry["name"]
        style = CLIENT_STYLE.get(name, {"dash": ""})
        dash = f' stroke-dasharray="{style["dash"]}"' if style.get("dash") else ""
        points = [
            (concurrency, entry["points"][concurrency])
            for concurrency in concurrencies
            if concurrency in entry["points"]
        ]
        if not points:
            continue
        coords = " ".join(f"{x_for(c):.1f},{y_for(v[0]):.1f}" for c, v in points)
        parts.append(f'<polyline points="{coords}" class="line series-{escape(name)}"{dash}></polyline>')
        for concurrency, (value, low, high) in points:
            x = x_for(concurrency)
            if high > low:
                parts.append(
                    f'<line x1="{x:.1f}" y1="{y_for(low):.1f}" x2="{x:.1f}" y2="{y_for(high):.1f}" '
                    f'class="whisker series-{escape(name)}"></line>'
                )
            parts.append(
                f'<circle cx="{x:.1f}" cy="{y_for(value):.1f}" r="4" class="marker series-{escape(name)}">'
                f'<title>{escape(name)} · c={concurrency} · {format_number(value, value_digits)}</title></circle>'
            )
        last_concurrency, (last_value, _, _) = points[-1]
        parts.append(
            f'<text x="{x_for(last_concurrency) + 10:.1f}" y="{y_for(last_value) + 4:.1f}" '
            f'class="direct-label">{escape(name)}</text>'
        )

    parts.append("</svg>")
    return f'<article class="chart-card">{"".join(parts)}</article>'


def tier_series(aggregates: dict, tier: str, metric: str) -> list[dict[str, Any]]:
    series = []
    for client in clients_in(aggregates, tier):
        points: dict[int, tuple[float, float, float]] = {}
        for concurrency in concurrencies_in(aggregates, tier):
            entry = aggregates.get((tier, client, concurrency))
            if entry is None:
                continue
            if metric == "efficiency":
                points[concurrency] = (
                    entry["efficiency_mean"], entry["efficiency_min"], entry["efficiency_max"],
                )
            elif metric == "chunks_per_second":
                value = entry["chunks_per_second_mean"]
                points[concurrency] = (value, value, value)
            else:  # ttfc_excess
                value = entry["ttfc_excess_p95_mean"]
                points[concurrency] = (value, value, value)
        series.append({"name": client, "points": points})
    return series


def render_tier_section(aggregates: dict, tier: str) -> str:
    concurrencies = concurrencies_in(aggregates, tier)
    sample = next(v for (t, _, _), v in aggregates.items() if t == tier)
    paced = sample["events_per_second"] > 0

    charts = []
    if paced:
        charts.append(line_chart(
            f"Delivery efficiency — {tier}", "observed / ideal events per second",
            concurrencies, tier_series(aggregates, tier, "efficiency"),
            y_max=1.1, reference_y=1.0,
        ))
        ttfc_values = [
            entry["ttfc_excess_p95_mean"]
            for (t, _, _), entry in aggregates.items() if t == tier
        ]
        ttfc_max = max(max(ttfc_values, default=0.0) * 1.15, 1.0)
        charts.append(line_chart(
            f"p95 TTFC excess — {tier}", "p95 time-to-first-chunk minus configured TTFC (ms)",
            concurrencies, tier_series(aggregates, tier, "ttfc_excess"),
            y_max=ttfc_max, value_digits=1,
        ))
    else:
        throughput = [
            entry["chunks_per_second_mean"]
            for (t, _, _), entry in aggregates.items() if t == tier
        ]
        charts.append(line_chart(
            f"Observed events/sec — {tier}", "mean parsed content events per second",
            concurrencies, tier_series(aggregates, tier, "chunks_per_second"),
            y_max=max(max(throughput, default=0.0) * 1.15, 1.0), value_digits=0,
        ))

    rows = []
    for client in clients_in(aggregates, tier):
        for concurrency in concurrencies:
            entry = aggregates.get((tier, client, concurrency))
            if entry is None:
                continue
            rows.append(
                "<tr>"
                f"<th>{escape(client)}</th>"
                f"<td>{concurrency}</td>"
                f"<td>{format_number(entry['efficiency_mean'], 3) if paced else 'n/a'}</td>"
                f"<td>{format_number(entry['chunks_per_second_mean'], 0)}</td>"
                f"<td>{format_number(entry['ttfc_excess_p95_mean'], 1) if paced else 'n/a'}</td>"
                f"<td>{format_number(entry['stretch_p95_mean'], 3) if paced else 'n/a'}</td>"
                f"<td>{format_number(entry['max_gap_p99_mean'], 1)}</td>"
                f"<td>{entry['failed']}</td>"
                f"<td>{entry['incomplete']}</td>"
                f"<td>{format_number(entry['server_slip_p99_max'], 2)}</td>"
                f"<td>{format_number(entry['client_cpu_mean'], 0)}%</td>"
                f"<td>{format_number(entry['server_cpu_mean'], 0)}%</td>"
                "</tr>"
            )
    headers = (
        "Client", "Concurrency", "Efficiency", "Events/s", "p95 TTFC excess ms",
        "p95 stretch", "p99 max gap ms", "Failed", "Incomplete",
        "Server slip p99 ms", "Client CPU", "Server CPU",
    )
    header_html = "".join(f"<th>{escape(h)}</th>" for h in headers)
    table = (
        f'<div class="table-wrap"><table><thead><tr>{header_html}</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table></div>'
    )
    legend = "".join(
        f'<span class="legend-item"><span class="legend-swatch series-{escape(c)}"></span>{escape(c)}</span>'
        for c in clients_in(aggregates, tier)
    )
    return (
        f'<section><h2>Tier: {escape(tier)}</h2>'
        f'<div class="legend">{legend}</div>'
        f'<div class="chart-grid">{"".join(charts)}</div>{table}</section>'
    )


def render_stops(sweep_meta: dict[str, Any]) -> str:
    stops = sweep_meta.get("stops", {})
    if not stops:
        return "<p>No stop rules triggered.</p>"
    rows = []
    for key, info in sorted(stops.items()):
        tier, _, client = key.partition(":")
        rows.append(
            f"<tr><th>{escape(tier)}</th><td>{escape(client)}</td>"
            f"<td>{info.get('concurrency', '')}</td><td>{escape(info.get('reason', ''))}</td></tr>"
        )
    return (
        '<div class="table-wrap"><table>'
        "<thead><tr><th>Tier</th><th>Client</th><th>Knee concurrency</th><th>Reason</th></tr></thead>"
        f'<tbody>{"".join(rows)}</tbody></table></div>'
    )


def css() -> str:
    light = "".join(
        f"--series-{name}: {style['light']};" for name, style in CLIENT_STYLE.items()
    )
    dark = "".join(
        f"--series-{name}: {style['dark']};" for name, style in CLIENT_STYLE.items()
    )
    series_rules = "".join(
        f".series-{name} {{ stroke: var(--series-{name}); }}"
        f".legend-swatch.series-{name} {{ background: var(--series-{name}); }}"
        for name in CLIENT_STYLE
    )
    return f"""
:root {{
  color-scheme: light dark;
  --surface-1: #fcfcfb; --page: #f9f9f7; --ink: #0b0b0b; --ink-2: #52514e;
  --muted: #898781; --grid: #e1e0d9; --border: rgba(11,11,11,0.10);
  {light}
}}
@media (prefers-color-scheme: dark) {{
  :root {{
    --surface-1: #1a1a19; --page: #0d0d0d; --ink: #ffffff; --ink-2: #c3c2b7;
    --grid: #2c2c2a; --border: rgba(255,255,255,0.10);
    {dark}
  }}
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--page); color: var(--ink);
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }}
main {{ width: min(1180px, calc(100vw - 40px)); margin: 0 auto; padding: 32px 0 56px; }}
h1 {{ font-size: 30px; margin: 6px 0 4px; }}
h2 {{ font-size: 20px; margin: 0 0 12px; }}
p {{ color: var(--ink-2); line-height: 1.55; }}
section {{ margin-top: 22px; padding: 20px; border: 1px solid var(--border);
  border-radius: 8px; background: var(--surface-1); }}
.eyebrow {{ color: var(--muted); font-size: 13px; font-weight: 700;
  text-transform: uppercase; letter-spacing: .04em; }}
.chart-grid {{ display: grid; grid-template-columns: 1fr; gap: 16px; margin-bottom: 16px; }}
.chart-card svg {{ width: 100%; height: auto; display: block; }}
.svg-title {{ font: 700 16px system-ui, sans-serif; fill: var(--ink); }}
.svg-note {{ font: 12px system-ui, sans-serif; fill: var(--muted); }}
.tick {{ font: 11px system-ui, sans-serif; fill: var(--muted);
  font-variant-numeric: tabular-nums; }}
.tick-y {{ text-anchor: end; }}
.tick-x {{ text-anchor: middle; }}
.grid {{ stroke: var(--grid); stroke-width: 1; }}
.reference {{ stroke: var(--muted); stroke-width: 1; stroke-dasharray: 2 3; }}
.line {{ fill: none; stroke-width: 2; }}
.whisker {{ stroke-width: 1.5; opacity: .6; }}
.marker {{ fill: var(--surface-1); stroke-width: 2; }}
.direct-label {{ font: 12px system-ui, sans-serif; fill: var(--ink-2); }}
.legend {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 10px; }}
.legend-item {{ display: inline-flex; align-items: center; gap: 6px;
  font-size: 13px; color: var(--ink-2); }}
.legend-swatch {{ width: 14px; height: 3px; border-radius: 2px; display: inline-block; }}
.table-wrap {{ overflow-x: auto; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ border-bottom: 1px solid var(--grid); padding: 8px;
  text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }}
th:first-child, td:first-child {{ text-align: left; }}
thead th {{ color: var(--ink-2); font-size: 12px; text-transform: uppercase;
  letter-spacing: .04em; }}
{series_rules}
"""


def render_report(run_dir: Path, cells: list[dict[str, Any]], sweep_meta: dict[str, Any]) -> str:
    if not cells:
        raise ValueError("Cannot render sweep report without cells")
    aggregates = aggregate_cells(cells)
    tiers = sorted({tier for (tier, _, _) in aggregates})
    # Paced tiers ordered by rate, then the unpaced tier(s) last.
    def tier_key(tier: str) -> tuple[int, int, str]:
        sample = next(v for (t, _, _), v in aggregates.items() if t == tier)
        eps = sample["events_per_second"]
        return (1, 0, tier) if eps == 0 else (0, eps, tier)
    tiers.sort(key=tier_key)

    sections = "".join(render_tier_section(aggregates, tier) for tier in tiers)
    generated_at = datetime.now(timezone.utc).isoformat()
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Streaming Client Sweep Report</title>
<style>{css()}</style>
</head>
<body>
<main>
<header>
  <p class="eyebrow">Concurrency sweep — synthetic OpenAI-style streaming</p>
  <h1>Streaming Client Sweep Report</h1>
  <p>Run: {escape(str(run_dir))} · Generated: {escape(generated_at)}</p>
  <p>Efficiency = observed parsed events/sec ÷ (events_per_second × concurrency).
  The drain client is a parse-free reference: any gap it shows is server/OS,
  any gap below it is client overhead.</p>
</header>
<section><h2>Stop rules triggered (knees)</h2>{render_stops(sweep_meta)}</section>
{sections}
</main>
</body>
</html>
"""


def write_report(run_dir: Path, output: Path) -> Path:
    cells = load_cells(run_dir)
    if not cells:
        raise FileNotFoundError(f"No cell summary.json files found under {run_dir}")
    sweep_meta = load_json_if_exists(run_dir / "sweep.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_report(run_dir, cells, sweep_meta))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a static HTML sweep report.")
    parser.add_argument("results_dir", nargs="?", default=None,
                        help="Sweep run directory. Defaults to newest under results/.")
    parser.add_argument("--output", default="reports/sweep/index.html", help="Output HTML path.")
    args = parser.parse_args()

    results_dir = Path(args.results_dir) if args.results_dir else find_latest_results_dir(Path("results"))
    output = write_report(results_dir, Path(args.output))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_generate_sweep_report -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Render a real report and eyeball it**

Using the smoke-sweep run from Task 10:

```bash
python3 scripts/generate_sweep_report.py
open reports/sweep/index.html
```

Check: lines are distinguishable, drain renders dashed gray, direct labels don't collide at the right edge, the table scrolls horizontally rather than overflowing, dark mode (toggle macOS appearance or DevTools emulation) keeps text legible.

- [ ] **Step 6: Commit**

```bash
git add scripts/generate_sweep_report.py tests/test_generate_sweep_report.py
git commit -m "Add efficiency-vs-concurrency sweep report generator"
```

### Task 12: Cleanup — single-run report, compare table, README, Makefile

**Files:**
- Modify: `scripts/generate_report.py`
- Modify: `tests/test_generate_report.py`
- Modify: `scripts/compare_results.py`
- Modify: `README.md`
- Modify: `Makefile`

**Interfaces:**
- Consumes: the new summary schema (Shared Contracts) and workload config fields (Task 3).
- Produces: single-run report and compare table aligned with the new schema; docs describing the sweep workflow.

- [ ] **Step 1: Update `tests/test_generate_report.py` fixtures (failing first)**

In both `write_summary` and `make_summary`, replace the `config` dict with:

```python
                "config": {
                    "base_url": "http://127.0.0.1:8080",
                    "duration_seconds": 2.0,
                    "warmup_seconds": 0.5,
                    "concurrency": 2,
                    "chunks_per_response": 3,
                    "chunk_bytes": 8,
                    "ttfc_ms": 200,
                    "events_per_second": 500,
                    "output_dir": "results",
                },
```

and in the `summary` dict: add `"incomplete_requests": 0,` after `"failed_requests": 0,`; replace `"per_chunk_overhead_ms": 0.1,` with:

```python
                    "p50_max_gap_ms": 1.0,
                    "p95_max_gap_ms": 2.0,
                    "p99_max_gap_ms": 3.0,
                    "max_max_gap_ms": 4.0,
                    "p50_stream_stretch": 1.0,
                    "p95_stream_stretch": 1.1,
                    "p99_stream_stretch": 1.2,
                    "ideal_events_per_second": 1000.0,
                    "efficiency": 0.95,
```

In `test_render_report_contains_charts_and_scaling_explanations`, delete the two assertions on hardcoded scaling prose (`"Go net/http + goroutines"` and `"Rust Hyper + Tokio"`), keep the rest, and add:

```python
        self.assertIn("Efficiency", html)
        self.assertNotIn("How Each Client Scales", html)
```

Run: `python3 -m unittest tests.test_generate_report -v` — expected: FAIL (report still renders scaling notes and per-chunk column; workload grid shows n/a).

- [ ] **Step 2: Update `scripts/generate_report.py`**

1. In `render_workload`, replace the `items` list with:

```python
    items = [
        ("duration s", config.get("duration_seconds", "n/a")),
        ("warmup s", config.get("warmup_seconds", "n/a")),
        ("concurrency", config.get("concurrency", "n/a")),
        ("chunks/response", config.get("chunks_per_response", "n/a")),
        ("chunk bytes", config.get("chunk_bytes", "n/a")),
        ("ttfc ms", config.get("ttfc_ms", "n/a")),
        ("events/s/request", config.get("events_per_second", "n/a")),
    ]
```

and change `.workload-grid { grid-template-columns: repeat(6, minmax(0, 1fr)); }` to `repeat(7, …)` in `css()`.

2. In `render_table`, replace the `"Per-chunk ms"` header with `"Efficiency"` and `"Failures"` with `"Failures"`, `"Incomplete"` (add a column); replace the per-chunk cell with:

```python
            f"<td>{escape(format_number(summary.get('efficiency', 0.0), digits=3))}</td>"
```

and after the failures cell add:

```python
            f"<td>{escape(str(summary.get('incomplete_requests', 0)))}</td>"
```

3. Delete `render_scaling_notes()` entirely and remove its `<section>` block (the `How Each Client Scales` section) from `render_report`. Remove the now-unused `.notes-grid` CSS rules.

4. In `render_caveats`, replace the first caveat with:

```python
        "This is a localhost synthetic benchmark, not a real LLM provider benchmark; clients are minimal hand-rolled loops, not official SDKs.",
        "HTTP/1.1 cleartext only — no TLS or HTTP/2, unlike production providers.",
```

(keep the remaining caveats).

Run: `python3 -m unittest tests.test_generate_report -v` — expected: PASS.

- [ ] **Step 3: Update `scripts/compare_results.py`**

In `print_table`, extend each row and the headers:

```python
        rows.append(
            [
                item["language"],
                item.get("implementation", ""),
                format_number(summary["requests_per_second"]),
                format_number(summary["chunks_per_second"]),
                format_number(summary.get("efficiency", 0.0)),
                format_number(summary["p95_request_latency_ms"]),
                format_number(summary["failed_requests"]),
                format_number(summary.get("incomplete_requests", 0)),
                item["_path"],
            ]
        )

    headers = ["language", "implementation", "req/s", "chunks/s", "efficiency", "p95 req ms", "failures", "incomplete", "file"]
```

Run: `python3 -m unittest discover -s tests -v` — expected: full suite PASS.

- [ ] **Step 4: Update `Makefile`**

Append:

```makefile
sweep:
	python3 scripts/run_sweep.py --config config/sweep.default.json

sweep-smoke:
	python3 scripts/run_sweep.py --config config/sweep.smoke.json

sweep-report:
	python3 scripts/generate_sweep_report.py
```

and add `sweep sweep-smoke sweep-report` to the `.PHONY` line.

- [ ] **Step 5: Update `README.md`**

- Remove the stale paragraph "At implementation time in this environment, `cargo`, `rustc`, and `go` were not available…".
- Replace the "Workloads" section's field listing with the new workload JSON (Shared Contracts) and document: `events_per_second: 0` = max speed; `ttfc_ms` = server delay before the first event; streams are role chunk + N content chunks + finish chunk + `[DONE]`.
- Replace the "Result Shape" example's summary keys with the new schema and note `efficiency`/`incomplete_requests`/`stream_stretch` semantics.
- Add a "Concurrency Sweep" section:

```markdown
## Concurrency Sweep

Run the full tier × concurrency sweep (builds all binaries, starts the server,
runs every client per cell, records server schedule-slip stats and CPU):

​```bash
make sweep          # full sweep, hours — tune config/sweep.default.json
make sweep-smoke    # 2-minute end-to-end sanity sweep
make sweep-report   # writes reports/sweep/index.html from the newest run
​```

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
​```
```

(Remove the stray backtick escapes when writing the actual file.)

- [ ] **Step 6: Final verification**

```bash
python3 -m unittest discover -s tests -v
cd go-client && go test ./... && cd ..
cargo test --manifest-path server-rust/Cargo.toml
cargo test --manifest-path rust-client/Cargo.toml
python3 scripts/run_smoke.py --config config/workload.smoke.json
python3 scripts/generate_report.py && open reports/latest/index.html
```

Expected: all suites PASS; smoke run green with 0 failures/incomplete for all five clients; single-run report renders with efficiency column and no scaling-notes section.

- [ ] **Step 7: Commit**

```bash
git add scripts/generate_report.py tests/test_generate_report.py scripts/compare_results.py README.md Makefile
git commit -m "Align single-run report, compare table, and docs with paced schema"
```

---

## Execution Notes

- Tasks 1→2 (server), 3→4→5 (Python), 6 (Go), 7→8 (Rust) have hard internal ordering but the language tracks are independent of each other after Task 2. Tasks 9→12 are sequential and depend on everything prior.
- Between Tasks 3 and 5 the Python client is temporarily broken against the new config — that is expected; run only the named test modules per step.
- Timing-sensitive tests (`paced_stream_takes_at_least_the_scheduled_duration`, smoke efficiency ≈ 1.0) can flake under heavy machine load; rerun once before investigating.
- The sweep default config is sized for an M-series MacBook with 64GB RAM. Full sweeps are long; `make sweep-smoke` is the routine gate.






