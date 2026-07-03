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
