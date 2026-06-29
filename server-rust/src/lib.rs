use std::{convert::Infallible, time::Duration};

use async_stream::stream;
use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::sleep;

const DEFAULT_CHUNKS: usize = 64;
const DEFAULT_CHUNK_BYTES: usize = 32;
const MAX_CHUNKS: usize = 1_000_000;
const MAX_CHUNK_BYTES: usize = 1_048_576;

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
    pub delay_us: Option<u64>,
    #[serde(default)]
    pub request_id: Option<String>,
}

pub fn app() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/chat/completions", post(chat_completions))
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
    if chunk_bytes > MAX_CHUNK_BYTES {
        return Err(format!("chunk_bytes must be <= {MAX_CHUNK_BYTES}"));
    }

    Ok(())
}

pub fn build_sse_events(request: &ChatRequest) -> Result<Vec<String>, String> {
    validate_request(request)?;

    let chunks = request.chunks.unwrap_or(DEFAULT_CHUNKS);
    let chunk_bytes = request.chunk_bytes.unwrap_or(DEFAULT_CHUNK_BYTES);
    let content = "x".repeat(chunk_bytes);
    let model = request.model.as_deref().unwrap_or("synthetic");
    let id = request.request_id.as_deref().unwrap_or("chatcmpl-synthetic");
    let mut events = Vec::with_capacity(chunks + 1);

    for _ in 0..chunks {
        let payload = json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": 0,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "content": content
                    },
                    "finish_reason": null
                }
            ]
        });
        let encoded = serde_json::to_string(&payload).map_err(|error| error.to_string())?;
        events.push(format!("data: {encoded}\n\n"));
    }

    events.push("data: [DONE]\n\n".to_string());
    Ok(events)
}

async fn health() -> &'static str {
    "ok"
}

async fn chat_completions(Json(request): Json<ChatRequest>) -> Result<Response, (StatusCode, String)> {
    let events = build_sse_events(&request).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let delay = Duration::from_micros(request.delay_us.unwrap_or(0));

    let response_stream = stream! {
        for (index, event) in events.into_iter().enumerate() {
            if index > 0 && !delay.is_zero() {
                sleep(delay).await;
            }
            yield Ok::<Bytes, Infallible>(Bytes::from(event));
        }
    };

    let mut response = Response::new(Body::from_stream(response_stream));
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    Ok(response)
}
