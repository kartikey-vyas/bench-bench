use std::{
    error::Error,
    io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use bytes::Bytes;
use chrono::Utc;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, header, Request};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client as HyperClient},
    rt::TokioExecutor,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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

#[derive(Debug, Clone)]
pub struct SseDecoder {
    buffer: String,
}

impl SseDecoder {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    pub fn feed(&mut self, text: &str) -> Vec<String> {
        self.buffer.push_str(&normalize_newlines(text));
        let mut events = Vec::new();

        while let Some(index) = self.buffer.find("\n\n") {
            let raw_event = self.buffer[..index].to_string();
            self.buffer = self.buffer[index + 2..].to_string();
            let mut data_lines = Vec::new();

            for line in raw_event.lines() {
                if line.is_empty() || line.starts_with(':') {
                    continue;
                }
                if let Some(value) = line.strip_prefix("data:") {
                    data_lines.push(value.strip_prefix(' ').unwrap_or(value).to_string());
                }
            }

            if !data_lines.is_empty() {
                events.push(data_lines.join("\n"));
            }
        }

        events
    }
}

impl Default for SseDecoder {
    fn default() -> Self {
        Self::new()
    }
}

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

#[derive(Debug, Clone, Serialize)]
pub struct ResultEnvelope {
    pub language: String,
    pub implementation: String,
    pub started_at: String,
    pub config: Config,
    pub summary: Summary,
}

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

#[derive(Debug, Deserialize)]
struct ChunkPayload {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    delta: Delta,
}

#[derive(Debug, Deserialize)]
struct Delta {
    #[serde(default)]
    content: String,
}

type HyperHttpClient = HyperClient<HttpConnector, Full<Bytes>>;

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
        let first_chunk_ms = self.first_event_at.map_or(0.0, |at| {
            at.duration_since(self.started).as_secs_f64() * 1000.0
        });
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

pub async fn run_one_drain_request(
    client: &HyperHttpClient,
    config: &Config,
    worker_index: usize,
    sequence: usize,
) -> Measurement {
    let started = Instant::now();
    let mut timing = StreamTiming::new(started);
    let body =
        match serde_json::to_vec(&config.request_payload(worker_index, sequence, "rust-drain")) {
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

#[derive(Clone)]
pub enum AnyClient {
    Reqwest(reqwest::Client),
    Hyper(HyperHttpClient),
    Drain(HyperHttpClient),
}

pub fn build_client(kind: ClientKind) -> Result<AnyClient, Box<dyn Error + Send + Sync>> {
    Ok(match kind {
        ClientKind::Reqwest => AnyClient::Reqwest(reqwest::Client::builder().build()?),
        ClientKind::Hyper => {
            // hyper-util's HttpConnector defaults nodelay=false, while Go/reqwest
            // default true; without this the hyper (and drain, the reference)
            // clients would be skewed relative to the others.
            let mut connector = HttpConnector::new();
            connector.set_nodelay(true);
            AnyClient::Hyper(HyperClient::builder(TokioExecutor::new()).build(connector))
        }
        ClientKind::Drain => {
            let mut connector = HttpConnector::new();
            connector.set_nodelay(true);
            AnyClient::Drain(HyperClient::builder(TokioExecutor::new()).build(connector))
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
        AnyClient::Drain(inner) => {
            run_one_drain_request(inner, config, worker_index, sequence).await
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

pub async fn run_benchmark(
    config: Config,
    output_dir: Option<PathBuf>,
) -> Result<ResultEnvelope, Box<dyn Error + Send + Sync>> {
    run_benchmark_with_client(config, output_dir, ClientKind::Reqwest).await
}

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

pub fn aggregate_summary(
    measurements: &[Measurement],
    duration_ms: f64,
    config: &Config,
) -> Summary {
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
    let ideal_events_per_second = if config.events_per_second > 0 {
        let ideal_request_seconds = config.ttfc_ms as f64 / 1000.0
            + (expected - 1) as f64 / config.events_per_second as f64;
        if ideal_request_seconds > 0.0 {
            (config.concurrency * expected) as f64 / ideal_request_seconds
        } else {
            0.0
        }
    } else {
        0.0
    };
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

pub fn percentile(values: &[f64], rank: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mut ordered = values.to_vec();
    ordered.sort_by(|left, right| left.total_cmp(right));
    let index = ((rank * ordered.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(ordered.len() - 1);
    ordered[index]
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn normalize_newlines(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}
