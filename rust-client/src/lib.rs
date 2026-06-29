use std::{
    cmp,
    error::Error,
    io,
    path::{Path, PathBuf},
    time::Instant,
};

use chrono::Utc;
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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
    pub total_requests: usize,
    pub concurrency: usize,
    pub chunks_per_response: usize,
    pub chunk_bytes: usize,
    pub delay_us: u64,
    pub warmup_requests: usize,
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
        if self.concurrency == 0 {
            return Err("concurrency must be > 0".to_string());
        }
        if self.chunks_per_response == 0 {
            return Err("chunks_per_response must be > 0".to_string());
        }
        Ok(())
    }

    pub fn endpoint(&self) -> String {
        format!(
            "{}/v1/chat/completions",
            self.base_url.trim_end_matches('/')
        )
    }

    pub fn request_payload(&self, request_index: usize, language: &str) -> Value {
        json!({
            "model": "synthetic",
            "messages": [{"role": "user", "content": "benchmark"}],
            "stream": true,
            "chunks": self.chunks_per_response,
            "chunk_bytes": self.chunk_bytes,
            "delay_us": self.delay_us,
            "request_id": format!("{language}-{request_index}"),
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
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub duration_ms: f64,
    pub successful_requests: usize,
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
    pub per_chunk_overhead_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResultEnvelope {
    pub language: String,
    pub implementation: String,
    pub started_at: String,
    pub config: Config,
    pub summary: Summary,
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

pub async fn run_one_request(client: &reqwest::Client, config: &Config, request_index: usize) -> Measurement {
    let started = Instant::now();
    let response = match client
        .post(config.endpoint())
        .json(&config.request_payload(request_index, "rust"))
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return failed_measurement(started, 0.0, 0, 0),
    };

    if !response.status().is_success() {
        let _ = response.bytes().await;
        return failed_measurement(started, 0.0, 0, 0);
    }

    let mut stream = response.bytes_stream();
    let mut decoder = SseDecoder::new();
    let mut first_chunk_ms = 0.0;
    let mut chunks = 0;
    let mut content_bytes = 0;
    let mut saw_done = false;

    while let Some(next) = stream.next().await {
        let bytes = match next {
            Ok(bytes) => bytes,
            Err(_) => return failed_measurement(started, first_chunk_ms, chunks, content_bytes),
        };

        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => return failed_measurement(started, first_chunk_ms, chunks, content_bytes),
        };

        for event in decoder.feed(text) {
            if event == "[DONE]" {
                saw_done = true;
                continue;
            }

            if chunks == 0 {
                first_chunk_ms = started.elapsed().as_secs_f64() * 1000.0;
            }

            let payload: ChunkPayload = match serde_json::from_str(&event) {
                Ok(payload) => payload,
                Err(_) => return failed_measurement(started, first_chunk_ms, chunks, content_bytes),
            };
            let Some(choice) = payload.choices.first() else {
                return failed_measurement(started, first_chunk_ms, chunks, content_bytes);
            };
            chunks += 1;
            content_bytes += choice.delta.content.as_bytes().len();
        }
    }

    Measurement {
        ok: saw_done,
        latency_ms: started.elapsed().as_secs_f64() * 1000.0,
        first_chunk_ms,
        chunks,
        bytes: content_bytes,
    }
}

pub async fn run_many(
    client: reqwest::Client,
    config: Config,
    total_requests: usize,
) -> Vec<Measurement> {
    let concurrency = cmp::max(1, config.concurrency);
    stream::iter(0..total_requests)
        .map(|request_index| {
            let client = client.clone();
            let config = config.clone();
            async move { run_one_request(&client, &config, request_index).await }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await
}

pub async fn run_benchmark(
    config: Config,
    output_dir: Option<PathBuf>,
) -> Result<ResultEnvelope, Box<dyn Error + Send + Sync>> {
    let client = reqwest::Client::builder().build()?;
    let started_at = Utc::now().to_rfc3339();

    if config.warmup_requests > 0 {
        let _ = run_many(client.clone(), config.clone(), config.warmup_requests).await;
    }

    let measured_start = Instant::now();
    let measurements = run_many(client, config.clone(), config.total_requests).await;
    let duration_ms = measured_start.elapsed().as_secs_f64() * 1000.0;

    let result = ResultEnvelope {
        language: "rust".to_string(),
        implementation: "reqwest-tokio".to_string(),
        started_at,
        config: config.clone(),
        summary: aggregate_summary(&measurements, duration_ms),
    };

    let destination = output_dir.unwrap_or_else(|| PathBuf::from(&config.output_dir).join("rust"));
    tokio::fs::create_dir_all(&destination).await?;
    let content = serde_json::to_string_pretty(&result)? + "\n";
    tokio::fs::write(destination.join("summary.json"), content).await?;

    Ok(result)
}

pub fn aggregate_summary(measurements: &[Measurement], duration_ms: f64) -> Summary {
    let mut latencies = Vec::new();
    let mut first_chunks = Vec::new();
    let mut successful_requests = 0;
    let mut failed_requests = 0;
    let mut total_chunks = 0;
    let mut total_bytes = 0;

    for measurement in measurements {
        if measurement.ok {
            successful_requests += 1;
            latencies.push(measurement.latency_ms);
            first_chunks.push(measurement.first_chunk_ms);
            total_chunks += measurement.chunks;
            total_bytes += measurement.bytes;
        } else {
            failed_requests += 1;
        }
    }

    let duration_seconds = duration_ms / 1000.0;
    Summary {
        duration_ms,
        successful_requests,
        failed_requests,
        total_chunks,
        total_bytes,
        requests_per_second: if duration_seconds > 0.0 {
            successful_requests as f64 / duration_seconds
        } else {
            0.0
        },
        chunks_per_second: if duration_seconds > 0.0 {
            total_chunks as f64 / duration_seconds
        } else {
            0.0
        },
        mean_request_latency_ms: mean(&latencies),
        p50_request_latency_ms: percentile(&latencies, 0.50),
        p95_request_latency_ms: percentile(&latencies, 0.95),
        p99_request_latency_ms: percentile(&latencies, 0.99),
        mean_time_to_first_chunk_ms: mean(&first_chunks),
        p50_time_to_first_chunk_ms: percentile(&first_chunks, 0.50),
        p95_time_to_first_chunk_ms: percentile(&first_chunks, 0.95),
        p99_time_to_first_chunk_ms: percentile(&first_chunks, 0.99),
        per_chunk_overhead_ms: if total_chunks > 0 {
            duration_ms / total_chunks as f64
        } else {
            0.0
        },
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

fn failed_measurement(
    started: Instant,
    first_chunk_ms: f64,
    chunks: usize,
    content_bytes: usize,
) -> Measurement {
    Measurement {
        ok: false,
        latency_ms: started.elapsed().as_secs_f64() * 1000.0,
        first_chunk_ms,
        chunks,
        bytes: content_bytes,
    }
}

fn normalize_newlines(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}
