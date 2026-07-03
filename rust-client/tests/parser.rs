use rust_client::{aggregate_summary, percentile, ClientKind, Config, Measurement, SseDecoder};

#[test]
fn decoder_buffers_partial_events() {
    let mut decoder = SseDecoder::new();

    assert!(decoder.feed("data: {\"a\":").is_empty());
    assert_eq!(
        decoder.feed("1}\n\ndata: [DONE]\n\n"),
        vec!["{\"a\":1}".to_string(), "[DONE]".to_string()]
    );
}

#[test]
fn decoder_ignores_comments() {
    let mut decoder = SseDecoder::new();

    assert_eq!(
        decoder.feed(": keepalive\n\ndata: hello\n\n\n"),
        vec!["hello".to_string()]
    );
}

#[test]
fn percentile_uses_nearest_rank() {
    assert_eq!(percentile(&[10.0, 20.0, 30.0, 40.0], 0.50), 20.0);
    assert_eq!(percentile(&[10.0, 20.0, 30.0, 40.0], 0.95), 40.0);
}

#[test]
fn client_kind_parses_and_labels_implementations() {
    assert_eq!(
        ClientKind::parse("reqwest").unwrap().implementation(),
        "reqwest-tokio"
    );
    assert_eq!(
        ClientKind::parse("hyper").unwrap().implementation(),
        "hyper-tokio"
    );
    assert!(ClientKind::parse("curl").is_err());
}

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
