use rust_client::{percentile, ClientKind, SseDecoder};

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
