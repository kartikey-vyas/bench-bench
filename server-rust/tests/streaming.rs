use server_rust::{build_sse_events, validate_request, ChatRequest};

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

#[test]
fn rejects_non_streaming_requests() {
    let request = ChatRequest {
        model: None,
        messages: vec![],
        stream: false,
        chunks: Some(1),
        chunk_bytes: Some(1),
        delay_us: Some(0),
        request_id: None,
    };

    let error = validate_request(&request).unwrap_err();

    assert_eq!(error, "stream must be true");
}
