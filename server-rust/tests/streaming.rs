use std::sync::Arc;
use std::time::Duration;

use http_body_util::BodyExt;
use server_rust::{
    app, app_with_stats, build_stream_plan, validate_request, ChatRequest, ServerStats,
};
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
    assert_eq!(
        batch.len(),
        plan.role_event.len() + 2 * plan.content_event.len()
    );
    let single = plan.batch(false, 1);
    assert_eq!(single, plan.content_event);
}

#[test]
fn rejects_non_streaming_and_zero_chunk_bytes() {
    let mut non_streaming = request(1, 1, 0, 0);
    non_streaming.stream = false;
    assert_eq!(
        validate_request(&non_streaming).unwrap_err(),
        "stream must be true"
    );

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

    assert_eq!(
        events.len(),
        6,
        "role + 3 content + finish + done, got {events:?}"
    );
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
    assert!(
        elapsed >= Duration::from_millis(45),
        "stream finished too fast: {elapsed:?}"
    );
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
        .oneshot(
            http::Request::get("/stats")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = stats_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["requests_started"], 1);
    assert_eq!(value["requests_completed"], 1);
    assert_eq!(value["events_emitted"], 2);

    let reset = router
        .clone()
        .oneshot(
            http::Request::post("/stats/reset")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reset.status(), http::StatusCode::NO_CONTENT);
}
