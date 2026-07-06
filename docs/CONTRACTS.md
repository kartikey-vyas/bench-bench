# Contracts

Extracted verbatim from the "Shared Contracts" section of `docs/superpowers/plans/2026-07-03-paced-streaming-sweep.md`. This file, not the plan, is the living source of truth for the wire protocol, workload config, and summary schema — update it directly when a contract changes.

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

`ok` (transport+parse success and saw `[DONE]`; drain: read to EOF), `latency_ms`, `first_chunk_ms` (first parsed SSE event — the role event), `chunks` (content events with non-empty `delta.content` only), `bytes` (content bytes; drain: wire bytes), `window_chunks` (content chunks whose arrival timestamp is ≤ the measured window's absolute end — see chunks_per_second below), `max_gap_ms` (max gap between successive parsed events), `stream_ms` (first event → last event).

### summary.json `summary` keys (identical in all clients)

`duration_ms`, `successful_requests`, `incomplete_requests`, `failed_requests`, `total_chunks`, `total_bytes`, `requests_per_second`, `chunks_per_second`, `mean_request_latency_ms`, `p50_request_latency_ms`, `p95_request_latency_ms`, `p99_request_latency_ms`, `mean_time_to_first_chunk_ms`, `p50_time_to_first_chunk_ms`, `p95_time_to_first_chunk_ms`, `p99_time_to_first_chunk_ms`, `p50_max_gap_ms`, `p95_max_gap_ms`, `p99_max_gap_ms`, `max_max_gap_ms`, `p50_stream_stretch`, `p95_stream_stretch`, `p99_stream_stretch`, `ideal_events_per_second`, `efficiency`.

Aggregation rules (identical everywhere):
- successful = `ok && chunks == chunks_per_response`; incomplete = `ok && chunks != expected`; failed = `!ok`.
- Percentiles (nearest-rank, existing implementations) over successful requests only; totals over successful only.
- `ideal_stream_ms = (expected - 1) / events_per_second * 1000` when `events_per_second > 0 && expected > 1`, else stretch list is empty and stretch percentiles are 0.0. `stream_stretch = stream_ms / ideal_stream_ms` per successful request.
- **[AMENDED after Task 9 integration]** `ideal_events_per_second` must account for TTFC dead time in the closed loop: when `events_per_second > 0`, `ideal_request_seconds = ttfc_ms/1000 + (expected - 1)/events_per_second`, and `ideal_events_per_second = concurrency * expected / ideal_request_seconds` (0.0 if `ideal_request_seconds` is 0). When unpaced, 0.0. `efficiency = chunks_per_second / ideal_events_per_second` (0.0 when unpaced). The original `eps × concurrency` definition was unreachable for any client (a perfect closed-loop client idles through TTFC every request) and would have falsely triggered stop rules. Python's `aggregate_summary` gains a `ttfc_ms` parameter.
- `requests_per_second = successful / duration_seconds` (actual duration).
- `chunks_per_second` = content chunks received inside the measured window (all requests, including failed/incomplete) ÷ configured `duration_seconds` — window-clipped so a straggling worker cannot dilute the aggregate. Frame-granular clients (drain, python-deferred) approximate clipped counts to within one event. Each per-request measurement carries a `window_chunks` field (content chunks, or for frame-granular clients, `min(total_content_chunks, in_window_event_boundaries)`) counted against the window's absolute end (`started + duration_seconds`), passed into the per-request call by the closed-loop worker loop that owns it. `chunks_per_second = sum(window_chunks over ALL measurements) / duration_seconds` (the configured value, not the stretched actual duration). `efficiency = chunks_per_second / ideal_events_per_second` (formula unchanged; only the numerator's counting changed).
- `per_chunk_overhead_ms` is REMOVED from the schema.

### Result envelope (unchanged shape)

`{"language", "implementation", "started_at", "config", "summary"}` — config is the full workload config.
