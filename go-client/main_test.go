package main

import (
	"math"
	"reflect"
	"testing"
)

func TestDecodeSSEEventsBuffersPartialEvents(t *testing.T) {
	decoder := newSSEDecoder()

	if events := decoder.feed(`data: {"a":`); len(events) != 0 {
		t.Fatalf("expected no complete events, got %v", events)
	}

	events := decoder.feed("1}\n\ndata: [DONE]\n\n")
	expected := []string{`{"a":1}`, "[DONE]"}
	if !reflect.DeepEqual(events, expected) {
		t.Fatalf("events mismatch: got %v want %v", events, expected)
	}
}

func TestDecodeSSEEventsIgnoresComments(t *testing.T) {
	decoder := newSSEDecoder()

	events := decoder.feed(": keepalive\n\ndata: hello\n\n\n")
	expected := []string{"hello"}
	if !reflect.DeepEqual(events, expected) {
		t.Fatalf("events mismatch: got %v want %v", events, expected)
	}
}

func TestPercentileUsesNearestRank(t *testing.T) {
	values := []float64{10, 20, 30, 40}

	if got := percentile(values, 0.50); got != 20 {
		t.Fatalf("p50 = %v, want 20", got)
	}
	if got := percentile(values, 0.95); got != 40 {
		t.Fatalf("p95 = %v, want 40", got)
	}
}

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
	// ideal_request_seconds = 0 + 3/100 = 0.03; ideal = 2*4/0.03 = 266.6667; efficiency = 4/266.6667 = 0.015
	if math.Abs(summary.IdealEventsPerSecond-266.6666666666667) > 1e-6 {
		t.Fatalf("ideal = %v, want 266.6666666666667", summary.IdealEventsPerSecond)
	}
	if math.Abs(summary.Efficiency-0.015) > 1e-6 {
		t.Fatalf("efficiency = %v, want 0.015", summary.Efficiency)
	}
	// ideal stream = (4-1)/100*1000 = 30ms; stretch = 30/30 = 1.0
	if math.Abs(summary.P50StreamStretch-1.0) > 1e-9 {
		t.Fatalf("p50 stretch = %v, want 1.0", summary.P50StreamStretch)
	}
	if summary.MaxMaxGapMS != 12.0 {
		t.Fatalf("max max gap = %v, want 12 (successful requests only)", summary.MaxMaxGapMS)
	}
}
