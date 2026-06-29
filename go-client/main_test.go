package main

import (
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
