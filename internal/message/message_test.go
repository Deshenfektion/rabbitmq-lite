package message

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewCopiesPayloadAndHeaders(t *testing.T) {
	payload := json.RawMessage(`{"customer_id":"C-1"}`)
	headers := map[string]string{"source": "erp"}

	msg := New(Publication{
		Exchange:   "erp.events",
		RoutingKey: "customer.created",
		Payload:    payload,
		Headers:    headers,
	}, "customer-sync", time.Now())

	payload[2] = 'X'
	headers["source"] = "mutated"

	if string(msg.Payload) == string(payload) {
		t.Fatal("payload must be copied, not aliased")
	}

	if msg.Headers["source"] != "erp" {
		t.Fatalf("headers must be copied, got %q", msg.Headers["source"])
	}
}

func TestNewStartsInCreatedState(t *testing.T) {
	now := time.Date(2025, time.June, 5, 12, 0, 0, 0, time.UTC)

	msg := New(Publication{Exchange: "erp.events"}, "customer-sync", now)

	if msg.State != StateCreated {
		t.Fatalf("expected CREATED, got %s", msg.State)
	}

	if !msg.AvailableAt.Equal(now) {
		t.Fatalf("expected AvailableAt %s, got %s", now, msg.AvailableAt)
	}

	if msg.ID == "" {
		t.Fatal("expected generated identifier")
	}
}

func TestCloneIsDeep(t *testing.T) {
	original := New(Publication{
		Payload: json.RawMessage(`{"a":1}`),
		Headers: map[string]string{"k": "v"},
	}, "q", time.Now())

	clone := original.Clone()
	clone.SetHeader("k", "changed")
	clone.Payload[1] = 'b'

	if original.Headers["k"] != "v" {
		t.Fatal("clone shares header map with original")
	}

	if string(original.Payload) != `{"a":1}` {
		t.Fatalf("clone shares payload with original: %s", original.Payload)
	}
}

func TestExhausted(t *testing.T) {
	cases := []struct {
		attempts    int
		maxAttempts int
		exhausted   bool
	}{
		{0, 3, false},
		{2, 3, false},
		{3, 3, true},
		{4, 3, true},
		{9, 0, false},
	}

	for _, tc := range cases {
		msg := &Message{Attempts: tc.attempts, MaxAttempts: tc.maxAttempts}
		if msg.Exhausted() != tc.exhausted {
			t.Errorf("attempts=%d max=%d: expected exhausted=%v", tc.attempts, tc.maxAttempts, tc.exhausted)
		}
	}
}
