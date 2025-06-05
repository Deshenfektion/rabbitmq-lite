package message

import (
	"errors"
	"testing"
	"time"
)

func TestStateTransitionsFollowLifecycle(t *testing.T) {
	cases := []struct {
		from  State
		to    State
		valid bool
	}{
		{StateCreated, StateQueued, true},
		{StateCreated, StateFailed, true},
		{StateCreated, StateProcessing, false},
		{StateQueued, StateProcessing, true},
		{StateQueued, StateAcknowledged, false},
		{StateProcessing, StateAcknowledged, true},
		{StateProcessing, StateFailed, true},
		{StateProcessing, StateQueued, true},
		{StateProcessing, StateDeadLettered, false},
		{StateFailed, StateRetrying, true},
		{StateFailed, StateDeadLettered, true},
		{StateFailed, StateQueued, false},
		{StateRetrying, StateQueued, true},
		{StateRetrying, StateAcknowledged, false},
		{StateAcknowledged, StateQueued, false},
		{StateDeadLettered, StateQueued, false},
	}

	for _, tc := range cases {
		if got := tc.from.CanTransitionTo(tc.to); got != tc.valid {
			t.Errorf("%s -> %s: expected valid=%v, got %v", tc.from, tc.to, tc.valid, got)
		}
	}
}

func TestTerminalStates(t *testing.T) {
	terminal := map[State]bool{
		StateAcknowledged: true,
		StateDeadLettered: true,
	}

	for _, state := range States() {
		if state.Terminal() != terminal[state] {
			t.Errorf("state %s: expected terminal=%v", state, terminal[state])
		}
	}
}

func TestTransitionRejectsInvalidTarget(t *testing.T) {
	msg := &Message{State: StateQueued}

	err := msg.Transition(StateDeadLettered, time.Now())

	var invalid *InvalidTransitionError
	if !errors.As(err, &invalid) {
		t.Fatalf("expected InvalidTransitionError, got %v", err)
	}

	if invalid.From != StateQueued || invalid.To != StateDeadLettered {
		t.Fatalf("unexpected transition error contents: %+v", invalid)
	}

	if msg.State != StateQueued {
		t.Fatalf("state must not change on rejected transition, got %s", msg.State)
	}
}

func TestTransitionRejectsUnknownState(t *testing.T) {
	msg := &Message{State: StateQueued}

	var unknown *UnknownStateError
	if err := msg.Transition(State("SHIPPED"), time.Now()); !errors.As(err, &unknown) {
		t.Fatalf("expected UnknownStateError, got %v", err)
	}
}

func TestTransitionAdvancesTimestamp(t *testing.T) {
	created := time.Date(2025, time.June, 5, 18, 0, 0, 0, time.UTC)
	moved := created.Add(90 * time.Second)

	msg := &Message{State: StateCreated, UpdatedAt: created}

	if err := msg.Transition(StateQueued, moved); err != nil {
		t.Fatalf("transition failed: %v", err)
	}

	if !msg.UpdatedAt.Equal(moved) {
		t.Fatalf("expected UpdatedAt %s, got %s", moved, msg.UpdatedAt)
	}
}

func TestParseState(t *testing.T) {
	state, err := ParseState("RETRYING")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if state != StateRetrying {
		t.Fatalf("expected RETRYING, got %s", state)
	}

	if _, err := ParseState("PARKED"); err == nil {
		t.Fatal("expected error for unknown state")
	}
}

func TestFullDeliveryPathIsReachable(t *testing.T) {
	path := []State{
		StateQueued,
		StateProcessing,
		StateFailed,
		StateRetrying,
		StateQueued,
		StateProcessing,
		StateFailed,
		StateDeadLettered,
	}

	msg := &Message{State: StateCreated}
	now := time.Now()

	for _, next := range path {
		if err := msg.Transition(next, now); err != nil {
			t.Fatalf("unexpected error walking lifecycle: %v", err)
		}
	}

	if !msg.State.Terminal() {
		t.Fatalf("expected terminal state, got %s", msg.State)
	}
}
