package message

import "fmt"

type State string

const (
	StateCreated      State = "CREATED"
	StateQueued       State = "QUEUED"
	StateProcessing   State = "PROCESSING"
	StateAcknowledged State = "ACKNOWLEDGED"
	StateFailed       State = "FAILED"
	StateRetrying     State = "RETRYING"
	StateDeadLettered State = "DEAD_LETTERED"
)

var allStates = []State{
	StateCreated,
	StateQueued,
	StateProcessing,
	StateAcknowledged,
	StateFailed,
	StateRetrying,
	StateDeadLettered,
}

var transitions = map[State][]State{
	StateCreated:      {StateQueued, StateFailed},
	StateQueued:       {StateProcessing},
	StateProcessing:   {StateAcknowledged, StateFailed, StateQueued},
	StateFailed:       {StateRetrying, StateDeadLettered},
	StateRetrying:     {StateQueued},
	StateAcknowledged: {},
	StateDeadLettered: {},
}

type InvalidTransitionError struct {
	From State
	To   State
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("message: invalid state transition %s -> %s", e.From, e.To)
}

type UnknownStateError struct {
	State State
}

func (e *UnknownStateError) Error() string {
	return fmt.Sprintf("message: unknown state %q", string(e.State))
}

func States() []State {
	return append([]State(nil), allStates...)
}

func ParseState(raw string) (State, error) {
	candidate := State(raw)
	if _, ok := transitions[candidate]; !ok {
		return "", &UnknownStateError{State: candidate}
	}

	return candidate, nil
}

func (s State) Valid() bool {
	_, ok := transitions[s]
	return ok
}

func (s State) Terminal() bool {
	return len(transitions[s]) == 0
}

func (s State) CanTransitionTo(next State) bool {
	for _, allowed := range transitions[s] {
		if allowed == next {
			return true
		}
	}

	return false
}

func (s State) String() string {
	return string(s)
}
