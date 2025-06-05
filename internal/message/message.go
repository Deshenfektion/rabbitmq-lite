package message

import (
	"encoding/json"
	"maps"
	"time"
)

type Publication struct {
	Exchange   string            `json:"exchange"`
	RoutingKey string            `json:"routing_key"`
	Payload    json.RawMessage   `json:"payload"`
	Headers    map[string]string `json:"headers,omitempty"`
	Schema     string            `json:"schema,omitempty"`
}

type Message struct {
	ID          string            `json:"id"`
	Exchange    string            `json:"exchange"`
	RoutingKey  string            `json:"routing_key"`
	Queue       string            `json:"queue"`
	Schema      string            `json:"schema,omitempty"`
	Payload     json.RawMessage   `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	State       State             `json:"state"`
	Attempts    int               `json:"attempts"`
	MaxAttempts int               `json:"max_attempts"`
	LastError   string            `json:"last_error,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	AvailableAt time.Time         `json:"available_at"`
}

func New(pub Publication, queue string, now time.Time) *Message {
	stamp := now.UTC()

	return &Message{
		ID:          newIDAt(stamp, defaultEntropy),
		Exchange:    pub.Exchange,
		RoutingKey:  pub.RoutingKey,
		Queue:       queue,
		Schema:      pub.Schema,
		Payload:     append(json.RawMessage(nil), pub.Payload...),
		Headers:     maps.Clone(pub.Headers),
		State:       StateCreated,
		CreatedAt:   stamp,
		UpdatedAt:   stamp,
		AvailableAt: stamp,
	}
}

func (m *Message) Transition(next State, now time.Time) error {
	if !next.Valid() {
		return &UnknownStateError{State: next}
	}

	if !m.State.CanTransitionTo(next) {
		return &InvalidTransitionError{From: m.State, To: next}
	}

	m.State = next
	m.UpdatedAt = now.UTC()

	return nil
}

func (m *Message) Exhausted() bool {
	return m.MaxAttempts > 0 && m.Attempts >= m.MaxAttempts
}

func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}

	copied := *m
	copied.Payload = append(json.RawMessage(nil), m.Payload...)
	copied.Headers = maps.Clone(m.Headers)

	return &copied
}

func (m *Message) Header(key string) (string, bool) {
	value, ok := m.Headers[key]
	return value, ok
}

func (m *Message) SetHeader(key, value string) {
	if m.Headers == nil {
		m.Headers = make(map[string]string, 1)
	}
	m.Headers[key] = value
}
