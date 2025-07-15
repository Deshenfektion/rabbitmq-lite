package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
)

type Store interface {
	TopologyStore
	MessageStore
	DeadLetterStore
	HistoryStore

	Close() error
}

type TopologyStore interface {
	SaveExchange(ctx context.Context, exchange *broker.Exchange) error
	Exchanges(ctx context.Context) ([]*broker.Exchange, error)
	SaveQueue(ctx context.Context, queue *broker.Queue) error
	Queues(ctx context.Context) ([]*broker.Queue, error)
	DeleteQueue(ctx context.Context, name string) error
	SaveBinding(ctx context.Context, binding broker.Binding) error
	Bindings(ctx context.Context) ([]broker.Binding, error)
	DeleteBinding(ctx context.Context, spec broker.BindingSpec) error
}

type MessageStore interface {
	Append(ctx context.Context, messages []*message.Message) error
	Message(ctx context.Context, id string) (*message.Message, error)
	Claim(ctx context.Context, req ClaimRequest) ([]*message.Message, error)
	Acknowledge(ctx context.Context, id string, at time.Time) error
	ScheduleRetry(ctx context.Context, req RetryRequest) error
	Release(ctx context.Context, id string, at time.Time) error
	MarkDeadLettered(ctx context.Context, id string, reason string, at time.Time) error
	ReclaimExpiredLeases(ctx context.Context, now time.Time) ([]*message.Message, error)
	Depth(ctx context.Context, queue string) (Depth, error)
	Purge(ctx context.Context, queue string) (int, error)
}

type DeadLetterStore interface {
	SaveDeadLetter(ctx context.Context, record *DeadLetter) error
	DeadLetters(ctx context.Context, filter DeadLetterFilter) ([]*DeadLetter, error)
	DeadLetter(ctx context.Context, id string) (*DeadLetter, error)
	MarkDeadLetterReplayed(ctx context.Context, id string, replayedAs string, at time.Time) error
	DeleteDeadLetter(ctx context.Context, id string) error
	CountDeadLetters(ctx context.Context, queue string) (int, error)
}

type HistoryStore interface {
	AppendHistory(ctx context.Context, event HistoryEvent) error
	History(ctx context.Context, messageID string) ([]HistoryEvent, error)
}

type ClaimRequest struct {
	Queue    string
	Consumer string
	Limit    int
	Lease    time.Duration
	Now      time.Time
}

type RetryRequest struct {
	ID          string
	Attempt     int
	Reason      string
	AvailableAt time.Time
	Now         time.Time
}

type Depth struct {
	Ready     int `json:"ready"`
	InFlight  int `json:"in_flight"`
	Scheduled int `json:"scheduled"`
	Total     int `json:"total"`
}

type DeadLetter struct {
	ID             string            `json:"id"`
	MessageID      string            `json:"message_id"`
	Queue          string            `json:"queue"`
	Exchange       string            `json:"exchange"`
	RoutingKey     string            `json:"routing_key"`
	Schema         string            `json:"schema,omitempty"`
	Payload        json.RawMessage   `json:"payload"`
	Headers        map[string]string `json:"headers,omitempty"`
	Reason         string            `json:"reason"`
	ErrorKind      string            `json:"error_kind"`
	Attempts       int               `json:"attempts"`
	PublishedAt    time.Time         `json:"published_at"`
	FirstFailedAt  time.Time         `json:"first_failed_at"`
	DeadLetteredAt time.Time         `json:"dead_lettered_at"`
	ReplayedAs     string            `json:"replayed_as,omitempty"`
	ReplayedAt     time.Time         `json:"replayed_at,omitzero"`
	ReplayCount    int               `json:"replay_count"`
}

type DeadLetterFilter struct {
	Queue  string
	Limit  int
	Offset int
}

type HistoryEvent struct {
	MessageID string        `json:"message_id"`
	Queue     string        `json:"queue"`
	From      message.State `json:"from"`
	To        message.State `json:"to"`
	Consumer  string        `json:"consumer,omitempty"`
	Detail    string        `json:"detail,omitempty"`
	At        time.Time     `json:"at"`
}

func (f DeadLetterFilter) Normalise() DeadLetterFilter {
	if f.Limit <= 0 || f.Limit > maxPageSize {
		f.Limit = defaultPageSize
	}

	if f.Offset < 0 {
		f.Offset = 0
	}

	return f
}

const (
	defaultPageSize = 50
	maxPageSize     = 500
)
