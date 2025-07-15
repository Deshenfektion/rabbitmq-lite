package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

const (
	errorKindHandler   = "handler"
	errorKindPermanent = "permanent"
	errorKindExpired   = "lease_expired"

	headerReplayOf     = "x-replay-of"
	headerDeadLetterID = "x-dead-letter-id"
)

type ReplayResult struct {
	DeadLetterID      string `json:"dead_letter_id"`
	OriginalMessageID string `json:"original_message_id"`
	MessageID         string `json:"message_id"`
	Queue             string `json:"queue"`
}

func (e *Engine) deadLetter(ctx context.Context, consumer string, delivery Delivery, cause error, now time.Time) {
	record := &storage.DeadLetter{
		MessageID:      delivery.Message.ID,
		Queue:          delivery.Queue,
		Exchange:       delivery.Message.Exchange,
		RoutingKey:     delivery.Message.RoutingKey,
		Schema:         delivery.Message.Schema,
		Payload:        delivery.Message.Payload,
		Headers:        delivery.Message.Headers,
		Reason:         cause.Error(),
		ErrorKind:      classify(cause),
		Attempts:       delivery.Attempt,
		PublishedAt:    delivery.Message.CreatedAt,
		FirstFailedAt:  e.firstFailureAt(ctx, delivery.Message.ID, now),
		DeadLetteredAt: now,
	}

	if err := e.store.SaveDeadLetter(ctx, record); err != nil {
		e.logger.Error("failed to persist dead letter record",
			slog.String("message_id", delivery.Message.ID),
			slog.String("error", err.Error()),
		)

		return
	}

	if err := e.store.MarkDeadLettered(ctx, delivery.Message.ID, cause.Error(), now); err != nil {
		e.logger.Error("failed to dead letter message",
			slog.String("message_id", delivery.Message.ID),
			slog.String("error", err.Error()),
		)

		return
	}

	e.recordHistory(ctx, storage.HistoryEvent{
		MessageID: delivery.Message.ID,
		Queue:     delivery.Queue,
		From:      message.StateFailed,
		To:        message.StateDeadLettered,
		Consumer:  consumer,
		Detail:    cause.Error(),
		At:        now,
	})

	e.logger.Warn("message dead lettered",
		slog.String("message_id", delivery.Message.ID),
		slog.String("dead_letter_id", record.ID),
		slog.String("queue", delivery.Queue),
		slog.Int("attempts", delivery.Attempt),
		slog.String("error", cause.Error()),
	)
}

func (e *Engine) DeadLetters(ctx context.Context, filter storage.DeadLetterFilter) ([]*storage.DeadLetter, error) {
	return e.store.DeadLetters(ctx, filter)
}

func (e *Engine) DeadLetter(ctx context.Context, id string) (*storage.DeadLetter, error) {
	return e.store.DeadLetter(ctx, id)
}

func (e *Engine) DiscardDeadLetter(ctx context.Context, id string) error {
	return e.store.DeleteDeadLetter(ctx, id)
}

func (e *Engine) CountDeadLetters(ctx context.Context, queue string) (int, error) {
	return e.store.CountDeadLetters(ctx, queue)
}

func (e *Engine) firstFailureAt(ctx context.Context, messageID string, fallback time.Time) time.Time {
	events, err := e.store.History(ctx, messageID)
	if err != nil {
		return fallback
	}

	for _, event := range events {
		if event.To == message.StateRetrying || event.To == message.StateFailed {
			return event.At
		}
	}

	return fallback
}

func classify(cause error) string {
	if isPermanent(cause) {
		return errorKindPermanent
	}

	return errorKindHandler
}

func (e *Engine) ReplayDeadLetter(ctx context.Context, id string) (*ReplayResult, error) {
	record, err := e.store.DeadLetter(ctx, id)
	if err != nil {
		return nil, err
	}

	queue, err := e.registry.Queue(record.Queue)
	if err != nil {
		return nil, err
	}

	now := e.clock()

	replay := message.New(message.Publication{
		Exchange:   record.Exchange,
		RoutingKey: record.RoutingKey,
		Payload:    record.Payload,
		Headers:    record.Headers,
		Schema:     record.Schema,
	}, queue.Name, now)

	replay.MaxAttempts = queue.MaxAttempts
	replay.SetHeader(headerReplayOf, record.MessageID)
	replay.SetHeader(headerDeadLetterID, record.ID)

	if err := replay.Transition(message.StateQueued, now); err != nil {
		return nil, err
	}

	if err := e.store.Append(ctx, []*message.Message{replay}); err != nil {
		return nil, err
	}

	e.recordHistory(ctx, storage.HistoryEvent{
		MessageID: replay.ID,
		Queue:     replay.Queue,
		From:      message.StateCreated,
		To:        message.StateQueued,
		Detail:    "replayed from dead letter " + record.ID,
		At:        now,
	})

	if err := e.store.MarkDeadLetterReplayed(ctx, record.ID, replay.ID, now); err != nil {
		e.logger.Error("failed to mark dead letter as replayed",
			slog.String("dead_letter_id", record.ID),
			slog.String("error", err.Error()),
		)
	}

	e.notify([]string{queue.Name})

	e.logger.Info("dead letter replayed",
		slog.String("dead_letter_id", record.ID),
		slog.String("original_message_id", record.MessageID),
		slog.String("message_id", replay.ID),
		slog.String("queue", queue.Name),
	)

	return &ReplayResult{
		DeadLetterID:      record.ID,
		OriginalMessageID: record.MessageID,
		MessageID:         replay.ID,
		Queue:             queue.Name,
	}, nil
}
