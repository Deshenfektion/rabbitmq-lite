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
)

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
