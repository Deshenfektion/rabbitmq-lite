package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

func (e *Engine) deadLetter(ctx context.Context, consumer string, delivery Delivery, cause error, now time.Time) {
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
		slog.String("queue", delivery.Queue),
		slog.Int("attempts", delivery.Attempt),
		slog.String("error", cause.Error()),
	)
}
