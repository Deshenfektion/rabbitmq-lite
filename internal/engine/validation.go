package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

const errorKindValidation = "validation"

func (e *Engine) validate(ctx context.Context, msg *message.Message, now time.Time) error {
	if e.schemas == nil || msg.Schema == "" {
		return nil
	}

	err := e.schemas.Validate(msg.Schema, msg.Payload)
	if err == nil {
		return nil
	}

	if transitionErr := msg.Transition(message.StateFailed, now); transitionErr != nil {
		e.logger.Error("failed to mark rejected message",
			slog.String("message_id", msg.ID),
			slog.String("error", transitionErr.Error()),
		)
	}

	e.parkRejected(ctx, msg, err, now)

	e.logger.Warn("publication rejected by schema validation",
		slog.String("queue", msg.Queue),
		slog.String("schema", msg.Schema),
		slog.String("error", err.Error()),
	)

	return err
}

func (e *Engine) parkRejected(ctx context.Context, msg *message.Message, cause error, now time.Time) {
	record := &storage.DeadLetter{
		MessageID:      msg.ID,
		Queue:          msg.Queue,
		Exchange:       msg.Exchange,
		RoutingKey:     msg.RoutingKey,
		Schema:         msg.Schema,
		Payload:        msg.Payload,
		Headers:        msg.Headers,
		Reason:         cause.Error(),
		ErrorKind:      errorKindValidation,
		Attempts:       0,
		PublishedAt:    msg.CreatedAt,
		FirstFailedAt:  now,
		DeadLetteredAt: now,
	}

	if err := e.store.SaveDeadLetter(ctx, record); err != nil {
		e.logger.Error("failed to park rejected payload",
			slog.String("message_id", msg.ID),
			slog.String("error", err.Error()),
		)
	}
}
