package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

const (
	defaultReclaimInterval = 5 * time.Second
	reclaimBatchSize       = 256
)

var errLeaseExpired = errors.New("lease expired before the consumer acknowledged the message")

func (e *Engine) startMaintenance(ctx context.Context) {
	e.wg.Add(1)

	go func() {
		defer e.wg.Done()

		ticker := time.NewTicker(e.reclaimInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.reclaim(ctx)
			}
		}
	}()
}

func (e *Engine) reclaim(ctx context.Context) {
	now := e.clock()

	expired, err := e.store.ExpiredLeases(ctx, now, reclaimBatchSize)
	if err != nil {
		if ctx.Err() == nil {
			e.logger.Error("lease reclaim failed", slog.String("error", err.Error()))
		}

		return
	}

	requeued := make([]string, 0, len(expired))

	for _, msg := range expired {
		if e.policy.ShouldRetry(msg.Attempts) {
			if e.requeueExpired(ctx, msg, now) {
				requeued = append(requeued, msg.Queue)
			}

			continue
		}

		e.deadLetter(ctx, msg.Headers[headerConsumer], Delivery{
			Message: msg,
			Attempt: msg.Attempts,
			Queue:   msg.Queue,
		}, errLeaseExpired, now)
	}

	e.notify(requeued)
}

func (e *Engine) requeueExpired(ctx context.Context, msg *message.Message, now time.Time) bool {
	if err := e.store.Release(ctx, msg.ID, now); err != nil {
		e.logger.Error("failed to requeue expired lease",
			slog.String("message_id", msg.ID),
			slog.String("error", err.Error()),
		)

		return false
	}

	e.recordHistory(ctx, storage.HistoryEvent{
		MessageID: msg.ID,
		Queue:     msg.Queue,
		From:      message.StateProcessing,
		To:        message.StateQueued,
		Detail:    errLeaseExpired.Error(),
		At:        now,
	})

	e.logger.Warn("lease expired, message returned to queue",
		slog.String("message_id", msg.ID),
		slog.String("queue", msg.Queue),
		slog.Int("attempts", msg.Attempts),
	)

	return true
}
