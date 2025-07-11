package engine

import (
	"context"
	"log/slog"
	"time"
)

const defaultReclaimInterval = 5 * time.Second

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
	reclaimed, err := e.store.ReclaimExpiredLeases(ctx, e.clock())
	if err != nil {
		if ctx.Err() == nil {
			e.logger.Error("lease reclaim failed", slog.String("error", err.Error()))
		}

		return
	}

	if len(reclaimed) == 0 {
		return
	}

	queues := make([]string, 0, len(reclaimed))

	for _, msg := range reclaimed {
		queues = append(queues, msg.Queue)

		e.logger.Warn("lease expired, message returned to queue",
			slog.String("message_id", msg.ID),
			slog.String("queue", msg.Queue),
			slog.Int("attempts", msg.Attempts),
		)
	}

	e.notify(queues)
}
