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
	defaultPullLimit = 10
	maxPullLimit     = 200
)

var ErrNothingToPull = errors.New("engine: no messages are available")

type PullRequest struct {
	Queue    string
	Consumer string
	Limit    int
	Lease    time.Duration
}

func (r PullRequest) withDefaults(visibilityTimeout time.Duration) PullRequest {
	if r.Limit <= 0 {
		r.Limit = defaultPullLimit
	}

	if r.Limit > maxPullLimit {
		r.Limit = maxPullLimit
	}

	if r.Lease <= 0 {
		r.Lease = visibilityTimeout
	}

	if r.Consumer == "" {
		r.Consumer = "anonymous"
	}

	return r
}

func (e *Engine) Pull(ctx context.Context, req PullRequest) ([]*message.Message, error) {
	queue, err := e.registry.Queue(req.Queue)
	if err != nil {
		return nil, err
	}

	req = req.withDefaults(queue.VisibilityTimeout)
	now := e.clock()

	claimed, err := e.store.Claim(ctx, storage.ClaimRequest{
		Queue:    req.Queue,
		Consumer: req.Consumer,
		Limit:    req.Limit,
		Lease:    req.Lease,
		Now:      now,
	})
	if err != nil {
		return nil, err
	}

	for _, msg := range claimed {
		e.recordHistory(ctx, storage.HistoryEvent{
			MessageID: msg.ID,
			Queue:     msg.Queue,
			From:      message.StateQueued,
			To:        message.StateProcessing,
			Consumer:  req.Consumer,
			At:        now,
		})
	}

	return claimed, nil
}

func (e *Engine) Ack(ctx context.Context, id string) error {
	msg, err := e.store.Message(ctx, id)
	if err != nil {
		return err
	}

	now := e.clock()

	if err := e.store.Acknowledge(ctx, id, now); err != nil {
		return err
	}

	e.recordHistory(ctx, storage.HistoryEvent{
		MessageID: id,
		Queue:     msg.Queue,
		From:      message.StateProcessing,
		To:        message.StateAcknowledged,
		At:        now,
	})

	e.logger.Debug("message acknowledged over http", slog.String("message_id", id))

	return nil
}

func (e *Engine) Nack(ctx context.Context, id, reason string, requeue bool) error {
	msg, err := e.store.Message(ctx, id)
	if err != nil {
		return err
	}

	if reason == "" {
		reason = "negatively acknowledged by consumer"
	}

	if requeue {
		now := e.clock()

		if err := e.store.Release(ctx, id, now); err != nil {
			return err
		}

		e.recordHistory(ctx, storage.HistoryEvent{
			MessageID: id,
			Queue:     msg.Queue,
			From:      message.StateProcessing,
			To:        message.StateQueued,
			Detail:    reason,
			At:        now,
		})

		e.notify([]string{msg.Queue})

		return nil
	}

	e.fail(ctx, "", Delivery{Message: msg, Attempt: msg.Attempts, Queue: msg.Queue}, errors.New(reason))

	e.notify([]string{msg.Queue})

	return nil
}
