package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/worker"
)

const idlePollInterval = 250 * time.Millisecond

func (e *Engine) Subscribe(spec ConsumerSpec) (*Consumer, error) {
	if spec.Handler == nil {
		return nil, ErrHandlerRequired
	}

	queue, err := e.registry.Queue(spec.Queue)
	if err != nil {
		return nil, err
	}

	spec = spec.withDefaults(queue.VisibilityTimeout)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.rootCtx == nil {
		return nil, ErrNotRunning
	}

	if _, exists := e.consumers[spec.Name]; exists {
		return nil, ErrConsumerExists
	}

	pool := worker.NewPool(worker.PoolConfig{Size: spec.Concurrency, QueueSize: spec.Prefetch})
	ctx, cancel := context.WithCancel(e.rootCtx)
	pool.Start(ctx)

	consumer := &Consumer{
		spec: spec,
		pool: pool,
		stop: cancel,
		done: make(chan struct{}),
	}

	e.consumers[spec.Name] = consumer
	e.wg.Add(1)

	go func() {
		defer e.wg.Done()
		defer close(consumer.done)
		e.dispatch(ctx, consumer)
	}()

	e.logger.Info("consumer subscribed",
		slog.String("consumer", spec.Name),
		slog.String("queue", spec.Queue),
		slog.Int("concurrency", spec.Concurrency),
		slog.Int("prefetch", spec.Prefetch),
	)

	return consumer, nil
}

func (e *Engine) Unsubscribe(name string) error {
	e.mu.Lock()
	consumer, ok := e.consumers[name]
	if ok {
		delete(e.consumers, name)
	}
	e.mu.Unlock()

	if !ok {
		return ErrConsumerNotFound
	}

	consumer.Stop()

	return nil
}

func (e *Engine) Consumers() []ConsumerStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	statuses := make([]ConsumerStatus, 0, len(e.consumers))
	for _, consumer := range e.consumers {
		statuses = append(statuses, consumer.Status())
	}

	return statuses
}

func (e *Engine) dispatch(ctx context.Context, consumer *Consumer) {
	signal := e.queueSignal(consumer.spec.Queue)
	idle := time.NewTimer(idlePollInterval)
	defer idle.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		claimed, err := e.claim(ctx, consumer)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			e.logger.Error("claim failed",
				slog.String("consumer", consumer.spec.Name),
				slog.String("queue", consumer.spec.Queue),
				slog.String("error", err.Error()),
			)
		}

		if claimed > 0 {
			continue
		}

		idle.Reset(idlePollInterval)

		select {
		case <-ctx.Done():
			return
		case <-signal:
		case <-idle.C:
		}
	}
}

func (e *Engine) claim(ctx context.Context, consumer *Consumer) (int, error) {
	queue, err := e.registry.Queue(consumer.spec.Queue)
	if err != nil {
		return 0, err
	}

	messages, err := e.store.Claim(ctx, storage.ClaimRequest{
		Queue:    consumer.spec.Queue,
		Consumer: consumer.spec.Name,
		Limit:    consumer.spec.Prefetch,
		Lease:    queue.VisibilityTimeout,
		Now:      e.clock(),
	})
	if err != nil {
		return 0, err
	}

	for _, msg := range messages {
		delivery := Delivery{Message: msg, Attempt: msg.Attempts, Queue: msg.Queue}

		e.recordHistory(ctx, storage.HistoryEvent{
			MessageID: msg.ID,
			Queue:     msg.Queue,
			From:      message.StateQueued,
			To:        message.StateProcessing,
			Consumer:  consumer.spec.Name,
			At:        e.clock(),
		})

		if err := consumer.pool.Submit(ctx, func(taskCtx context.Context) {
			e.process(taskCtx, consumer, delivery)
		}); err != nil {
			if releaseErr := e.store.Release(context.WithoutCancel(ctx), msg.ID, e.clock()); releaseErr != nil {
				e.logger.Error("failed to release undispatched message",
					slog.String("message_id", msg.ID),
					slog.String("error", releaseErr.Error()),
				)
			}

			if errors.Is(err, worker.ErrPoolStopped) || ctx.Err() != nil {
				return len(messages), nil
			}

			return len(messages), err
		}
	}

	return len(messages), nil
}

func (e *Engine) process(ctx context.Context, consumer *Consumer, delivery Delivery) {
	handlerCtx, cancel := context.WithTimeout(ctx, consumer.spec.ProcessingTimeout)
	defer cancel()

	started := e.clock()
	err := consumer.spec.Handler.Handle(handlerCtx, delivery)
	completionCtx := context.WithoutCancel(ctx)

	if err == nil {
		e.acknowledge(completionCtx, consumer, delivery, started)
		return
	}

	e.fail(completionCtx, consumer, delivery, err)
}

func (e *Engine) acknowledge(ctx context.Context, consumer *Consumer, delivery Delivery, started time.Time) {
	now := e.clock()

	if err := e.store.Acknowledge(ctx, delivery.Message.ID, now); err != nil {
		e.logger.Error("acknowledge failed",
			slog.String("message_id", delivery.Message.ID),
			slog.String("error", err.Error()),
		)

		return
	}

	e.recordHistory(ctx, storage.HistoryEvent{
		MessageID: delivery.Message.ID,
		Queue:     delivery.Queue,
		From:      message.StateProcessing,
		To:        message.StateAcknowledged,
		Consumer:  consumer.spec.Name,
		At:        now,
	})

	e.logger.Debug("message acknowledged",
		slog.String("message_id", delivery.Message.ID),
		slog.String("queue", delivery.Queue),
		slog.Duration("duration", now.Sub(started)),
	)
}

func (e *Engine) fail(ctx context.Context, consumer *Consumer, delivery Delivery, cause error) {
	now := e.clock()
	retryable := !isPermanent(cause) && e.policy.ShouldRetry(delivery.Attempt)

	if !retryable {
		e.deadLetter(ctx, consumer.spec.Name, delivery, cause, now)
		return
	}

	availableAt := e.policy.NextAttemptAt(now, delivery.Attempt, e.randomiser)

	if err := e.store.ScheduleRetry(ctx, storage.RetryRequest{
		ID:          delivery.Message.ID,
		Attempt:     delivery.Attempt,
		Reason:      cause.Error(),
		AvailableAt: availableAt,
		Now:         now,
	}); err != nil {
		e.logger.Error("failed to schedule retry",
			slog.String("message_id", delivery.Message.ID),
			slog.String("error", err.Error()),
		)

		return
	}

	e.recordHistory(ctx, storage.HistoryEvent{
		MessageID: delivery.Message.ID,
		Queue:     delivery.Queue,
		From:      message.StateFailed,
		To:        message.StateRetrying,
		Consumer:  consumer.spec.Name,
		Detail:    cause.Error(),
		At:        now,
	})

	e.logger.Warn("delivery failed, retry scheduled",
		slog.String("message_id", delivery.Message.ID),
		slog.String("queue", delivery.Queue),
		slog.Int("attempt", delivery.Attempt),
		slog.Time("available_at", availableAt),
		slog.String("error", cause.Error()),
	)
}

func (e *Engine) queueSignal(queue string) chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()

	signal, ok := e.signals[queue]
	if !ok {
		signal = make(chan struct{}, 1)
		e.signals[queue] = signal
	}

	return signal
}

func (e *Engine) notify(queues []string) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, queue := range queues {
		signal, ok := e.signals[queue]
		if !ok {
			continue
		}

		select {
		case signal <- struct{}{}:
		default:
		}
	}
}
