package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

type Options struct {
	Store  storage.Store
	Clock  func() time.Time
	Logger *slog.Logger
}

type Engine struct {
	store    storage.Store
	registry *broker.Registry
	clock    func() time.Time
	logger   *slog.Logger
}

type PublishResult struct {
	Exchange   string   `json:"exchange"`
	RoutingKey string   `json:"routing_key"`
	MessageIDs []string `json:"message_ids"`
	Queues     []string `json:"queues"`
}

func New(opts Options) (*Engine, error) {
	if opts.Store == nil {
		return nil, errors.New("engine: store is required")
	}

	if opts.Clock == nil {
		opts.Clock = func() time.Time { return time.Now().UTC() }
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	return &Engine{
		store:    opts.Store,
		registry: broker.NewRegistry(),
		clock:    opts.Clock,
		logger:   opts.Logger,
	}, nil
}

func (e *Engine) Registry() *broker.Registry {
	return e.registry
}

func (e *Engine) Store() storage.Store {
	return e.store
}

func (e *Engine) Restore(ctx context.Context) error {
	exchanges, err := e.store.Exchanges(ctx)
	if err != nil {
		return fmt.Errorf("engine: restore exchanges: %w", err)
	}

	queues, err := e.store.Queues(ctx)
	if err != nil {
		return fmt.Errorf("engine: restore queues: %w", err)
	}

	bindings, err := e.store.Bindings(ctx)
	if err != nil {
		return fmt.Errorf("engine: restore bindings: %w", err)
	}

	e.registry.Restore(exchanges, queues, bindings)

	e.logger.Info("topology restored",
		slog.Int("exchanges", len(exchanges)),
		slog.Int("queues", len(queues)),
		slog.Int("bindings", len(bindings)),
	)

	return nil
}

func (e *Engine) DeclareExchange(ctx context.Context, spec broker.ExchangeSpec) (*broker.Exchange, error) {
	exchange, err := e.registry.DeclareExchange(spec)
	if err != nil {
		return nil, err
	}

	if exchange.Durable {
		if err := e.store.SaveExchange(ctx, exchange); err != nil {
			return nil, err
		}
	}

	return exchange, nil
}

func (e *Engine) DeclareQueue(ctx context.Context, spec broker.QueueSpec) (*broker.Queue, error) {
	queue, err := e.registry.DeclareQueue(spec)
	if err != nil {
		return nil, err
	}

	if queue.Durable {
		if err := e.store.SaveQueue(ctx, queue); err != nil {
			return nil, err
		}
	}

	return queue, nil
}

func (e *Engine) Bind(ctx context.Context, spec broker.BindingSpec) (*broker.Binding, error) {
	binding, err := e.registry.Bind(spec)
	if err != nil {
		return nil, err
	}

	if err := e.store.SaveBinding(ctx, *binding); err != nil {
		return nil, err
	}

	return binding, nil
}

func (e *Engine) Unbind(ctx context.Context, spec broker.BindingSpec) error {
	if err := e.registry.Unbind(spec); err != nil {
		return err
	}

	if err := e.store.DeleteBinding(ctx, spec); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}

	return nil
}

func (e *Engine) DeleteQueue(ctx context.Context, name string) error {
	if err := e.registry.DeleteQueue(name); err != nil {
		return err
	}

	if err := e.store.DeleteQueue(ctx, name); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}

	return nil
}

func (e *Engine) Publish(ctx context.Context, pub message.Publication) (*PublishResult, error) {
	queues, err := e.registry.Route(pub.Exchange, pub.RoutingKey)
	if err != nil {
		return nil, err
	}

	now := e.clock()
	messages := make([]*message.Message, 0, len(queues))
	result := &PublishResult{
		Exchange:   pub.Exchange,
		RoutingKey: pub.RoutingKey,
		MessageIDs: make([]string, 0, len(queues)),
		Queues:     make([]string, 0, len(queues)),
	}

	for _, queue := range queues {
		msg := message.New(pub, queue.Name, now)
		msg.MaxAttempts = queue.MaxAttempts

		if msg.Schema == "" {
			msg.Schema = queue.Schema
		}

		if err := msg.Transition(message.StateQueued, now); err != nil {
			return nil, err
		}

		messages = append(messages, msg)
		result.MessageIDs = append(result.MessageIDs, msg.ID)
		result.Queues = append(result.Queues, queue.Name)
	}

	if err := e.store.Append(ctx, messages); err != nil {
		return nil, err
	}

	for _, msg := range messages {
		e.recordHistory(ctx, storage.HistoryEvent{
			MessageID: msg.ID,
			Queue:     msg.Queue,
			From:      message.StateCreated,
			To:        message.StateQueued,
			At:        now,
		})
	}

	return result, nil
}

func (e *Engine) Message(ctx context.Context, id string) (*message.Message, error) {
	return e.store.Message(ctx, id)
}

func (e *Engine) History(ctx context.Context, id string) ([]storage.HistoryEvent, error) {
	return e.store.History(ctx, id)
}

func (e *Engine) Depth(ctx context.Context, queue string) (storage.Depth, error) {
	return e.store.Depth(ctx, queue)
}

func (e *Engine) Purge(ctx context.Context, queue string) (int, error) {
	return e.store.Purge(ctx, queue)
}

func (e *Engine) recordHistory(ctx context.Context, event storage.HistoryEvent) {
	if err := e.store.AppendHistory(ctx, event); err != nil {
		e.logger.Warn("failed to record message history",
			slog.String("message_id", event.MessageID),
			slog.String("error", err.Error()),
		)
	}
}
