package app

import (
	"context"
	"fmt"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/config"
	"github.com/deshenrao/rabbitmq-lite/internal/engine"
)

func declareTopology(ctx context.Context, instance *engine.Engine, cfg config.Config) error {
	for _, declaration := range cfg.Topology.Exchanges {
		kind, err := broker.ParseExchangeKind(declaration.Kind)
		if err != nil {
			return fmt.Errorf("app: exchange %s: %w", declaration.Name, err)
		}

		if _, err := instance.DeclareExchange(ctx, broker.ExchangeSpec{
			Name:    declaration.Name,
			Kind:    kind,
			Durable: declaration.Durable,
		}); err != nil {
			return fmt.Errorf("app: declare exchange %s: %w", declaration.Name, err)
		}
	}

	for _, declaration := range cfg.Topology.Queues {
		maxAttempts := declaration.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = cfg.Engine.MaxAttempts
		}

		visibility := declaration.VisibilityTimeout.Duration()
		if visibility == 0 {
			visibility = cfg.Engine.VisibilityTimeout.Duration()
		}

		if _, err := instance.DeclareQueue(ctx, broker.QueueSpec{
			Name:              declaration.Name,
			Durable:           declaration.Durable,
			MaxAttempts:       maxAttempts,
			VisibilityTimeout: visibility,
			Schema:            declaration.Schema,
		}); err != nil {
			return fmt.Errorf("app: declare queue %s: %w", declaration.Name, err)
		}
	}

	for _, declaration := range cfg.Topology.Bindings {
		if _, err := instance.Bind(ctx, broker.BindingSpec{
			Exchange:   declaration.Exchange,
			Queue:      declaration.Queue,
			RoutingKey: declaration.RoutingKey,
		}); err != nil {
			return fmt.Errorf("app: bind %s to %s: %w", declaration.Exchange, declaration.Queue, err)
		}
	}

	return nil
}
