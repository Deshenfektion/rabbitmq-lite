package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

func (s *Store) SaveExchange(ctx context.Context, exchange *broker.Exchange) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO exchanges (name, kind, durable, created_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET kind = excluded.kind, durable = excluded.durable`,
		exchange.Name, string(exchange.Kind), exchange.Durable, formatTime(exchange.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("sqlite: save exchange %s: %w", exchange.Name, err)
	}

	return nil
}

func (s *Store) Exchanges(ctx context.Context) ([]*broker.Exchange, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, kind, durable, created_at FROM exchanges ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list exchanges: %w", err)
	}
	defer rows.Close()

	exchanges := make([]*broker.Exchange, 0)

	for rows.Next() {
		var (
			exchange broker.Exchange
			kind     string
			created  string
		)

		if err := rows.Scan(&exchange.Name, &kind, &exchange.Durable, &created); err != nil {
			return nil, fmt.Errorf("sqlite: scan exchange: %w", err)
		}

		exchange.Kind = broker.ExchangeKind(kind)

		if exchange.CreatedAt, err = parseTime(created); err != nil {
			return nil, err
		}

		exchanges = append(exchanges, &exchange)
	}

	return exchanges, rows.Err()
}

func (s *Store) SaveQueue(ctx context.Context, queue *broker.Queue) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO queues (name, durable, max_attempts, visibility_timeout_ms, schema_name, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET
			durable = excluded.durable,
			max_attempts = excluded.max_attempts,
			visibility_timeout_ms = excluded.visibility_timeout_ms,
			schema_name = excluded.schema_name`,
		queue.Name, queue.Durable, queue.MaxAttempts,
		queue.VisibilityTimeout.Milliseconds(), queue.Schema, formatTime(queue.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("sqlite: save queue %s: %w", queue.Name, err)
	}

	return nil
}

func (s *Store) Queues(ctx context.Context) ([]*broker.Queue, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, durable, max_attempts, visibility_timeout_ms, schema_name, created_at
		 FROM queues ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list queues: %w", err)
	}
	defer rows.Close()

	queues := make([]*broker.Queue, 0)

	for rows.Next() {
		var (
			queue     broker.Queue
			timeoutMS int64
			created   string
		)

		if err := rows.Scan(&queue.Name, &queue.Durable, &queue.MaxAttempts, &timeoutMS, &queue.Schema, &created); err != nil {
			return nil, fmt.Errorf("sqlite: scan queue: %w", err)
		}

		queue.VisibilityTimeout = time.Duration(timeoutMS) * time.Millisecond

		if queue.CreatedAt, err = parseTime(created); err != nil {
			return nil, err
		}

		queues = append(queues, &queue)
	}

	return queues, rows.Err()
}

func (s *Store) DeleteQueue(ctx context.Context, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: delete queue %s: %w", name, err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, `DELETE FROM queues WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("sqlite: delete queue %s: %w", name, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: delete queue %s: %w", name, err)
	}

	if affected == 0 {
		return storage.ErrNotFound
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE queue = ?`, name); err != nil {
		return fmt.Errorf("sqlite: delete queue messages %s: %w", name, err)
	}

	return tx.Commit()
}

func (s *Store) SaveBinding(ctx context.Context, binding broker.Binding) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO bindings (exchange, queue, routing_key, created_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(exchange, queue, routing_key) DO NOTHING`,
		binding.Exchange, binding.Queue, binding.RoutingKey, formatTime(binding.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("sqlite: save binding: %w", err)
	}

	return nil
}

func (s *Store) Bindings(ctx context.Context) ([]broker.Binding, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT exchange, queue, routing_key, created_at FROM bindings
		 ORDER BY exchange, queue, routing_key`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list bindings: %w", err)
	}
	defer rows.Close()

	bindings := make([]broker.Binding, 0)

	for rows.Next() {
		var (
			binding broker.Binding
			created string
		)

		if err := rows.Scan(&binding.Exchange, &binding.Queue, &binding.RoutingKey, &created); err != nil {
			return nil, fmt.Errorf("sqlite: scan binding: %w", err)
		}

		if binding.CreatedAt, err = parseTime(created); err != nil {
			return nil, err
		}

		bindings = append(bindings, binding)
	}

	return bindings, rows.Err()
}

func (s *Store) DeleteBinding(ctx context.Context, spec broker.BindingSpec) error {
	result, err := s.db.ExecContext(ctx,
		`DELETE FROM bindings WHERE exchange = ? AND queue = ? AND routing_key = ?`,
		spec.Exchange, spec.Queue, spec.RoutingKey,
	)
	if err != nil {
		return fmt.Errorf("sqlite: delete binding: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: delete binding: %w", err)
	}

	if affected == 0 {
		return storage.ErrNotFound
	}

	return nil
}

func translateNoRows(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	}

	return err
}
