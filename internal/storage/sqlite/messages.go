package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

const messageColumns = `id, queue, exchange, routing_key, schema_name, payload, headers,
	state, attempts, max_attempts, last_error, created_at, updated_at, available_at`

func (s *Store) Append(ctx context.Context, messages []*message.Message) error {
	if len(messages) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: append messages: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	statement, err := tx.PrepareContext(ctx,
		`INSERT INTO messages (`+messageColumns+`)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("sqlite: prepare append: %w", err)
	}
	defer statement.Close()

	for _, msg := range messages {
		headers, err := encodeHeaders(msg.Headers)
		if err != nil {
			return err
		}

		if _, err := statement.ExecContext(ctx,
			msg.ID, msg.Queue, msg.Exchange, msg.RoutingKey, msg.Schema, []byte(msg.Payload), headers,
			string(msg.State), msg.Attempts, msg.MaxAttempts, msg.LastError,
			formatTime(msg.CreatedAt), formatTime(msg.UpdatedAt), formatTime(msg.AvailableAt),
		); err != nil {
			if isUniqueViolation(err) {
				return storage.ErrConflict
			}

			return fmt.Errorf("sqlite: append message %s: %w", msg.ID, err)
		}
	}

	return tx.Commit()
}

func (s *Store) Message(ctx context.Context, id string) (*message.Message, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+messageColumns+` FROM messages WHERE id = ?`, id)

	msg, err := scanMessage(row)
	if err != nil {
		return nil, translateNoRows(err)
	}

	return msg, nil
}

func (s *Store) Depth(ctx context.Context, queue string) (storage.Depth, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM queues WHERE name = ?`, queue).Scan(&exists); err != nil {
		return storage.Depth{}, fmt.Errorf("sqlite: depth %s: %w", queue, err)
	}

	if exists == 0 {
		return storage.Depth{}, storage.ErrNotFound
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT state, COUNT(*) FROM messages WHERE queue = ? GROUP BY state`, queue)
	if err != nil {
		return storage.Depth{}, fmt.Errorf("sqlite: depth %s: %w", queue, err)
	}
	defer rows.Close()

	var depth storage.Depth

	for rows.Next() {
		var (
			state string
			count int
		)

		if err := rows.Scan(&state, &count); err != nil {
			return storage.Depth{}, fmt.Errorf("sqlite: scan depth: %w", err)
		}

		switch message.State(state) {
		case message.StateQueued:
			depth.Ready += count
		case message.StateProcessing:
			depth.InFlight += count
		case message.StateRetrying, message.StateFailed:
			depth.Scheduled += count
		}
	}

	if err := rows.Err(); err != nil {
		return storage.Depth{}, err
	}

	depth.Total = depth.Ready + depth.InFlight + depth.Scheduled

	return depth, nil
}

func (s *Store) Purge(ctx context.Context, queue string) (int, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM queues WHERE name = ?`, queue).Scan(&exists); err != nil {
		return 0, fmt.Errorf("sqlite: purge %s: %w", queue, err)
	}

	if exists == 0 {
		return 0, storage.ErrNotFound
	}

	result, err := s.db.ExecContext(ctx,
		`DELETE FROM messages WHERE queue = ? AND state NOT IN (?, ?)`,
		queue, string(message.StateAcknowledged), string(message.StateDeadLettered),
	)
	if err != nil {
		return 0, fmt.Errorf("sqlite: purge %s: %w", queue, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite: purge %s: %w", queue, err)
	}

	return int(affected), nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanMessage(row scanner) (*message.Message, error) {
	var (
		msg         message.Message
		state       string
		payload     []byte
		headers     string
		created     string
		updated     string
		availableAt string
	)

	if err := row.Scan(
		&msg.ID, &msg.Queue, &msg.Exchange, &msg.RoutingKey, &msg.Schema, &payload, &headers,
		&state, &msg.Attempts, &msg.MaxAttempts, &msg.LastError, &created, &updated, &availableAt,
	); err != nil {
		return nil, err
	}

	msg.State = message.State(state)
	msg.Payload = json.RawMessage(payload)

	decoded, err := decodeHeaders(headers)
	if err != nil {
		return nil, err
	}

	msg.Headers = decoded

	if msg.CreatedAt, err = parseTime(created); err != nil {
		return nil, err
	}

	if msg.UpdatedAt, err = parseTime(updated); err != nil {
		return nil, err
	}

	if msg.AvailableAt, err = parseTime(availableAt); err != nil {
		return nil, err
	}

	return &msg, nil
}

func scanMessages(rows *sql.Rows) ([]*message.Message, error) {
	defer rows.Close()

	messages := make([]*message.Message, 0)

	for rows.Next() {
		msg, err := scanMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: scan message: %w", err)
		}

		messages = append(messages, msg)
	}

	return messages, rows.Err()
}

func encodeHeaders(headers map[string]string) (string, error) {
	if len(headers) == 0 {
		return "{}", nil
	}

	encoded, err := json.Marshal(headers)
	if err != nil {
		return "", fmt.Errorf("sqlite: encode headers: %w", err)
	}

	return string(encoded), nil
}

func decodeHeaders(raw string) (map[string]string, error) {
	if raw == "" || raw == "{}" {
		return nil, nil
	}

	headers := make(map[string]string)
	if err := json.Unmarshal([]byte(raw), &headers); err != nil {
		return nil, fmt.Errorf("sqlite: decode headers: %w", err)
	}

	return headers, nil
}
