package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

const deadLetterColumns = `id, message_id, queue, exchange, routing_key, schema_name, payload, headers,
	reason, error_kind, attempts, published_at, first_failed_at, dead_lettered_at, replayed_as, replay_count`

func (s *Store) SaveDeadLetter(ctx context.Context, entry *storage.DeadLetter) error {
	if entry.ID == "" {
		entry.ID = message.NewID()
	}

	headers, err := encodeHeaders(entry.Headers)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO dead_letters (`+deadLetterColumns+`)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.ID, entry.MessageID, entry.Queue, entry.Exchange, entry.RoutingKey, entry.Schema,
		[]byte(entry.Payload), headers, entry.Reason, entry.ErrorKind, entry.Attempts,
		formatTime(entry.PublishedAt), formatTime(entry.FirstFailedAt), formatTime(entry.DeadLetteredAt),
		entry.ReplayedAs, entry.ReplayedAtCount,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return storage.ErrConflict
		}

		return fmt.Errorf("sqlite: save dead letter %s: %w", entry.ID, err)
	}

	return nil
}

func (s *Store) DeadLetters(ctx context.Context, filter storage.DeadLetterFilter) ([]*storage.DeadLetter, error) {
	filter = filter.Normalise()

	query := strings.Builder{}
	query.WriteString(`SELECT ` + deadLetterColumns + ` FROM dead_letters`)

	args := make([]any, 0, 3)

	if filter.Queue != "" {
		query.WriteString(` WHERE queue = ?`)
		args = append(args, filter.Queue)
	}

	query.WriteString(` ORDER BY dead_lettered_at DESC, id DESC LIMIT ? OFFSET ?`)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list dead letters: %w", err)
	}
	defer rows.Close()

	entries := make([]*storage.DeadLetter, 0, filter.Limit)

	for rows.Next() {
		entry, err := scanDeadLetter(rows)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

func (s *Store) DeadLetter(ctx context.Context, id string) (*storage.DeadLetter, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+deadLetterColumns+` FROM dead_letters WHERE id = ?`, id)

	entry, err := scanDeadLetter(row)
	if err != nil {
		return nil, translateNoRows(err)
	}

	return entry, nil
}

func (s *Store) DeleteDeadLetter(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM dead_letters WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("sqlite: delete dead letter %s: %w", id, err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: delete dead letter %s: %w", id, err)
	}

	if affected == 0 {
		return storage.ErrNotFound
	}

	return nil
}

func (s *Store) CountDeadLetters(ctx context.Context, queue string) (int, error) {
	query := `SELECT COUNT(*) FROM dead_letters`
	args := make([]any, 0, 1)

	if queue != "" {
		query += ` WHERE queue = ?`
		args = append(args, queue)
	}

	var count int
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("sqlite: count dead letters: %w", err)
	}

	return count, nil
}

func scanDeadLetter(row scanner) (*storage.DeadLetter, error) {
	var (
		entry     storage.DeadLetter
		payload   []byte
		headers   string
		published string
		first     string
		dead      string
	)

	if err := row.Scan(
		&entry.ID, &entry.MessageID, &entry.Queue, &entry.Exchange, &entry.RoutingKey, &entry.Schema,
		&payload, &headers, &entry.Reason, &entry.ErrorKind, &entry.Attempts,
		&published, &first, &dead, &entry.ReplayedAs, &entry.ReplayedAtCount,
	); err != nil {
		return nil, err
	}

	entry.Payload = json.RawMessage(payload)

	decoded, err := decodeHeaders(headers)
	if err != nil {
		return nil, err
	}

	entry.Headers = decoded

	if entry.PublishedAt, err = parseTime(published); err != nil {
		return nil, err
	}

	if entry.FirstFailedAt, err = parseTime(first); err != nil {
		return nil, err
	}

	if entry.DeadLetteredAt, err = parseTime(dead); err != nil {
		return nil, err
	}

	return &entry, nil
}
