package sqlite

import (
	"context"
	"fmt"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

func (s *Store) AppendHistory(ctx context.Context, event storage.HistoryEvent) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO message_history (message_id, queue, from_state, to_state, consumer, detail, at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		event.MessageID, event.Queue, string(event.From), string(event.To),
		event.Consumer, event.Detail, formatTime(event.At),
	)
	if err != nil {
		return fmt.Errorf("sqlite: append history for %s: %w", event.MessageID, err)
	}

	return nil
}

func (s *Store) History(ctx context.Context, messageID string) ([]storage.HistoryEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT message_id, queue, from_state, to_state, consumer, detail, at
		 FROM message_history WHERE message_id = ? ORDER BY id`, messageID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read history for %s: %w", messageID, err)
	}
	defer rows.Close()

	events := make([]storage.HistoryEvent, 0)

	for rows.Next() {
		var (
			event     storage.HistoryEvent
			fromState string
			toState   string
			at        string
		)

		if err := rows.Scan(&event.MessageID, &event.Queue, &fromState, &toState, &event.Consumer, &event.Detail, &at); err != nil {
			return nil, fmt.Errorf("sqlite: scan history: %w", err)
		}

		event.From = message.State(fromState)
		event.To = message.State(toState)

		if event.At, err = parseTime(at); err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	return events, rows.Err()
}
