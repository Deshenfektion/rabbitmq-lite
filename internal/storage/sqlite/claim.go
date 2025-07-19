package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

func (s *Store) Claim(ctx context.Context, req storage.ClaimRequest) ([]*message.Message, error) {
	if req.Limit <= 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite: claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT `+messageColumns+` FROM messages
		 WHERE queue = ? AND state IN (?, ?) AND available_at <= ?
		 ORDER BY available_at, seq
		 LIMIT ?`,
		req.Queue, string(message.StateQueued), string(message.StateRetrying),
		formatTime(req.Now), req.Limit,
	)
	if err != nil {
		return nil, fmt.Errorf("sqlite: claim %s: %w", req.Queue, err)
	}

	candidates, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}

	leaseExpiry := req.Now.Add(req.Lease)
	claimed := make([]*message.Message, 0, len(candidates))

	for _, msg := range candidates {
		if msg.State == message.StateRetrying {
			if err := msg.Transition(message.StateQueued, req.Now); err != nil {
				return nil, err
			}
		}

		if err := msg.Transition(message.StateProcessing, req.Now); err != nil {
			return nil, err
		}

		msg.Attempts++

		if _, err := tx.ExecContext(ctx,
			`UPDATE messages
			 SET state = ?, attempts = ?, updated_at = ?, lease_expires_at = ?, consumer = ?
			 WHERE id = ?`,
			string(msg.State), msg.Attempts, formatTime(msg.UpdatedAt),
			formatTime(leaseExpiry), req.Consumer, msg.ID,
		); err != nil {
			return nil, fmt.Errorf("sqlite: lease message %s: %w", msg.ID, err)
		}

		claimed = append(claimed, msg)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: commit claim: %w", err)
	}

	return claimed, nil
}

func (s *Store) Acknowledge(ctx context.Context, id string, at time.Time) error {
	return s.transitionLeased(ctx, id, at, func(tx *sql.Tx, msg *message.Message) error {
		if err := msg.Transition(message.StateAcknowledged, at); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx,
			`UPDATE messages SET state = ?, updated_at = ?, lease_expires_at = NULL, consumer = '' WHERE id = ?`,
			string(msg.State), formatTime(msg.UpdatedAt), msg.ID,
		)

		return err
	})
}

func (s *Store) ScheduleRetry(ctx context.Context, req storage.RetryRequest) error {
	return s.transitionLeased(ctx, req.ID, req.Now, func(tx *sql.Tx, msg *message.Message) error {
		if err := msg.Transition(message.StateFailed, req.Now); err != nil {
			return err
		}

		if err := msg.Transition(message.StateRetrying, req.Now); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx,
			`UPDATE messages
			 SET state = ?, updated_at = ?, available_at = ?, last_error = ?,
			     lease_expires_at = NULL, consumer = ''
			 WHERE id = ?`,
			string(msg.State), formatTime(msg.UpdatedAt), formatTime(req.AvailableAt), req.Reason, msg.ID,
		)

		return err
	})
}

func (s *Store) Release(ctx context.Context, id string, at time.Time) error {
	return s.transitionLeased(ctx, id, at, func(tx *sql.Tx, msg *message.Message) error {
		if err := msg.Transition(message.StateQueued, at); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx,
			`UPDATE messages
			 SET state = ?, updated_at = ?, available_at = ?, lease_expires_at = NULL, consumer = ''
			 WHERE id = ?`,
			string(msg.State), formatTime(msg.UpdatedAt), formatTime(at), msg.ID,
		)

		return err
	})
}

func (s *Store) MarkDeadLettered(ctx context.Context, id string, reason string, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: dead letter %s: %w", id, err)
	}
	defer func() { _ = tx.Rollback() }()

	msg, err := loadMessage(ctx, tx, id)
	if err != nil {
		return err
	}

	if msg.State == message.StateProcessing {
		if err := msg.Transition(message.StateFailed, at); err != nil {
			return err
		}
	}

	if err := msg.Transition(message.StateDeadLettered, at); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE messages
		 SET state = ?, updated_at = ?, last_error = ?, lease_expires_at = NULL, consumer = ''
		 WHERE id = ?`,
		string(msg.State), formatTime(msg.UpdatedAt), reason, msg.ID,
	); err != nil {
		return fmt.Errorf("sqlite: dead letter %s: %w", id, err)
	}

	return tx.Commit()
}

func (s *Store) ExpiredLeases(ctx context.Context, now time.Time, limit int) ([]*message.Message, error) {
	if limit <= 0 {
		limit = -1
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT `+messageColumns+` FROM messages
		 WHERE state = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
		 ORDER BY seq
		 LIMIT ?`,
		string(message.StateProcessing), formatTime(now), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read expired leases: %w", err)
	}

	return scanMessages(rows)
}

func (s *Store) transitionLeased(ctx context.Context, id string, _ time.Time, apply func(*sql.Tx, *message.Message) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: update message %s: %w", id, err)
	}
	defer func() { _ = tx.Rollback() }()

	msg, err := loadMessage(ctx, tx, id)
	if err != nil {
		return err
	}

	if msg.State != message.StateProcessing {
		return &storage.StateError{
			MessageID: id,
			Expected:  message.StateProcessing,
			Actual:    msg.State,
		}
	}

	if err := apply(tx, msg); err != nil {
		return fmt.Errorf("sqlite: update message %s: %w", id, err)
	}

	return tx.Commit()
}

func loadMessage(ctx context.Context, tx *sql.Tx, id string) (*message.Message, error) {
	row := tx.QueryRowContext(ctx, `SELECT `+messageColumns+` FROM messages WHERE id = ?`, id)

	msg, err := scanMessage(row)
	if err != nil {
		return nil, translateNoRows(err)
	}

	return msg, nil
}
