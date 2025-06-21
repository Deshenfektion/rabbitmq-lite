package sqlite

import (
	"context"
	"fmt"
	"time"
)

type migration struct {
	version    int
	name       string
	statements []string
}

var migrations = []migration{
	{
		version: 1,
		name:    "topology",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS exchanges (
				name       TEXT PRIMARY KEY,
				kind       TEXT NOT NULL,
				durable    INTEGER NOT NULL DEFAULT 0,
				created_at TEXT NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS queues (
				name                  TEXT PRIMARY KEY,
				durable               INTEGER NOT NULL DEFAULT 0,
				max_attempts          INTEGER NOT NULL,
				visibility_timeout_ms INTEGER NOT NULL,
				schema_name           TEXT NOT NULL DEFAULT '',
				created_at            TEXT NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS bindings (
				exchange    TEXT NOT NULL,
				queue       TEXT NOT NULL,
				routing_key TEXT NOT NULL,
				created_at  TEXT NOT NULL,
				PRIMARY KEY (exchange, queue, routing_key)
			)`,
		},
	},
	{
		version: 2,
		name:    "messages",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS messages (
				seq              INTEGER PRIMARY KEY AUTOINCREMENT,
				id               TEXT NOT NULL UNIQUE,
				queue            TEXT NOT NULL,
				exchange         TEXT NOT NULL DEFAULT '',
				routing_key      TEXT NOT NULL DEFAULT '',
				schema_name      TEXT NOT NULL DEFAULT '',
				payload          BLOB NOT NULL,
				headers          TEXT NOT NULL DEFAULT '{}',
				state            TEXT NOT NULL,
				attempts         INTEGER NOT NULL DEFAULT 0,
				max_attempts     INTEGER NOT NULL DEFAULT 0,
				last_error       TEXT NOT NULL DEFAULT '',
				created_at       TEXT NOT NULL,
				updated_at       TEXT NOT NULL,
				available_at     TEXT NOT NULL,
				lease_expires_at TEXT,
				consumer         TEXT NOT NULL DEFAULT ''
			)`,
			`CREATE INDEX IF NOT EXISTS idx_messages_ready
				ON messages (queue, state, available_at, seq)`,
			`CREATE INDEX IF NOT EXISTS idx_messages_lease
				ON messages (state, lease_expires_at)`,
		},
	},
	{
		version: 3,
		name:    "dead_letters",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS dead_letters (
				id               TEXT PRIMARY KEY,
				message_id       TEXT NOT NULL,
				queue            TEXT NOT NULL,
				exchange         TEXT NOT NULL DEFAULT '',
				routing_key      TEXT NOT NULL DEFAULT '',
				schema_name      TEXT NOT NULL DEFAULT '',
				payload          BLOB NOT NULL,
				headers          TEXT NOT NULL DEFAULT '{}',
				reason           TEXT NOT NULL,
				error_kind       TEXT NOT NULL DEFAULT '',
				attempts         INTEGER NOT NULL DEFAULT 0,
				published_at     TEXT NOT NULL,
				first_failed_at  TEXT NOT NULL,
				dead_lettered_at TEXT NOT NULL,
				replayed_as      TEXT NOT NULL DEFAULT '',
				replay_count     INTEGER NOT NULL DEFAULT 0
			)`,
			`CREATE INDEX IF NOT EXISTS idx_dead_letters_queue
				ON dead_letters (queue, dead_lettered_at)`,
		},
	},
	{
		version: 4,
		name:    "message_history",
		statements: []string{
			`CREATE TABLE IF NOT EXISTS message_history (
				id         INTEGER PRIMARY KEY AUTOINCREMENT,
				message_id TEXT NOT NULL,
				queue      TEXT NOT NULL DEFAULT '',
				from_state TEXT NOT NULL,
				to_state   TEXT NOT NULL,
				consumer   TEXT NOT NULL DEFAULT '',
				detail     TEXT NOT NULL DEFAULT '',
				at         TEXT NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS idx_message_history_message
				ON message_history (message_id, id)`,
		},
	},
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INTEGER PRIMARY KEY,
		name       TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("sqlite: create migration table: %w", err)
	}

	applied, err := s.appliedMigrations(ctx)
	if err != nil {
		return err
	}

	for _, step := range migrations {
		if _, done := applied[step.version]; done {
			continue
		}

		if err := s.applyMigration(ctx, step); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) appliedMigrations(ctx context.Context) (map[int]struct{}, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]struct{})

	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("sqlite: scan migration: %w", err)
		}

		applied[version] = struct{}{}
	}

	return applied, rows.Err()
}

func (s *Store) applyMigration(ctx context.Context, step migration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin migration %d: %w", step.version, err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, statement := range step.statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("sqlite: migration %d (%s): %w", step.version, step.name, err)
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`,
		step.version, step.name, formatTime(time.Now()),
	); err != nil {
		return fmt.Errorf("sqlite: record migration %d: %w", step.version, err)
	}

	return tx.Commit()
}
