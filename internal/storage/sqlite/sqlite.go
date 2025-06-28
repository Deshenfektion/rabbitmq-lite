package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/storage"

	_ "modernc.org/sqlite"
)

const timeLayout = "2006-01-02T15:04:05.000000000Z"

type Options struct {
	DSN          string
	BusyTimeout  time.Duration
	JournalMode  string
	Synchronous  string
	MaxOpenConns int
}

type Store struct {
	db *sql.DB
}

func (o Options) withDefaults() Options {
	if o.DSN == "" {
		o.DSN = ":memory:"
	}

	if o.BusyTimeout == 0 {
		o.BusyTimeout = 5 * time.Second
	}

	if o.JournalMode == "" {
		o.JournalMode = "WAL"
	}

	if o.Synchronous == "" {
		o.Synchronous = "NORMAL"
	}

	if o.MaxOpenConns == 0 {
		o.MaxOpenConns = 1
	}

	return o
}

func Open(ctx context.Context, opts Options) (*Store, error) {
	opts = opts.withDefaults()

	db, err := sql.Open("sqlite", dataSourceName(opts))
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}

	db.SetMaxOpenConns(opts.MaxOpenConns)
	db.SetMaxIdleConns(opts.MaxOpenConns)
	db.SetConnMaxLifetime(0)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: ping: %w", err)
	}

	store := &Store{db: db}

	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func dataSourceName(opts Options) string {
	params := []string{
		fmt.Sprintf("_pragma=busy_timeout(%d)", opts.BusyTimeout.Milliseconds()),
		fmt.Sprintf("_pragma=journal_mode(%s)", opts.JournalMode),
		fmt.Sprintf("_pragma=synchronous(%s)", opts.Synchronous),
		"_pragma=foreign_keys(1)",
	}

	separator := "?"
	if strings.Contains(opts.DSN, "?") {
		separator = "&"
	}

	return opts.DSN + separator + strings.Join(params, "&")
}

func formatTime(value time.Time) string {
	return value.UTC().Format(timeLayout)
}

func parseTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}

	parsed, err := time.Parse(timeLayout, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("sqlite: parse timestamp %q: %w", value, err)
	}

	return parsed, nil
}

var _ storage.Store = (*Store)(nil)
