package sqlite_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/sqlite"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/storagetest"
)

func openStore(t *testing.T, path string) *sqlite.Store {
	t.Helper()

	store, err := sqlite.Open(context.Background(), sqlite.Options{DSN: path})
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}

	return store
}

func TestSQLiteStoreConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storage.Store {
		return openStore(t, filepath.Join(t.TempDir(), "broker.db"))
	})
}

func TestMigrationsAreIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.db")

	first := openStore(t, path)
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second := openStore(t, path)
	t.Cleanup(func() { _ = second.Close() })

	var applied int
	if err := second.DB().QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("count migrations: %v", err)
	}

	if applied != 4 {
		t.Fatalf("expected 4 applied migrations, got %d", applied)
	}
}

func TestMessagesSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "broker.db")
	now := time.Date(2025, time.June, 24, 20, 0, 0, 0, time.UTC)

	store := openStore(t, path)

	queue := &broker.Queue{
		QueueSpec: broker.QueueSpec{
			Name:              "invoice-processing",
			Durable:           true,
			MaxAttempts:       3,
			VisibilityTimeout: 30 * time.Second,
		},
		CreatedAt: now,
	}

	if err := store.SaveQueue(ctx, queue); err != nil {
		t.Fatalf("save queue: %v", err)
	}

	msg := message.New(message.Publication{
		Exchange:   "erp.events",
		RoutingKey: "invoice.issued",
		Payload:    json.RawMessage(`{"invoice_id":"INV-42","total":199.95}`),
		Headers:    map[string]string{"tenant": "acme"},
	}, queue.Name, now)
	msg.MaxAttempts = 3

	if err := msg.Transition(message.StateQueued, now); err != nil {
		t.Fatalf("transition: %v", err)
	}

	if err := store.Append(ctx, []*message.Message{msg}); err != nil {
		t.Fatalf("append: %v", err)
	}

	claimed, err := store.Claim(ctx, storage.ClaimRequest{
		Queue: queue.Name, Consumer: "worker-1", Limit: 1, Lease: 30 * time.Second, Now: now,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(claimed) != 1 {
		t.Fatalf("expected one claimed message, got %d", len(claimed))
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openStore(t, path)
	t.Cleanup(func() { _ = reopened.Close() })

	restored, err := reopened.Message(ctx, msg.ID)
	if err != nil {
		t.Fatalf("message after reopen: %v", err)
	}

	if restored.State != message.StateProcessing || restored.Attempts != 1 {
		t.Fatalf("unexpected restored message state %s attempts %d", restored.State, restored.Attempts)
	}

	if string(restored.Payload) != `{"invoice_id":"INV-42","total":199.95}` {
		t.Fatalf("payload not durable: %s", restored.Payload)
	}

	reclaimed, err := reopened.ReclaimExpiredLeases(ctx, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	if len(reclaimed) != 1 {
		t.Fatalf("expected lease to be reclaimable after restart, got %d", len(reclaimed))
	}

	depth, err := reopened.Depth(ctx, queue.Name)
	if err != nil {
		t.Fatalf("depth: %v", err)
	}

	if depth.Ready != 1 {
		t.Fatalf("expected message to be ready again, got %+v", depth)
	}
}
