package storagetest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

type BenchFactory func(b *testing.B) storage.Store

const benchQueue = "invoice-processing"

func Benchmark(b *testing.B, factory BenchFactory) {
	b.Helper()

	b.Run("AppendSingle", func(b *testing.B) { benchmarkAppend(b, factory, 1) })
	b.Run("AppendBatch32", func(b *testing.B) { benchmarkAppend(b, factory, 32) })
	b.Run("Claim", func(b *testing.B) { benchmarkClaim(b, factory) })
	b.Run("DeliveryCycle", func(b *testing.B) { benchmarkDeliveryCycle(b, factory) })
}

func prepare(b *testing.B, factory BenchFactory) storage.Store {
	b.Helper()

	store := factory(b)

	queue := &broker.Queue{
		QueueSpec: broker.QueueSpec{
			Name:              benchQueue,
			Durable:           true,
			MaxAttempts:       3,
			VisibilityTimeout: 30 * time.Second,
		},
		CreatedAt: time.Now().UTC(),
	}

	if err := store.SaveQueue(context.Background(), queue); err != nil {
		b.Fatalf("save queue: %v", err)
	}

	b.Cleanup(func() { _ = store.Close() })

	return store
}

func benchMessages(count int) []*message.Message {
	messages := make([]*message.Message, 0, count)
	now := time.Now().UTC()

	for range count {
		msg := message.New(message.Publication{
			Exchange:   "erp.events",
			RoutingKey: "invoice.issued",
			Payload:    json.RawMessage(`{"invoice_id":"INV-1042210","total_net":1299.5}`),
			Headers:    map[string]string{"tenant": "acme"},
		}, benchQueue, now)

		msg.MaxAttempts = 3
		msg.State = message.StateQueued

		messages = append(messages, msg)
	}

	return messages
}

func benchmarkAppend(b *testing.B, factory BenchFactory, batch int) {
	store := prepare(b, factory)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := store.Append(ctx, benchMessages(batch)); err != nil {
			b.Fatalf("append: %v", err)
		}
	}

	b.ReportMetric(float64(batch), "messages/op")
}

func benchmarkClaim(b *testing.B, factory BenchFactory) {
	store := prepare(b, factory)
	ctx := context.Background()

	if err := store.Append(ctx, benchMessages(b.N+1)); err != nil {
		b.Fatalf("append: %v", err)
	}

	now := time.Now().UTC().Add(time.Second)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		claimed, err := store.Claim(ctx, storage.ClaimRequest{
			Queue: benchQueue, Consumer: "bench", Limit: 1, Lease: time.Minute, Now: now,
		})
		if err != nil {
			b.Fatalf("claim: %v", err)
		}

		if len(claimed) == 0 {
			b.Fatal("expected the queue to hold messages")
		}
	}
}

func benchmarkDeliveryCycle(b *testing.B, factory BenchFactory) {
	store := prepare(b, factory)
	ctx := context.Background()

	if err := store.Append(ctx, benchMessages(b.N+1)); err != nil {
		b.Fatalf("append: %v", err)
	}

	now := time.Now().UTC().Add(time.Second)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		claimed, err := store.Claim(ctx, storage.ClaimRequest{
			Queue: benchQueue, Consumer: "bench", Limit: 1, Lease: time.Minute, Now: now,
		})
		if err != nil {
			b.Fatalf("claim: %v", err)
		}

		if len(claimed) == 0 {
			b.Fatal("expected the queue to hold messages")
		}

		if err := store.Acknowledge(ctx, claimed[0].ID, now); err != nil {
			b.Fatalf("acknowledge: %v", err)
		}
	}
}
