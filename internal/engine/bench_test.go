package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/retry"
	"github.com/deshenrao/rabbitmq-lite/internal/schema"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/memory"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/sqlite"
)

const invoicePayload = `{"invoice_id":"INV-1042210","customer_id":"C-100244","currency":"EUR",` +
	`"issued_on":"2025-08-17","lines":[{"position":1,"sku":"PALLET-EUR","quantity":12,` +
	`"unit_price_net":18.5,"tax_rate":0.19}]}`

const benchmarkSchema = `{
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"type": "object",
	"required": ["invoice_id", "customer_id", "currency", "issued_on", "lines"],
	"properties": {
		"invoice_id": {"type": "string", "pattern": "^INV-[0-9]{4,10}$"},
		"customer_id": {"type": "string"},
		"currency": {"type": "string", "enum": ["EUR", "USD", "CHF"]},
		"issued_on": {"type": "string"},
		"lines": {
			"type": "array",
			"minItems": 1,
			"items": {
				"type": "object",
				"required": ["position", "sku", "quantity", "unit_price_net"],
				"properties": {
					"position": {"type": "integer"},
					"sku": {"type": "string"},
					"quantity": {"type": "number"},
					"unit_price_net": {"type": "number"},
					"tax_rate": {"type": "number"}
				}
			}
		}
	}
}`

type benchOptions struct {
	store       storage.Store
	queues      []string
	schemas     *schema.Registry
	queueSchema string
	maxAttempts int
}

func newBenchEngine(b *testing.B, opts benchOptions) *Engine {
	b.Helper()

	if opts.maxAttempts == 0 {
		opts.maxAttempts = 3
	}

	instance, err := New(Options{
		Store:   opts.store,
		Schemas: opts.schemas,
		Logger:  slog.New(slog.DiscardHandler),
		Retry: retry.Policy{
			MaxAttempts:     opts.maxAttempts,
			InitialInterval: time.Millisecond,
			MaxInterval:     5 * time.Millisecond,
			Multiplier:      2,
		},
		ReclaimInterval: time.Hour,
	})
	if err != nil {
		b.Fatalf("new engine: %v", err)
	}

	instance.Start(context.Background())

	ctx := context.Background()

	if _, err := instance.DeclareExchange(ctx, broker.ExchangeSpec{
		Name: "erp.events", Kind: broker.ExchangeTopic, Durable: true,
	}); err != nil {
		b.Fatalf("declare exchange: %v", err)
	}

	for _, queue := range opts.queues {
		if _, err := instance.DeclareQueue(ctx, broker.QueueSpec{
			Name: queue, Durable: true, MaxAttempts: opts.maxAttempts,
			VisibilityTimeout: 30 * time.Second, Schema: opts.queueSchema,
		}); err != nil {
			b.Fatalf("declare queue: %v", err)
		}

		if _, err := instance.Bind(ctx, broker.BindingSpec{
			Exchange: "erp.events", Queue: queue, RoutingKey: "invoice.#",
		}); err != nil {
			b.Fatalf("bind: %v", err)
		}
	}

	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_ = instance.Shutdown(ctx)
		_ = opts.store.Close()
	})

	return instance
}

func openBenchSQLite(b *testing.B) storage.Store {
	b.Helper()

	store, err := sqlite.Open(context.Background(), sqlite.Options{
		DSN: filepath.Join(b.TempDir(), "bench.db"),
	})
	if err != nil {
		b.Fatalf("open sqlite: %v", err)
	}

	return store
}

func benchPublication() message.Publication {
	return message.Publication{
		Exchange:   "erp.events",
		RoutingKey: "invoice.issued",
		Payload:    json.RawMessage(invoicePayload),
		Headers:    map[string]string{"tenant": "acme", "erp-module": "FI"},
	}
}

func reportLatencies(b *testing.B, samples []time.Duration) {
	b.Helper()

	if len(samples) == 0 {
		return
	}

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })

	quantile := func(q float64) float64 {
		index := int(q * float64(len(samples)))
		if index >= len(samples) {
			index = len(samples) - 1
		}

		return float64(samples[index].Nanoseconds()) / 1e6
	}

	b.ReportMetric(quantile(0.50), "p50_ms")
	b.ReportMetric(quantile(0.95), "p95_ms")
	b.ReportMetric(quantile(0.99), "p99_ms")
	b.ReportMetric(float64(samples[len(samples)-1].Nanoseconds())/1e6, "max_ms")
}

func BenchmarkPublishMemory(b *testing.B) {
	instance := newBenchEngine(b, benchOptions{store: memory.New(), queues: []string{"invoice-processing"}})
	ctx := context.Background()
	publication := benchPublication()

	samples := make([]time.Duration, 0, 4096)

	b.ReportAllocs()

	for b.Loop() {
		started := time.Now()

		if _, err := instance.Publish(ctx, publication); err != nil {
			b.Fatalf("publish: %v", err)
		}

		samples = append(samples, time.Since(started))
	}

	reportLatencies(b, samples)
}

func BenchmarkPublishSQLite(b *testing.B) {
	instance := newBenchEngine(b, benchOptions{store: openBenchSQLite(b), queues: []string{"invoice-processing"}})
	ctx := context.Background()
	publication := benchPublication()

	samples := make([]time.Duration, 0, 4096)

	b.ReportAllocs()

	for b.Loop() {
		started := time.Now()

		if _, err := instance.Publish(ctx, publication); err != nil {
			b.Fatalf("publish: %v", err)
		}

		samples = append(samples, time.Since(started))
	}

	reportLatencies(b, samples)
}

func BenchmarkPublishFanout(b *testing.B) {
	instance := newBenchEngine(b, benchOptions{
		store:  memory.New(),
		queues: []string{"invoice-processing", "audit-log", "analytics"},
	})

	ctx := context.Background()
	publication := benchPublication()

	b.ReportAllocs()

	for b.Loop() {
		if _, err := instance.Publish(ctx, publication); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
}

func BenchmarkPublishWithSchemaValidation(b *testing.B) {
	registry := schema.NewRegistry()
	if _, err := registry.Register("invoice", []byte(benchmarkSchema)); err != nil {
		b.Fatalf("register schema: %v", err)
	}

	instance := newBenchEngine(b, benchOptions{
		store:       memory.New(),
		queues:      []string{"invoice-processing"},
		schemas:     registry,
		queueSchema: "invoice",
	})

	ctx := context.Background()
	publication := benchPublication()

	samples := make([]time.Duration, 0, 4096)

	b.ReportAllocs()

	for b.Loop() {
		started := time.Now()

		if _, err := instance.Publish(ctx, publication); err != nil {
			b.Fatalf("publish: %v", err)
		}

		samples = append(samples, time.Since(started))
	}

	reportLatencies(b, samples)
}

func BenchmarkClaimBatch(b *testing.B) {
	for _, batch := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("prefetch-%d", batch), func(b *testing.B) {
			store := memory.New()
			instance := newBenchEngine(b, benchOptions{store: store, queues: []string{"invoice-processing"}})
			ctx := context.Background()
			publication := benchPublication()

			for range b.N * batch {
				if _, err := instance.Publish(ctx, publication); err != nil {
					b.Fatalf("publish: %v", err)
				}
			}

			b.ResetTimer()

			for range b.N {
				claimed, err := store.Claim(ctx, storage.ClaimRequest{
					Queue: "invoice-processing", Consumer: "bench", Limit: batch,
					Lease: time.Minute, Now: time.Now().UTC(),
				})
				if err != nil {
					b.Fatalf("claim: %v", err)
				}

				if len(claimed) == 0 {
					b.Fatal("expected the queue to hold messages")
				}
			}
		})
	}
}

func BenchmarkEndToEndDelivery(b *testing.B) {
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			instance := newBenchEngine(b, benchOptions{store: memory.New(), queues: []string{"invoice-processing"}})

			var (
				mu        sync.Mutex
				remaining int
				drained   = make(chan struct{}, 1)
			)

			if _, err := instance.Subscribe(ConsumerSpec{
				Name:        "bench-worker",
				Queue:       "invoice-processing",
				Concurrency: workers,
				Prefetch:    workers * 4,
				Handler: HandlerFunc(func(context.Context, Delivery) error {
					mu.Lock()
					remaining--
					finished := remaining == 0
					mu.Unlock()

					if finished {
						select {
						case drained <- struct{}{}:
						default:
						}
					}

					return nil
				}),
			}); err != nil {
				b.Fatalf("subscribe: %v", err)
			}

			ctx := context.Background()
			publication := benchPublication()

			mu.Lock()
			remaining = b.N
			mu.Unlock()

			b.ResetTimer()

			for range b.N {
				if _, err := instance.Publish(ctx, publication); err != nil {
					b.Fatalf("publish: %v", err)
				}
			}

			select {
			case <-drained:
			case <-time.After(60 * time.Second):
				b.Fatal("timed out waiting for deliveries to drain")
			}
		})
	}
}

func BenchmarkRetryOverhead(b *testing.B) {
	for _, failures := range []int{0, 1, 2} {
		b.Run(fmt.Sprintf("failures-%d", failures), func(b *testing.B) {
			instance := newBenchEngine(b, benchOptions{
				store:       memory.New(),
				queues:      []string{"invoice-processing"},
				maxAttempts: failures + 1,
			})

			var (
				attempts  sync.Map
				completed atomic.Int64
				target    = int64(b.N)
				drained   = make(chan struct{}, 1)
			)

			if _, err := instance.Subscribe(ConsumerSpec{
				Name:        "bench-worker",
				Queue:       "invoice-processing",
				Concurrency: 4,
				Prefetch:    16,
				Handler: HandlerFunc(func(_ context.Context, delivery Delivery) error {
					seen, _ := attempts.LoadOrStore(delivery.Message.ID, new(atomic.Int64))
					counter := seen.(*atomic.Int64)

					if counter.Add(1) <= int64(failures) {
						return errors.New("simulated downstream failure")
					}

					if completed.Add(1) == target {
						select {
						case drained <- struct{}{}:
						default:
						}
					}

					return nil
				}),
			}); err != nil {
				b.Fatalf("subscribe: %v", err)
			}

			ctx := context.Background()
			publication := benchPublication()

			b.ResetTimer()

			for range b.N {
				if _, err := instance.Publish(ctx, publication); err != nil {
					b.Fatalf("publish: %v", err)
				}
			}

			select {
			case <-drained:
			case <-time.After(120 * time.Second):
				b.Fatalf("timed out after %d of %d completions", completed.Load(), target)
			}
		})
	}
}
