package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/retry"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/memory"
)

func fastPolicy(maxAttempts int) retry.Policy {
	return retry.Policy{
		MaxAttempts:     maxAttempts,
		InitialInterval: 5 * time.Millisecond,
		MaxInterval:     20 * time.Millisecond,
		Multiplier:      2,
	}
}

func newTestEngine(t *testing.T, policy retry.Policy) *Engine {
	t.Helper()

	store := memory.New()

	instance, err := New(Options{
		Store:  store,
		Retry:  policy,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	instance.Start(context.Background())

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := instance.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}

		_ = store.Close()
	})

	return instance
}

func declareTopology(t *testing.T, instance *Engine, queues ...string) {
	t.Helper()

	ctx := context.Background()

	if _, err := instance.DeclareExchange(ctx, broker.ExchangeSpec{
		Name: "erp.events", Kind: broker.ExchangeTopic, Durable: true,
	}); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	for _, queue := range queues {
		if _, err := instance.DeclareQueue(ctx, broker.QueueSpec{
			Name: queue, Durable: true, MaxAttempts: 3, VisibilityTimeout: 2 * time.Second,
		}); err != nil {
			t.Fatalf("declare queue %s: %v", queue, err)
		}

		if _, err := instance.Bind(ctx, broker.BindingSpec{
			Exchange: "erp.events", Queue: queue, RoutingKey: "invoice.#",
		}); err != nil {
			t.Fatalf("bind %s: %v", queue, err)
		}
	}
}

func publishInvoice(t *testing.T, instance *Engine, id string) *PublishResult {
	t.Helper()

	result, err := instance.Publish(context.Background(), message.Publication{
		Exchange:   "erp.events",
		RoutingKey: "invoice.issued",
		Payload:    json.RawMessage(`{"invoice_id":"` + id + `"}`),
		Headers:    map[string]string{"tenant": "acme"},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	return result
}

func waitFor(t *testing.T, description string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", description)
}

func awaitState(t *testing.T, instance *Engine, id string, state message.State) *message.Message {
	t.Helper()

	var observed *message.Message

	waitFor(t, "message "+id+" to reach "+string(state), func() bool {
		msg, err := instance.Message(context.Background(), id)
		if err != nil {
			return false
		}

		observed = msg

		return msg.State == state
	})

	return observed
}

func TestPublishAndConsumeAcknowledgesMessage(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing")

	received := make(chan Delivery, 1)

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:        "invoice-worker",
		Queue:       "invoice-processing",
		Concurrency: 2,
		Prefetch:    4,
		Handler: HandlerFunc(func(_ context.Context, delivery Delivery) error {
			received <- delivery
			return nil
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	result := publishInvoice(t, instance, "INV-1")

	select {
	case delivery := <-received:
		if delivery.Queue != "invoice-processing" {
			t.Fatalf("unexpected queue %s", delivery.Queue)
		}

		if delivery.Attempt != 1 {
			t.Fatalf("expected first attempt, got %d", delivery.Attempt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was never invoked")
	}

	stored := awaitState(t, instance, result.MessageIDs[0], message.StateAcknowledged)

	history, err := instance.History(context.Background(), stored.ID)
	if err != nil {
		t.Fatalf("history: %v", err)
	}

	if len(history) != 3 {
		t.Fatalf("expected 3 lifecycle events, got %d", len(history))
	}
}

func TestTransientFailureIsRetriedUntilItSucceeds(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(5))
	declareTopology(t, instance, "invoice-processing")

	var attempts atomic.Int64

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:  "invoice-worker",
		Queue: "invoice-processing",
		Handler: HandlerFunc(func(_ context.Context, _ Delivery) error {
			if attempts.Add(1) < 3 {
				return errors.New("downstream unavailable")
			}

			return nil
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	result := publishInvoice(t, instance, "INV-2")

	awaitState(t, instance, result.MessageIDs[0], message.StateAcknowledged)

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 handler invocations, got %d", attempts.Load())
	}
}

func TestExhaustedRetriesEndInDeadLetterState(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing")

	var attempts atomic.Int64

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:  "invoice-worker",
		Queue: "invoice-processing",
		Handler: HandlerFunc(func(_ context.Context, _ Delivery) error {
			attempts.Add(1)
			return errors.New("downstream returned 500")
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	result := publishInvoice(t, instance, "INV-3")

	stored := awaitState(t, instance, result.MessageIDs[0], message.StateDeadLettered)

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts before dead lettering, got %d", attempts.Load())
	}

	if stored.LastError != "downstream returned 500" {
		t.Fatalf("expected failure reason to be retained, got %q", stored.LastError)
	}
}

func TestPermanentFailureSkipsRetries(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(5))
	declareTopology(t, instance, "invoice-processing")

	var attempts atomic.Int64

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:  "invoice-worker",
		Queue: "invoice-processing",
		Handler: HandlerFunc(func(_ context.Context, _ Delivery) error {
			attempts.Add(1)
			return Permanent(errors.New("payload rejected"))
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	result := publishInvoice(t, instance, "INV-4")

	awaitState(t, instance, result.MessageIDs[0], message.StateDeadLettered)

	if attempts.Load() != 1 {
		t.Fatalf("expected a single attempt, got %d", attempts.Load())
	}
}

func TestFanoutDeliversToEveryBoundQueue(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing", "audit-log")

	var (
		mu       sync.Mutex
		observed = map[string]int{}
	)

	for _, queue := range []string{"invoice-processing", "audit-log"} {
		if _, err := instance.Subscribe(ConsumerSpec{
			Name:  queue + "-worker",
			Queue: queue,
			Handler: HandlerFunc(func(_ context.Context, delivery Delivery) error {
				mu.Lock()
				observed[delivery.Queue]++
				mu.Unlock()

				return nil
			}),
		}); err != nil {
			t.Fatalf("subscribe %s: %v", queue, err)
		}
	}

	result := publishInvoice(t, instance, "INV-5")

	if len(result.MessageIDs) != 2 {
		t.Fatalf("expected 2 routed messages, got %d", len(result.MessageIDs))
	}

	waitFor(t, "both consumers to observe the message", func() bool {
		mu.Lock()
		defer mu.Unlock()

		return observed["invoice-processing"] == 1 && observed["audit-log"] == 1
	})
}

func TestConcurrentConsumersDrainBacklog(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing")

	const total = 200

	var processed atomic.Int64

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:        "invoice-worker",
		Queue:       "invoice-processing",
		Concurrency: 8,
		Prefetch:    16,
		Handler: HandlerFunc(func(_ context.Context, _ Delivery) error {
			processed.Add(1)
			return nil
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	for i := range total {
		publishInvoice(t, instance, "INV-BULK-"+string(rune('A'+i%26)))
	}

	waitFor(t, "backlog to drain", func() bool {
		if processed.Load() != total {
			return false
		}

		depth, err := instance.Depth(context.Background(), "invoice-processing")

		return err == nil && depth.Total == 0
	})
}

func TestSubscribeRejectsDuplicateConsumerNames(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing")

	spec := ConsumerSpec{
		Name:    "invoice-worker",
		Queue:   "invoice-processing",
		Handler: HandlerFunc(func(context.Context, Delivery) error { return nil }),
	}

	if _, err := instance.Subscribe(spec); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if _, err := instance.Subscribe(spec); !errors.Is(err, ErrConsumerExists) {
		t.Fatalf("expected ErrConsumerExists, got %v", err)
	}
}

func TestSubscribeRequiresKnownQueueAndHandler(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing")

	if _, err := instance.Subscribe(ConsumerSpec{Name: "a", Queue: "invoice-processing"}); !errors.Is(err, ErrHandlerRequired) {
		t.Fatalf("expected ErrHandlerRequired, got %v", err)
	}

	_, err := instance.Subscribe(ConsumerSpec{
		Name:    "b",
		Queue:   "unknown",
		Handler: HandlerFunc(func(context.Context, Delivery) error { return nil }),
	})
	if !errors.Is(err, broker.ErrQueueNotFound) {
		t.Fatalf("expected ErrQueueNotFound, got %v", err)
	}
}

func TestUnroutablePublicationIsRejected(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing")

	_, err := instance.Publish(context.Background(), message.Publication{
		Exchange:   "erp.events",
		RoutingKey: "customer.created",
		Payload:    json.RawMessage(`{}`),
	})
	if !errors.Is(err, broker.ErrUnroutable) {
		t.Fatalf("expected ErrUnroutable, got %v", err)
	}
}

func TestShutdownWaitsForInFlightDeliveries(t *testing.T) {
	store := memory.New()
	defer store.Close()

	instance, err := New(Options{Store: store, Retry: fastPolicy(3), Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	instance.Start(context.Background())
	declareTopology(t, instance, "invoice-processing")

	entered := make(chan struct{})
	var completed atomic.Bool

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:  "slow-worker",
		Queue: "invoice-processing",
		Handler: HandlerFunc(func(context.Context, Delivery) error {
			close(entered)
			time.Sleep(150 * time.Millisecond)
			completed.Store(true)

			return nil
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	result := publishInvoice(t, instance, "INV-6")

	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := instance.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if !completed.Load() {
		t.Fatal("shutdown returned before the in-flight delivery finished")
	}

	stored, err := store.Message(context.Background(), result.MessageIDs[0])
	if err != nil {
		t.Fatalf("message: %v", err)
	}

	if stored.State != message.StateAcknowledged {
		t.Fatalf("expected in-flight delivery to be acknowledged, got %s", stored.State)
	}
}

func TestExpiredLeasesAreRequeued(t *testing.T) {
	store := memory.New()
	defer store.Close()

	ctx := context.Background()

	instance, err := New(Options{Store: store, Retry: fastPolicy(3), Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	instance.Start(ctx)
	declareTopology(t, instance, "invoice-processing")

	result := publishInvoice(t, instance, "INV-7")
	now := time.Now().UTC()

	if _, err := store.Claim(ctx, storage.ClaimRequest{
		Queue: "invoice-processing", Consumer: "crashed", Limit: 1, Lease: time.Millisecond, Now: now,
	}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	time.Sleep(5 * time.Millisecond)

	expired, err := store.ExpiredLeases(ctx, time.Now().UTC(), 0)
	if err != nil {
		t.Fatalf("expired leases: %v", err)
	}

	if len(expired) != 1 || expired[0].ID != result.MessageIDs[0] {
		t.Fatalf("expected the abandoned message to be reported, got %+v", expired)
	}

	instance.reclaim(ctx)

	restored, err := store.Message(ctx, result.MessageIDs[0])
	if err != nil {
		t.Fatalf("message: %v", err)
	}

	if restored.State != message.StateQueued {
		t.Fatalf("expected the message to be requeued, got %s", restored.State)
	}
}
