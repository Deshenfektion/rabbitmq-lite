package storagetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

type Factory func(t *testing.T) storage.Store

var base = time.Date(2025, time.June, 18, 9, 0, 0, 0, time.UTC)

func Run(t *testing.T, factory Factory) {
	t.Helper()

	tests := map[string]func(*testing.T, storage.Store){
		"TopologyRoundTrip":            testTopologyRoundTrip,
		"AppendAndFetch":               testAppendAndFetch,
		"AppendRejectsDuplicates":      testAppendRejectsDuplicates,
		"ClaimRespectsOrderAndLimit":   testClaimRespectsOrderAndLimit,
		"ClaimSkipsFutureAvailability": testClaimSkipsFutureAvailability,
		"ClaimIsExclusive":             testClaimIsExclusive,
		"Acknowledge":                  testAcknowledge,
		"AcknowledgeRequiresLease":     testAcknowledgeRequiresLease,
		"ScheduleRetry":                testScheduleRetry,
		"Release":                      testRelease,
		"MarkDeadLettered":             testMarkDeadLettered,
		"ReclaimExpiredLeases":         testReclaimExpiredLeases,
		"Depth":                        testDepth,
		"Purge":                        testPurge,
		"DeadLetterLifecycle":          testDeadLetterLifecycle,
		"History":                      testHistory,
	}

	names := make([]string, 0, len(tests))
	for name := range tests {
		names = append(names, name)
	}

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			store := factory(t)
			t.Cleanup(func() { _ = store.Close() })
			tests[name](t, store)
		})
	}
}

func seedQueue(t *testing.T, store storage.Store, name string) *broker.Queue {
	t.Helper()

	queue := &broker.Queue{
		QueueSpec: broker.QueueSpec{
			Name:              name,
			Durable:           true,
			MaxAttempts:       3,
			VisibilityTimeout: 30 * time.Second,
		},
		CreatedAt: base,
	}

	if err := store.SaveQueue(context.Background(), queue); err != nil {
		t.Fatalf("save queue: %v", err)
	}

	return queue
}

func seedMessages(t *testing.T, store storage.Store, queue string, count int) []*message.Message {
	t.Helper()

	messages := make([]*message.Message, 0, count)

	for i := range count {
		msg := message.New(message.Publication{
			Exchange:   "erp.events",
			RoutingKey: "invoice.issued",
			Payload:    json.RawMessage(`{"invoice_id":"INV-1"}`),
			Headers:    map[string]string{"tenant": "acme"},
		}, queue, base.Add(time.Duration(i)*time.Millisecond))

		msg.MaxAttempts = 3
		msg.AvailableAt = base

		if err := msg.Transition(message.StateQueued, base); err != nil {
			t.Fatalf("transition: %v", err)
		}

		messages = append(messages, msg)
	}

	if err := store.Append(context.Background(), messages); err != nil {
		t.Fatalf("append: %v", err)
	}

	return messages
}

func claimOne(t *testing.T, store storage.Store, queue string, now time.Time) *message.Message {
	t.Helper()

	claimed, err := store.Claim(context.Background(), storage.ClaimRequest{
		Queue:    queue,
		Consumer: "worker-1",
		Limit:    1,
		Lease:    30 * time.Second,
		Now:      now,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(claimed) != 1 {
		t.Fatalf("expected one claimed message, got %d", len(claimed))
	}

	return claimed[0]
}

func testTopologyRoundTrip(t *testing.T, store storage.Store) {
	ctx := context.Background()

	exchange := &broker.Exchange{
		ExchangeSpec: broker.ExchangeSpec{Name: "erp.events", Kind: broker.ExchangeTopic, Durable: true},
		CreatedAt:    base,
	}

	if err := store.SaveExchange(ctx, exchange); err != nil {
		t.Fatalf("save exchange: %v", err)
	}

	queue := seedQueue(t, store, "invoice-processing")

	binding := broker.Binding{
		BindingSpec: broker.BindingSpec{Exchange: "erp.events", Queue: queue.Name, RoutingKey: "invoice.*"},
		CreatedAt:   base,
	}

	if err := store.SaveBinding(ctx, binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}

	exchanges, err := store.Exchanges(ctx)
	if err != nil {
		t.Fatalf("exchanges: %v", err)
	}

	if len(exchanges) != 1 || exchanges[0].Kind != broker.ExchangeTopic {
		t.Fatalf("unexpected exchanges %+v", exchanges)
	}

	queues, err := store.Queues(ctx)
	if err != nil {
		t.Fatalf("queues: %v", err)
	}

	if len(queues) != 1 || queues[0].VisibilityTimeout != 30*time.Second {
		t.Fatalf("unexpected queues %+v", queues)
	}

	bindings, err := store.Bindings(ctx)
	if err != nil {
		t.Fatalf("bindings: %v", err)
	}

	if len(bindings) != 1 || bindings[0].RoutingKey != "invoice.*" {
		t.Fatalf("unexpected bindings %+v", bindings)
	}

	if err := store.DeleteBinding(ctx, binding.BindingSpec); err != nil {
		t.Fatalf("delete binding: %v", err)
	}

	if err := store.DeleteBinding(ctx, binding.BindingSpec); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func testAppendAndFetch(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seeded := seedMessages(t, store, "invoice-processing", 1)

	fetched, err := store.Message(context.Background(), seeded[0].ID)
	if err != nil {
		t.Fatalf("message: %v", err)
	}

	if fetched.State != message.StateQueued {
		t.Fatalf("expected QUEUED, got %s", fetched.State)
	}

	if string(fetched.Payload) != `{"invoice_id":"INV-1"}` {
		t.Fatalf("payload not preserved: %s", fetched.Payload)
	}

	if fetched.Headers["tenant"] != "acme" {
		t.Fatalf("headers not preserved: %+v", fetched.Headers)
	}

	if !fetched.CreatedAt.Equal(seeded[0].CreatedAt) {
		t.Fatalf("timestamp drift: %s != %s", fetched.CreatedAt, seeded[0].CreatedAt)
	}

	if _, err := store.Message(context.Background(), "does-not-exist"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func testAppendRejectsDuplicates(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seeded := seedMessages(t, store, "invoice-processing", 1)

	if err := store.Append(context.Background(), seeded); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func testClaimRespectsOrderAndLimit(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seeded := seedMessages(t, store, "invoice-processing", 5)

	claimed, err := store.Claim(context.Background(), storage.ClaimRequest{
		Queue:    "invoice-processing",
		Consumer: "worker-1",
		Limit:    2,
		Lease:    time.Minute,
		Now:      base,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(claimed) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(claimed))
	}

	if claimed[0].ID != seeded[0].ID || claimed[1].ID != seeded[1].ID {
		t.Fatal("expected first in first out delivery order")
	}

	for _, msg := range claimed {
		if msg.State != message.StateProcessing {
			t.Fatalf("expected PROCESSING, got %s", msg.State)
		}

		if msg.Attempts != 1 {
			t.Fatalf("expected attempt counter 1, got %d", msg.Attempts)
		}
	}
}

func testClaimSkipsFutureAvailability(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seeded := seedMessages(t, store, "invoice-processing", 1)

	claimed := claimOne(t, store, "invoice-processing", base)

	if err := store.ScheduleRetry(context.Background(), storage.RetryRequest{
		ID:          claimed.ID,
		Attempt:     1,
		Reason:      "downstream timeout",
		AvailableAt: base.Add(time.Minute),
		Now:         base,
	}); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}

	early, err := store.Claim(context.Background(), storage.ClaimRequest{
		Queue: "invoice-processing", Limit: 10, Lease: time.Minute, Now: base.Add(30 * time.Second),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(early) != 0 {
		t.Fatalf("expected no messages before backoff elapsed, got %d", len(early))
	}

	late, err := store.Claim(context.Background(), storage.ClaimRequest{
		Queue: "invoice-processing", Limit: 10, Lease: time.Minute, Now: base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(late) != 1 || late[0].ID != seeded[0].ID {
		t.Fatalf("expected retried message to become claimable, got %d", len(late))
	}

	if late[0].Attempts != 2 {
		t.Fatalf("expected attempt counter 2, got %d", late[0].Attempts)
	}
}

func testClaimIsExclusive(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seedMessages(t, store, "invoice-processing", 1)

	claimOne(t, store, "invoice-processing", base)

	second, err := store.Claim(context.Background(), storage.ClaimRequest{
		Queue: "invoice-processing", Limit: 10, Lease: time.Minute, Now: base,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(second) != 0 {
		t.Fatalf("expected leased message to be invisible, got %d", len(second))
	}
}

func testAcknowledge(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seedMessages(t, store, "invoice-processing", 1)

	claimed := claimOne(t, store, "invoice-processing", base)

	if err := store.Acknowledge(context.Background(), claimed.ID, base.Add(time.Second)); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}

	stored, err := store.Message(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("message: %v", err)
	}

	if stored.State != message.StateAcknowledged {
		t.Fatalf("expected ACKNOWLEDGED, got %s", stored.State)
	}
}

func testAcknowledgeRequiresLease(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seeded := seedMessages(t, store, "invoice-processing", 1)

	err := store.Acknowledge(context.Background(), seeded[0].ID, base)

	var stateErr *storage.StateError
	if !errors.As(err, &stateErr) {
		t.Fatalf("expected StateError, got %v", err)
	}
}

func testScheduleRetry(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seedMessages(t, store, "invoice-processing", 1)

	claimed := claimOne(t, store, "invoice-processing", base)

	if err := store.ScheduleRetry(context.Background(), storage.RetryRequest{
		ID:          claimed.ID,
		Attempt:     1,
		Reason:      "connection refused",
		AvailableAt: base.Add(2 * time.Second),
		Now:         base.Add(time.Second),
	}); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}

	stored, err := store.Message(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("message: %v", err)
	}

	if stored.State != message.StateRetrying {
		t.Fatalf("expected RETRYING, got %s", stored.State)
	}

	if stored.LastError != "connection refused" {
		t.Fatalf("expected failure reason to be recorded, got %q", stored.LastError)
	}

	if !stored.AvailableAt.Equal(base.Add(2 * time.Second)) {
		t.Fatalf("unexpected availability %s", stored.AvailableAt)
	}
}

func testRelease(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seedMessages(t, store, "invoice-processing", 1)

	claimed := claimOne(t, store, "invoice-processing", base)

	if err := store.Release(context.Background(), claimed.ID, base.Add(time.Second)); err != nil {
		t.Fatalf("release: %v", err)
	}

	again, err := store.Claim(context.Background(), storage.ClaimRequest{
		Queue: "invoice-processing", Limit: 1, Lease: time.Minute, Now: base.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(again) != 1 {
		t.Fatal("expected released message to be claimable again")
	}
}

func testMarkDeadLettered(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seedMessages(t, store, "invoice-processing", 1)

	claimed := claimOne(t, store, "invoice-processing", base)

	if err := store.MarkDeadLettered(context.Background(), claimed.ID, "retries exhausted", base.Add(time.Second)); err != nil {
		t.Fatalf("mark dead lettered: %v", err)
	}

	stored, err := store.Message(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("message: %v", err)
	}

	if stored.State != message.StateDeadLettered {
		t.Fatalf("expected DEAD_LETTERED, got %s", stored.State)
	}
}

func testReclaimExpiredLeases(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seedMessages(t, store, "invoice-processing", 1)

	claimed := claimOne(t, store, "invoice-processing", base)

	none, err := store.ReclaimExpiredLeases(context.Background(), base.Add(10*time.Second))
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	if len(none) != 0 {
		t.Fatalf("expected live lease to be untouched, got %d", len(none))
	}

	reclaimed, err := store.ReclaimExpiredLeases(context.Background(), base.Add(time.Minute))
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	if len(reclaimed) != 1 || reclaimed[0].ID != claimed.ID {
		t.Fatalf("expected expired lease to be reclaimed, got %+v", reclaimed)
	}

	if reclaimed[0].State != message.StateQueued {
		t.Fatalf("expected QUEUED after reclaim, got %s", reclaimed[0].State)
	}
}

func testDepth(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seedMessages(t, store, "invoice-processing", 3)

	claimed := claimOne(t, store, "invoice-processing", base)

	if err := store.ScheduleRetry(context.Background(), storage.RetryRequest{
		ID: claimed.ID, Attempt: 1, Reason: "boom", AvailableAt: base.Add(time.Hour), Now: base,
	}); err != nil {
		t.Fatalf("schedule retry: %v", err)
	}

	second := claimOne(t, store, "invoice-processing", base)
	_ = second

	depth, err := store.Depth(context.Background(), "invoice-processing")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}

	if depth.Ready != 1 || depth.InFlight != 1 || depth.Scheduled != 1 || depth.Total != 3 {
		t.Fatalf("unexpected depth %+v", depth)
	}

	if _, err := store.Depth(context.Background(), "unknown"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func testPurge(t *testing.T, store storage.Store) {
	seedQueue(t, store, "invoice-processing")
	seedMessages(t, store, "invoice-processing", 4)

	claimed := claimOne(t, store, "invoice-processing", base)

	if err := store.Acknowledge(context.Background(), claimed.ID, base); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}

	purged, err := store.Purge(context.Background(), "invoice-processing")
	if err != nil {
		t.Fatalf("purge: %v", err)
	}

	if purged != 3 {
		t.Fatalf("expected 3 purged messages, got %d", purged)
	}

	depth, err := store.Depth(context.Background(), "invoice-processing")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}

	if depth.Total != 0 {
		t.Fatalf("expected empty queue, got %+v", depth)
	}
}

func testDeadLetterLifecycle(t *testing.T, store storage.Store) {
	ctx := context.Background()

	seedQueue(t, store, "invoice-processing")

	entry := &storage.DeadLetter{
		MessageID:      "abc123",
		Queue:          "invoice-processing",
		Exchange:       "erp.events",
		RoutingKey:     "invoice.issued",
		Payload:        json.RawMessage(`{"invoice_id":"INV-9"}`),
		Headers:        map[string]string{"tenant": "acme"},
		Reason:         "downstream returned 500",
		ErrorKind:      "handler",
		Attempts:       3,
		PublishedAt:    base,
		FirstFailedAt:  base.Add(time.Second),
		DeadLetteredAt: base.Add(10 * time.Second),
	}

	if err := store.SaveDeadLetter(ctx, entry); err != nil {
		t.Fatalf("save dead letter: %v", err)
	}

	if entry.ID == "" {
		t.Fatal("expected generated dead letter identifier")
	}

	fetched, err := store.DeadLetter(ctx, entry.ID)
	if err != nil {
		t.Fatalf("dead letter: %v", err)
	}

	if fetched.Reason != entry.Reason || fetched.Attempts != 3 {
		t.Fatalf("unexpected dead letter %+v", fetched)
	}

	listed, err := store.DeadLetters(ctx, storage.DeadLetterFilter{Queue: "invoice-processing"})
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}

	if len(listed) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(listed))
	}

	count, err := store.CountDeadLetters(ctx, "invoice-processing")
	if err != nil {
		t.Fatalf("count dead letters: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	if err := store.DeleteDeadLetter(ctx, entry.ID); err != nil {
		t.Fatalf("delete dead letter: %v", err)
	}

	if _, err := store.DeadLetter(ctx, entry.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func testHistory(t *testing.T, store storage.Store) {
	ctx := context.Background()

	seedQueue(t, store, "invoice-processing")
	seeded := seedMessages(t, store, "invoice-processing", 1)

	events := []storage.HistoryEvent{
		{MessageID: seeded[0].ID, Queue: "invoice-processing", From: message.StateCreated, To: message.StateQueued, At: base},
		{MessageID: seeded[0].ID, Queue: "invoice-processing", From: message.StateQueued, To: message.StateProcessing, Consumer: "worker-1", At: base.Add(time.Second)},
	}

	for _, event := range events {
		if err := store.AppendHistory(ctx, event); err != nil {
			t.Fatalf("append history: %v", err)
		}
	}

	stored, err := store.History(ctx, seeded[0].ID)
	if err != nil {
		t.Fatalf("history: %v", err)
	}

	if len(stored) != 2 {
		t.Fatalf("expected 2 history events, got %d", len(stored))
	}

	if stored[1].Consumer != "worker-1" || stored[1].To != message.StateProcessing {
		t.Fatalf("unexpected history event %+v", stored[1])
	}
}
