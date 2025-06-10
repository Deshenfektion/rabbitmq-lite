package broker

import (
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
)

func newTestBroker(t *testing.T) *Broker {
	t.Helper()

	b := New()

	if _, err := b.DeclareExchange(ExchangeSpec{Name: "erp.events", Kind: ExchangeDirect}); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	return b
}

func declareQueue(t *testing.T, b *Broker, name string) {
	t.Helper()

	if _, err := b.DeclareQueue(QueueSpec{Name: name, Durable: true}); err != nil {
		t.Fatalf("declare queue %s: %v", name, err)
	}
}

func bind(t *testing.T, b *Broker, exchange, queue, key string) {
	t.Helper()

	if _, err := b.Bind(BindingSpec{Exchange: exchange, Queue: queue, RoutingKey: key}); err != nil {
		t.Fatalf("bind %s -> %s: %v", exchange, queue, err)
	}
}

func routedQueues(t *testing.T, b *Broker, exchange, key string) []string {
	t.Helper()

	routed, err := b.Publish(message.Publication{
		Exchange:   exchange,
		RoutingKey: key,
		Payload:    json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	names := make([]string, 0, len(routed))
	for _, msg := range routed {
		names = append(names, msg.Queue)
	}

	sort.Strings(names)

	return names
}

func TestDeclareQueueAppliesDefaults(t *testing.T) {
	b := New()

	queue, err := b.DeclareQueue(QueueSpec{Name: "invoice-processing"})
	if err != nil {
		t.Fatalf("declare: %v", err)
	}

	if queue.MaxAttempts != defaultMaxAttempts {
		t.Fatalf("expected default max attempts %d, got %d", defaultMaxAttempts, queue.MaxAttempts)
	}

	if queue.VisibilityTimeout != defaultVisibilityTime {
		t.Fatalf("expected default visibility timeout, got %s", queue.VisibilityTimeout)
	}
}

func TestDeclareQueueIsIdempotentForIdenticalSpecs(t *testing.T) {
	b := New()

	spec := QueueSpec{Name: "invoice-processing", Durable: true, MaxAttempts: 5, VisibilityTimeout: time.Minute}

	first, err := b.DeclareQueue(spec)
	if err != nil {
		t.Fatalf("first declare: %v", err)
	}

	second, err := b.DeclareQueue(spec)
	if err != nil {
		t.Fatalf("second declare: %v", err)
	}

	if first != second {
		t.Fatal("expected repeated declaration to return the same queue")
	}
}

func TestDeclareQueueRejectsConflictingRedeclaration(t *testing.T) {
	b := New()

	if _, err := b.DeclareQueue(QueueSpec{Name: "invoice-processing", MaxAttempts: 3}); err != nil {
		t.Fatalf("declare: %v", err)
	}

	_, err := b.DeclareQueue(QueueSpec{Name: "invoice-processing", MaxAttempts: 9})
	if !errors.Is(err, ErrQueueExists) {
		t.Fatalf("expected ErrQueueExists, got %v", err)
	}
}

func TestDeclareQueueRejectsInvalidNames(t *testing.T) {
	b := New()

	for _, name := range []string{"", ".leading", "has space", "tab\t"} {
		if _, err := b.DeclareQueue(QueueSpec{Name: name}); err == nil {
			t.Errorf("expected rejection for queue name %q", name)
		}
	}
}

func TestDirectExchangeRoutesByExactKey(t *testing.T) {
	b := newTestBroker(t)

	declareQueue(t, b, "customer-sync")
	declareQueue(t, b, "invoice-processing")
	bind(t, b, "erp.events", "customer-sync", "customer.created")
	bind(t, b, "erp.events", "invoice-processing", "invoice.issued")

	if got := routedQueues(t, b, "erp.events", "customer.created"); len(got) != 1 || got[0] != "customer-sync" {
		t.Fatalf("expected customer-sync, got %v", got)
	}
}

func TestFanoutExchangeIgnoresRoutingKey(t *testing.T) {
	b := New()

	if _, err := b.DeclareExchange(ExchangeSpec{Name: "erp.broadcast", Kind: ExchangeFanout}); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	declareQueue(t, b, "audit-log")
	declareQueue(t, b, "analytics")
	bind(t, b, "erp.broadcast", "audit-log", "")
	bind(t, b, "erp.broadcast", "analytics", "")

	got := routedQueues(t, b, "erp.broadcast", "anything.at.all")
	if len(got) != 2 || got[0] != "analytics" || got[1] != "audit-log" {
		t.Fatalf("expected both queues, got %v", got)
	}
}

func TestPublishToUnboundKeyIsUnroutable(t *testing.T) {
	b := newTestBroker(t)

	declareQueue(t, b, "customer-sync")
	bind(t, b, "erp.events", "customer-sync", "customer.created")

	_, err := b.Publish(message.Publication{Exchange: "erp.events", RoutingKey: "customer.deleted"})
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("expected ErrUnroutable, got %v", err)
	}
}

func TestPublishToUnknownExchangeFails(t *testing.T) {
	b := New()

	_, err := b.Publish(message.Publication{Exchange: "missing", RoutingKey: "x"})
	if !errors.Is(err, ErrExchangeNotFound) {
		t.Fatalf("expected ErrExchangeNotFound, got %v", err)
	}
}

func TestDuplicateBindingsDeliverOnce(t *testing.T) {
	b := newTestBroker(t)

	declareQueue(t, b, "customer-sync")
	bind(t, b, "erp.events", "customer-sync", "customer.created")
	bind(t, b, "erp.events", "customer-sync", "customer.created")

	if got := routedQueues(t, b, "erp.events", "customer.created"); len(got) != 1 {
		t.Fatalf("expected single delivery, got %v", got)
	}
}

func TestUnbindStopsDelivery(t *testing.T) {
	b := newTestBroker(t)

	declareQueue(t, b, "customer-sync")
	spec := BindingSpec{Exchange: "erp.events", Queue: "customer-sync", RoutingKey: "customer.created"}
	bind(t, b, spec.Exchange, spec.Queue, spec.RoutingKey)

	if err := b.Unbind(spec); err != nil {
		t.Fatalf("unbind: %v", err)
	}

	if _, err := b.Publish(message.Publication{Exchange: "erp.events", RoutingKey: "customer.created"}); !errors.Is(err, ErrUnroutable) {
		t.Fatalf("expected ErrUnroutable after unbind, got %v", err)
	}

	if err := b.Unbind(spec); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("expected ErrBindingNotFound, got %v", err)
	}
}

func TestDirectBindingRejectsWildcards(t *testing.T) {
	b := newTestBroker(t)

	declareQueue(t, b, "customer-sync")

	if _, err := b.Bind(BindingSpec{Exchange: "erp.events", Queue: "customer-sync", RoutingKey: "customer.*"}); err == nil {
		t.Fatal("expected wildcard binding on direct exchange to be rejected")
	}
}

func TestPublishedMessagesEnterQueuedState(t *testing.T) {
	b := newTestBroker(t)

	declareQueue(t, b, "customer-sync")
	bind(t, b, "erp.events", "customer-sync", "customer.created")

	routed, err := b.Publish(message.Publication{
		Exchange:   "erp.events",
		RoutingKey: "customer.created",
		Payload:    json.RawMessage(`{"customer_id":"C-1"}`),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if routed[0].State != message.StateQueued {
		t.Fatalf("expected QUEUED, got %s", routed[0].State)
	}

	if routed[0].MaxAttempts != defaultMaxAttempts {
		t.Fatalf("expected queue max attempts to be inherited, got %d", routed[0].MaxAttempts)
	}

	depth, err := b.Depth("customer-sync")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}

	if depth != 1 {
		t.Fatalf("expected depth 1, got %d", depth)
	}
}
