package broker

import (
	"errors"
	"sort"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()

	b := NewRegistry()

	if _, err := b.DeclareExchange(ExchangeSpec{Name: "erp.events", Kind: ExchangeDirect}); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	return b
}

func declareQueue(t *testing.T, b *Registry, name string) {
	t.Helper()

	if _, err := b.DeclareQueue(QueueSpec{Name: name, Durable: true}); err != nil {
		t.Fatalf("declare queue %s: %v", name, err)
	}
}

func bind(t *testing.T, b *Registry, exchange, queue, key string) {
	t.Helper()

	if _, err := b.Bind(BindingSpec{Exchange: exchange, Queue: queue, RoutingKey: key}); err != nil {
		t.Fatalf("bind %s -> %s: %v", exchange, queue, err)
	}
}

func routedQueues(t *testing.T, b *Registry, exchange, key string) []string {
	t.Helper()

	routed, err := b.Route(exchange, key)
	if err != nil {
		t.Fatalf("route: %v", err)
	}

	names := make([]string, 0, len(routed))
	for _, queue := range routed {
		names = append(names, queue.Name)
	}

	sort.Strings(names)

	return names
}

func TestDeclareQueueAppliesDefaults(t *testing.T) {
	b := NewRegistry()

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
	b := NewRegistry()

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
	b := NewRegistry()

	if _, err := b.DeclareQueue(QueueSpec{Name: "invoice-processing", MaxAttempts: 3}); err != nil {
		t.Fatalf("declare: %v", err)
	}

	_, err := b.DeclareQueue(QueueSpec{Name: "invoice-processing", MaxAttempts: 9})
	if !errors.Is(err, ErrQueueExists) {
		t.Fatalf("expected ErrQueueExists, got %v", err)
	}
}

func TestDeclareQueueRejectsInvalidNames(t *testing.T) {
	b := NewRegistry()

	for _, name := range []string{"", ".leading", "has space", "tab\t"} {
		if _, err := b.DeclareQueue(QueueSpec{Name: name}); err == nil {
			t.Errorf("expected rejection for queue name %q", name)
		}
	}
}

func TestDirectExchangeRoutesByExactKey(t *testing.T) {
	b := newTestRegistry(t)

	declareQueue(t, b, "customer-sync")
	declareQueue(t, b, "invoice-processing")
	bind(t, b, "erp.events", "customer-sync", "customer.created")
	bind(t, b, "erp.events", "invoice-processing", "invoice.issued")

	if got := routedQueues(t, b, "erp.events", "customer.created"); len(got) != 1 || got[0] != "customer-sync" {
		t.Fatalf("expected customer-sync, got %v", got)
	}
}

func TestFanoutExchangeIgnoresRoutingKey(t *testing.T) {
	b := NewRegistry()

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
	b := newTestRegistry(t)

	declareQueue(t, b, "customer-sync")
	bind(t, b, "erp.events", "customer-sync", "customer.created")

	_, err := b.Route("erp.events", "customer.deleted")
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("expected ErrUnroutable, got %v", err)
	}
}

func TestPublishToUnknownExchangeFails(t *testing.T) {
	b := NewRegistry()

	_, err := b.Route("missing", "x")
	if !errors.Is(err, ErrExchangeNotFound) {
		t.Fatalf("expected ErrExchangeNotFound, got %v", err)
	}
}

func TestDuplicateBindingsDeliverOnce(t *testing.T) {
	b := newTestRegistry(t)

	declareQueue(t, b, "customer-sync")
	bind(t, b, "erp.events", "customer-sync", "customer.created")
	bind(t, b, "erp.events", "customer-sync", "customer.created")

	if got := routedQueues(t, b, "erp.events", "customer.created"); len(got) != 1 {
		t.Fatalf("expected single delivery, got %v", got)
	}
}

func TestUnbindStopsDelivery(t *testing.T) {
	b := newTestRegistry(t)

	declareQueue(t, b, "customer-sync")
	spec := BindingSpec{Exchange: "erp.events", Queue: "customer-sync", RoutingKey: "customer.created"}
	bind(t, b, spec.Exchange, spec.Queue, spec.RoutingKey)

	if err := b.Unbind(spec); err != nil {
		t.Fatalf("unbind: %v", err)
	}

	if _, err := b.Route("erp.events", "customer.created"); !errors.Is(err, ErrUnroutable) {
		t.Fatalf("expected ErrUnroutable after unbind, got %v", err)
	}

	if err := b.Unbind(spec); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("expected ErrBindingNotFound, got %v", err)
	}
}

func TestDirectBindingRejectsWildcards(t *testing.T) {
	b := newTestRegistry(t)

	declareQueue(t, b, "customer-sync")

	if _, err := b.Bind(BindingSpec{Exchange: "erp.events", Queue: "customer-sync", RoutingKey: "customer.*"}); err == nil {
		t.Fatal("expected wildcard binding on direct exchange to be rejected")
	}
}

func TestRouteReturnsQueueDefinitions(t *testing.T) {
	b := newTestRegistry(t)

	if _, err := b.DeclareQueue(QueueSpec{Name: "customer-sync", Durable: true, MaxAttempts: 5, VisibilityTimeout: time.Minute}); err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	bind(t, b, "erp.events", "customer-sync", "customer.created")

	routed, err := b.Route("erp.events", "customer.created")
	if err != nil {
		t.Fatalf("route: %v", err)
	}

	if len(routed) != 1 || routed[0].MaxAttempts != 5 {
		t.Fatalf("expected queue definition to be returned, got %+v", routed)
	}
}

func TestDeleteQueueRejectsBoundQueues(t *testing.T) {
	b := newTestRegistry(t)

	declareQueue(t, b, "customer-sync")
	bind(t, b, "erp.events", "customer-sync", "customer.created")

	if err := b.DeleteQueue("customer-sync"); !errors.Is(err, ErrQueueInUse) {
		t.Fatalf("expected ErrQueueInUse, got %v", err)
	}

	if err := b.Unbind(BindingSpec{Exchange: "erp.events", Queue: "customer-sync", RoutingKey: "customer.created"}); err != nil {
		t.Fatalf("unbind: %v", err)
	}

	if err := b.DeleteQueue("customer-sync"); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
}

func TestRestoreRebuildsTopology(t *testing.T) {
	source := newTestRegistry(t)
	declareQueue(t, source, "customer-sync")
	bind(t, source, "erp.events", "customer-sync", "customer.created")

	restored := NewRegistry()
	restored.Restore(source.Exchanges(), source.Queues(), source.AllBindings())

	routed, err := restored.Route("erp.events", "customer.created")
	if err != nil {
		t.Fatalf("route after restore: %v", err)
	}

	if len(routed) != 1 || routed[0].Name != "customer-sync" {
		t.Fatalf("unexpected restored routing %+v", routed)
	}
}
