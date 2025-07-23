package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/schema"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/memory"
)

const inventorySchema = `{
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"title": "Inventory adjustment",
	"type": "object",
	"required": ["sku", "warehouse", "delta"],
	"additionalProperties": false,
	"properties": {
		"sku": {"type": "string", "minLength": 1},
		"warehouse": {"type": "string", "minLength": 1},
		"delta": {"type": "integer"}
	}
}`

func newValidatingEngine(t *testing.T) *Engine {
	t.Helper()

	registry := schema.NewRegistry()
	if _, err := registry.Register("inventory-adjustment", []byte(inventorySchema)); err != nil {
		t.Fatalf("register schema: %v", err)
	}

	store := memory.New()

	instance, err := New(Options{
		Store:   store,
		Retry:   fastPolicy(3),
		Schemas: registry,
		Logger:  slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	instance.Start(context.Background())

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = instance.Shutdown(ctx)
		_ = store.Close()
	})

	ctx := context.Background()

	if _, err := instance.DeclareExchange(ctx, broker.ExchangeSpec{Name: "erp.events", Kind: broker.ExchangeTopic, Durable: true}); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	if _, err := instance.DeclareQueue(ctx, broker.QueueSpec{
		Name:              "inventory-sync",
		Durable:           true,
		MaxAttempts:       3,
		VisibilityTimeout: time.Second,
		Schema:            "inventory-adjustment",
	}); err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	if _, err := instance.Bind(ctx, broker.BindingSpec{
		Exchange: "erp.events", Queue: "inventory-sync", RoutingKey: "inventory.#",
	}); err != nil {
		t.Fatalf("bind: %v", err)
	}

	return instance
}

func publishAdjustment(instance *Engine, payload string) (*PublishResult, error) {
	return instance.Publish(context.Background(), message.Publication{
		Exchange:   "erp.events",
		RoutingKey: "inventory.adjusted",
		Payload:    json.RawMessage(payload),
	})
}

func TestQueueSchemaIsAppliedToPublications(t *testing.T) {
	instance := newValidatingEngine(t)

	result, err := publishAdjustment(instance, `{"sku":"SKU-1","warehouse":"DE-01","delta":-4}`)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	stored, err := instance.Message(context.Background(), result.MessageIDs[0])
	if err != nil {
		t.Fatalf("message: %v", err)
	}

	if stored.Schema != "inventory-adjustment" {
		t.Fatalf("expected the queue schema to be attached, got %q", stored.Schema)
	}
}

func TestInvalidPayloadIsRejectedBeforeEnqueue(t *testing.T) {
	instance := newValidatingEngine(t)

	_, err := publishAdjustment(instance, `{"sku":"SKU-1","delta":"minus four"}`)

	validation, ok := schema.AsValidationError(err)
	if !ok {
		t.Fatalf("expected a schema validation error, got %v", err)
	}

	if len(validation.Violations) == 0 {
		t.Fatal("expected field level violations")
	}

	depth, err := instance.Depth(context.Background(), "inventory-sync")
	if err != nil {
		t.Fatalf("depth: %v", err)
	}

	if depth.Total != 0 {
		t.Fatalf("rejected payloads must not enter the queue, got %+v", depth)
	}
}

func TestRejectedPayloadsArePreservedForInspection(t *testing.T) {
	instance := newValidatingEngine(t)

	if _, err := publishAdjustment(instance, `{"sku":"SKU-1","delta":"minus four"}`); err == nil {
		t.Fatal("expected publication to be rejected")
	}

	entries, err := instance.DeadLetters(context.Background(), storage.DeadLetterFilter{Queue: "inventory-sync"})
	if err != nil {
		t.Fatalf("dead letters: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected the rejected payload to be parked, got %d", len(entries))
	}

	if entries[0].ErrorKind != errorKindValidation {
		t.Fatalf("unexpected error kind %q", entries[0].ErrorKind)
	}

	if entries[0].Attempts != 0 {
		t.Fatalf("a rejected payload was never delivered, got %d attempts", entries[0].Attempts)
	}

	if string(entries[0].Payload) != `{"sku":"SKU-1","delta":"minus four"}` {
		t.Fatalf("original payload not preserved: %s", entries[0].Payload)
	}
}

func TestPublicationSchemaOverridesQueueDefault(t *testing.T) {
	instance := newValidatingEngine(t)

	_, err := instance.Publish(context.Background(), message.Publication{
		Exchange:   "erp.events",
		RoutingKey: "inventory.adjusted",
		Payload:    json.RawMessage(`{"anything":true}`),
		Schema:     "unregistered-schema",
	})
	if err == nil {
		t.Fatal("expected an unknown schema to be reported")
	}
}
