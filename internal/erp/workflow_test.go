package erp_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/engine"
	"github.com/deshenrao/rabbitmq-lite/internal/erp"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/retry"
	"github.com/deshenrao/rabbitmq-lite/internal/schema"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/memory"
)

var queues = []string{"customer-sync", "invoice-processing", "inventory-sync", "employee-sync", "audit-log"}

func newBroker(t *testing.T, maxAttempts int) *engine.Engine {
	t.Helper()

	registry := schema.NewRegistry()
	if _, err := registry.LoadDirectory(filepath.Join("..", "..", "schemas")); err != nil {
		t.Fatalf("load schemas: %v", err)
	}

	store := memory.New()

	instance, err := engine.New(engine.Options{
		Store:   store,
		Schemas: registry,
		Logger:  slog.New(slog.DiscardHandler),
		Retry: retry.Policy{
			MaxAttempts:     maxAttempts,
			InitialInterval: time.Millisecond,
			MaxInterval:     5 * time.Millisecond,
			Multiplier:      2,
		},
		ReclaimInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	instance.Start(context.Background())

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_ = instance.Shutdown(ctx)
		_ = store.Close()
	})

	if err := erp.DeclareTopology(context.Background(), instance, time.Second, maxAttempts); err != nil {
		t.Fatalf("declare topology: %v", err)
	}

	return instance
}

func waitForDrain(t *testing.T, instance *engine.Engine) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)

	for time.Now().Before(deadline) {
		drained := true

		for _, queue := range queues {
			depth, err := instance.Depth(context.Background(), queue)
			if err != nil || depth.Total != 0 {
				drained = false
				break
			}
		}

		if drained {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	for _, queue := range queues {
		depth, _ := instance.Depth(context.Background(), queue)
		t.Logf("queue %s depth %+v", queue, depth)
	}

	t.Fatal("timed out waiting for the nightly export to drain")
}

func TestNightlyExportDrainsThroughEveryConsumer(t *testing.T) {
	instance := newBroker(t, 8)

	workflow, err := erp.Subscribe(instance, erp.WorkflowOptions{
		Seed:           42,
		GatewayFailure: 0.25,
		Concurrency:    4,
		Prefetch:       8,
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	generator := erp.NewGenerator(7)

	published, err := generator.NightlyExport(context.Background(), instance, 48)
	if err != nil {
		t.Fatalf("nightly export: %v", err)
	}

	if published != 48 {
		t.Fatalf("expected 48 published events, got %d", published)
	}

	waitForDrain(t, instance)

	if workflow.Audit.Entries() == 0 {
		t.Fatal("expected the audit log to observe every event")
	}

	total := int64(0)
	for _, stats := range workflow.Stats() {
		total += stats.Handled
	}

	if total == 0 {
		t.Fatal("expected downstream systems to have handled work")
	}

	if workflow.Invoices.Stats().Failed == 0 {
		t.Log("no transient ledger failures were simulated in this run")
	}
}

func TestUnsupportedCurrencyDeadLettersWithoutRetrying(t *testing.T) {
	instance := newBroker(t, 5)

	workflow, err := erp.Subscribe(instance, erp.WorkflowOptions{Seed: 1, Concurrency: 2, Prefetch: 4})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	invoice := erp.Invoice{
		InvoiceID:  "INV-9000001",
		CustomerID: "C-100244",
		Currency:   "USD",
		IssuedOn:   time.Now().UTC().Format(time.DateOnly),
		Lines: []erp.InvoiceLine{
			{Position: 1, SKU: "PALLET-EUR", Quantity: 4, UnitPriceNet: 18.5, TaxRate: 0.19},
		},
	}

	payload, err := json.Marshal(invoice)
	if err != nil {
		t.Fatalf("encode invoice: %v", err)
	}

	if _, err := instance.Publish(context.Background(), message.Publication{
		Exchange:   erp.ExchangeEvents,
		RoutingKey: erp.RoutingInvoiceIssued,
		Payload:    payload,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	record := awaitDeadLetter(t, instance, "invoice-processing")

	if record.ErrorKind != "permanent" {
		t.Fatalf("expected a permanent classification, got %q", record.ErrorKind)
	}

	if record.Attempts != 1 {
		t.Fatalf("a permanent failure must not be retried, got %d attempts", record.Attempts)
	}

	if workflow.Invoices.Stats().Handled != 0 {
		t.Fatal("the ledger must not have accepted an unsupported currency")
	}
}

func TestMalformedLegacyExportIsRejectedAtPublish(t *testing.T) {
	instance := newBroker(t, 3)

	if _, err := erp.Subscribe(instance, erp.WorkflowOptions{Seed: 2}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	generator := erp.NewGenerator(11)

	_, err := instance.Publish(context.Background(), generator.MalformedInvoice())
	if err == nil {
		t.Fatal("expected the legacy export to be rejected")
	}

	validation, ok := schema.AsValidationError(err)
	if !ok {
		t.Fatalf("expected a schema validation error, got %v", err)
	}

	if len(validation.Violations) == 0 {
		t.Fatal("expected field level violations")
	}

	record := awaitDeadLetter(t, instance, "invoice-processing")

	if record.ErrorKind != "validation" {
		t.Fatalf("expected the rejected payload to be parked as a validation failure, got %q", record.ErrorKind)
	}

	if record.Headers["export-file"] != "legacy_invoice_batch.csv" {
		t.Fatalf("expected the provenance header to survive, got %+v", record.Headers)
	}
}

func TestReplayAfterDownstreamRecovery(t *testing.T) {
	instance := newBroker(t, 1)

	if _, err := erp.Subscribe(instance, erp.WorkflowOptions{
		Seed:           3,
		GatewayFailure: 1,
		Concurrency:    1,
		Prefetch:       1,
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	generator := erp.NewGenerator(13)

	if _, err := instance.Publish(context.Background(), generator.InventoryAdjustment()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	record := awaitDeadLetter(t, instance, "inventory-sync")

	if record.Reason == "" {
		t.Fatal("expected the gateway failure to be recorded")
	}

	if err := instance.Unsubscribe("wms-projector"); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}

	recovered := erp.NewInventoryProjection(erp.DownstreamOptions{Name: "wms", Seed: 4})

	if _, err := instance.Subscribe(engine.ConsumerSpec{
		Name:    "wms-projector-recovered",
		Queue:   "inventory-sync",
		Handler: recovered,
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	replay, err := instance.ReplayDeadLetter(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		stored, err := instance.Message(context.Background(), replay.MessageID)
		if err == nil && stored.State == message.StateAcknowledged {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("replayed message was never acknowledged (downstream handled %d)", recovered.Stats().Handled)
}

func awaitDeadLetter(t *testing.T, instance *engine.Engine, queue string) *storage.DeadLetter {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		entries, err := instance.DeadLetters(context.Background(), storage.DeadLetterFilter{Queue: queue})
		if err == nil && len(entries) > 0 {
			return entries[0]
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for a dead letter on %s", queue)

	return nil
}
