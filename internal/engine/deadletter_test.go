package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

func awaitDeadLetter(t *testing.T, instance *Engine, queue string) *storage.DeadLetter {
	t.Helper()

	var record *storage.DeadLetter

	waitFor(t, "a dead letter to appear on "+queue, func() bool {
		entries, err := instance.DeadLetters(context.Background(), storage.DeadLetterFilter{Queue: queue})
		if err != nil || len(entries) == 0 {
			return false
		}

		record = entries[0]

		return true
	})

	return record
}

func TestDeadLetterCapturesFailureContext(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing")

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:  "invoice-worker",
		Queue: "invoice-processing",
		Handler: HandlerFunc(func(context.Context, Delivery) error {
			return errors.New("erp gateway timeout")
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	result := publishInvoice(t, instance, "INV-100")
	record := awaitDeadLetter(t, instance, "invoice-processing")

	if record.MessageID != result.MessageIDs[0] {
		t.Fatalf("unexpected message reference %s", record.MessageID)
	}

	if record.Reason != "erp gateway timeout" {
		t.Fatalf("unexpected reason %q", record.Reason)
	}

	if record.ErrorKind != errorKindHandler {
		t.Fatalf("unexpected error kind %q", record.ErrorKind)
	}

	if record.Attempts != 3 {
		t.Fatalf("expected 3 recorded attempts, got %d", record.Attempts)
	}

	if string(record.Payload) != `{"invoice_id":"INV-100"}` {
		t.Fatalf("original payload not preserved: %s", record.Payload)
	}

	if record.Headers["tenant"] != "acme" {
		t.Fatalf("headers not preserved: %+v", record.Headers)
	}

	if record.RoutingKey != "invoice.issued" || record.Exchange != "erp.events" {
		t.Fatalf("routing metadata not preserved: %+v", record)
	}

	if record.FirstFailedAt.After(record.DeadLetteredAt) {
		t.Fatalf("first failure %s must not be after dead lettering %s", record.FirstFailedAt, record.DeadLetteredAt)
	}

	count, err := instance.CountDeadLetters(context.Background(), "invoice-processing")
	if err != nil {
		t.Fatalf("count dead letters: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected a single dead letter, got %d", count)
	}
}

func TestPermanentFailuresAreLabelled(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(3))
	declareTopology(t, instance, "invoice-processing")

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:  "invoice-worker",
		Queue: "invoice-processing",
		Handler: HandlerFunc(func(context.Context, Delivery) error {
			return Permanent(errors.New("unknown invoice type"))
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	publishInvoice(t, instance, "INV-101")
	record := awaitDeadLetter(t, instance, "invoice-processing")

	if record.ErrorKind != errorKindPermanent {
		t.Fatalf("expected permanent classification, got %q", record.ErrorKind)
	}

	if record.Attempts != 1 {
		t.Fatalf("expected a single attempt, got %d", record.Attempts)
	}
}

func TestReplayRequeuesTheOriginalPayload(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(2))
	declareTopology(t, instance, "invoice-processing")

	var fail atomic.Bool
	fail.Store(true)

	processed := make(chan Delivery, 4)

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:  "invoice-worker",
		Queue: "invoice-processing",
		Handler: HandlerFunc(func(_ context.Context, delivery Delivery) error {
			if fail.Load() {
				return errors.New("erp gateway timeout")
			}

			processed <- delivery

			return nil
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	publishInvoice(t, instance, "INV-102")
	record := awaitDeadLetter(t, instance, "invoice-processing")

	fail.Store(false)

	replay, err := instance.ReplayDeadLetter(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if replay.MessageID == record.MessageID {
		t.Fatal("replay must create a new message identity")
	}

	delivery := <-processed

	if delivery.Message.ID != replay.MessageID {
		t.Fatalf("expected replayed message %s, got %s", replay.MessageID, delivery.Message.ID)
	}

	if string(delivery.Message.Payload) != `{"invoice_id":"INV-102"}` {
		t.Fatalf("replayed payload differs: %s", delivery.Message.Payload)
	}

	if delivery.Message.Headers[headerReplayOf] != record.MessageID {
		t.Fatalf("expected replay provenance header, got %+v", delivery.Message.Headers)
	}

	awaitState(t, instance, replay.MessageID, message.StateAcknowledged)

	updated, err := instance.DeadLetter(context.Background(), record.ID)
	if err != nil {
		t.Fatalf("dead letter: %v", err)
	}

	if updated.ReplayCount != 1 || updated.ReplayedAs != replay.MessageID {
		t.Fatalf("expected replay metadata on the record, got %+v", updated)
	}
}

func TestDiscardDeadLetter(t *testing.T) {
	instance := newTestEngine(t, fastPolicy(1))
	declareTopology(t, instance, "invoice-processing")

	if _, err := instance.Subscribe(ConsumerSpec{
		Name:  "invoice-worker",
		Queue: "invoice-processing",
		Handler: HandlerFunc(func(context.Context, Delivery) error {
			return errors.New("permanently broken downstream")
		}),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	publishInvoice(t, instance, "INV-103")
	record := awaitDeadLetter(t, instance, "invoice-processing")

	if err := instance.DiscardDeadLetter(context.Background(), record.ID); err != nil {
		t.Fatalf("discard: %v", err)
	}

	if _, err := instance.DeadLetter(context.Background(), record.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
