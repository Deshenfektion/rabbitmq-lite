package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/api"
	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/engine"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/retry"
	"github.com/deshenrao/rabbitmq-lite/internal/schema"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/memory"
)

const inventorySchema = `{
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"title": "Inventory adjustment",
	"type": "object",
	"required": ["sku", "delta"],
	"additionalProperties": false,
	"properties": {
		"sku": {"type": "string", "minLength": 1},
		"delta": {"type": "integer"}
	}
}`

type harness struct {
	server *httptest.Server
	engine *engine.Engine
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	registry := schema.NewRegistry()
	if _, err := registry.Register("inventory-adjustment", []byte(inventorySchema)); err != nil {
		t.Fatalf("register schema: %v", err)
	}

	store := memory.New()

	instance, err := engine.New(engine.Options{
		Store:   store,
		Schemas: registry,
		Logger:  slog.New(slog.DiscardHandler),
		Retry: retry.Policy{
			MaxAttempts:     2,
			InitialInterval: time.Millisecond,
			MaxInterval:     2 * time.Millisecond,
			Multiplier:      2,
		},
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	instance.Start(context.Background())

	server := httptest.NewServer(api.New(api.Options{
		Engine:  instance,
		Schemas: registry,
		Logger:  slog.New(slog.DiscardHandler),
		Version: "test",
	}))

	t.Cleanup(func() {
		server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = instance.Shutdown(ctx)
		_ = store.Close()
	})

	return &harness{server: server, engine: instance}
}

func (h *harness) do(t *testing.T, method, path string, body any) (*http.Response, []byte) {
	t.Helper()

	var reader io.Reader

	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode request: %v", err)
		}

		reader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequest(method, h.server.URL+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}

	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := h.server.Client().Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}

	defer response.Body.Close()

	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	return response, payload
}

func (h *harness) expect(t *testing.T, method, path string, body any, status int) []byte {
	t.Helper()

	response, payload := h.do(t, method, path, body)

	if response.StatusCode != status {
		t.Fatalf("%s %s: expected %d, got %d (%s)", method, path, status, response.StatusCode, payload)
	}

	return payload
}

func decode[T any](t *testing.T, payload []byte) T {
	t.Helper()

	var value T
	if err := json.Unmarshal(payload, &value); err != nil {
		t.Fatalf("decode response: %v (%s)", err, payload)
	}

	return value
}

func (h *harness) declareTopology(t *testing.T) {
	t.Helper()

	h.expect(t, http.MethodPost, "/api/v1/exchanges", map[string]any{
		"name": "erp.events", "kind": "topic", "durable": true,
	}, http.StatusCreated)

	h.expect(t, http.MethodPost, "/api/v1/queues", map[string]any{
		"name": "inventory-sync", "durable": true, "max_attempts": 2,
		"visibility_timeout": "2s", "schema": "inventory-adjustment",
	}, http.StatusCreated)

	h.expect(t, http.MethodPost, "/api/v1/bindings", map[string]any{
		"exchange": "erp.events", "queue": "inventory-sync", "routing_key": "inventory.#",
	}, http.StatusCreated)
}

func (h *harness) publish(t *testing.T, payload map[string]any, status int) []byte {
	t.Helper()

	return h.expect(t, http.MethodPost, "/api/v1/messages", map[string]any{
		"exchange":    "erp.events",
		"routing_key": "inventory.adjusted",
		"payload":     payload,
	}, status)
}

func TestHealthAndReadiness(t *testing.T) {
	h := newHarness(t)

	h.expect(t, http.MethodGet, "/healthz", nil, http.StatusOK)
	h.expect(t, http.MethodGet, "/readyz", nil, http.StatusOK)
}

func TestRequestIDIsEchoed(t *testing.T) {
	h := newHarness(t)

	response, _ := h.do(t, http.MethodGet, "/healthz", nil)

	if response.Header.Get("X-Request-Id") == "" {
		t.Fatal("expected a generated request identifier")
	}
}

func TestDeclareTopologyOverHTTP(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	payload := h.expect(t, http.MethodGet, "/api/v1/queues/inventory-sync", nil, http.StatusOK)
	queue := decode[map[string]any](t, payload)

	if queue["schema"] != "inventory-adjustment" {
		t.Fatalf("unexpected queue view %v", queue)
	}

	if queue["visibility_timeout"] != "2s" {
		t.Fatalf("unexpected visibility timeout %v", queue["visibility_timeout"])
	}
}

func TestConflictingQueueDeclarationIsRejected(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	h.expect(t, http.MethodPost, "/api/v1/queues", map[string]any{
		"name": "inventory-sync", "durable": true, "max_attempts": 9, "visibility_timeout": "2s",
		"schema": "inventory-adjustment",
	}, http.StatusConflict)
}

func TestInvalidQueueNameIsRejected(t *testing.T) {
	h := newHarness(t)

	h.expect(t, http.MethodPost, "/api/v1/queues", map[string]any{"name": "not valid"}, http.StatusBadRequest)
}

func TestUnknownSchemaOnQueueIsRejected(t *testing.T) {
	h := newHarness(t)

	h.expect(t, http.MethodPost, "/api/v1/queues", map[string]any{
		"name": "purchase-orders", "schema": "purchase-order",
	}, http.StatusUnprocessableEntity)
}

func TestUnknownFieldsAreRejected(t *testing.T) {
	h := newHarness(t)

	h.expect(t, http.MethodPost, "/api/v1/exchanges", map[string]any{
		"name": "erp.events", "kind": "topic", "durabel": true,
	}, http.StatusBadRequest)
}

func TestPublishRoutesAndValidates(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	payload := h.publish(t, map[string]any{"sku": "PALLET-EUR", "delta": -3}, http.StatusAccepted)
	result := decode[engine.PublishResult](t, payload)

	if len(result.MessageIDs) != 1 || result.Queues[0] != "inventory-sync" {
		t.Fatalf("unexpected publish result %+v", result)
	}

	stored := decode[map[string]any](t,
		h.expect(t, http.MethodGet, "/api/v1/messages/"+result.MessageIDs[0], nil, http.StatusOK))

	if stored["state"] != string(message.StateQueued) {
		t.Fatalf("unexpected message state %v", stored["state"])
	}

	if _, ok := stored["history"]; !ok {
		t.Fatal("expected the message view to include its history")
	}
}

func TestPublishRejectsInvalidPayloadWithViolations(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	payload := h.publish(t, map[string]any{"sku": "", "delta": "three"}, http.StatusUnprocessableEntity)
	failure := decode[struct {
		Error      string             `json:"error"`
		Violations []schema.Violation `json:"violations"`
	}](t, payload)

	if failure.Error != "schema_validation_failed" {
		t.Fatalf("unexpected error code %q", failure.Error)
	}

	if len(failure.Violations) == 0 {
		t.Fatal("expected field level violations in the response")
	}
}

func TestPublishToUnboundRoutingKeyIsUnprocessable(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	h.expect(t, http.MethodPost, "/api/v1/messages", map[string]any{
		"exchange":    "erp.events",
		"routing_key": "customer.created",
		"payload":     map[string]any{"sku": "X", "delta": 1},
	}, http.StatusUnprocessableEntity)
}

func TestPublishToUnknownExchangeIsNotFound(t *testing.T) {
	h := newHarness(t)

	h.expect(t, http.MethodPost, "/api/v1/messages", map[string]any{
		"exchange":    "missing",
		"routing_key": "inventory.adjusted",
		"payload":     map[string]any{"sku": "X", "delta": 1},
	}, http.StatusNotFound)
}

func TestUnknownMessageIsNotFound(t *testing.T) {
	h := newHarness(t)

	h.expect(t, http.MethodGet, "/api/v1/messages/deadbeef", nil, http.StatusNotFound)
}

func TestConsumeAcknowledgeCycle(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	published := decode[engine.PublishResult](t,
		h.publish(t, map[string]any{"sku": "PALLET-EUR", "delta": 7}, http.StatusAccepted))

	batch := decode[struct {
		Messages []*message.Message `json:"messages"`
		Lease    string             `json:"lease"`
	}](t, h.expect(t, http.MethodPost, "/api/v1/queues/inventory-sync/consume",
		map[string]any{"consumer": "warehouse-worker", "limit": 5}, http.StatusOK))

	if len(batch.Messages) != 1 || batch.Messages[0].ID != published.MessageIDs[0] {
		t.Fatalf("unexpected consume batch %+v", batch.Messages)
	}

	if batch.Messages[0].State != message.StateProcessing {
		t.Fatalf("expected PROCESSING, got %s", batch.Messages[0].State)
	}

	if batch.Lease != "2s" {
		t.Fatalf("expected the queue visibility timeout as lease, got %q", batch.Lease)
	}

	h.expect(t, http.MethodPost, "/api/v1/messages/"+published.MessageIDs[0]+"/ack", nil, http.StatusOK)

	stored := decode[map[string]any](t,
		h.expect(t, http.MethodGet, "/api/v1/messages/"+published.MessageIDs[0], nil, http.StatusOK))

	if stored["state"] != string(message.StateAcknowledged) {
		t.Fatalf("expected ACKNOWLEDGED, got %v", stored["state"])
	}
}

func TestConsumeOnEmptyQueueReturnsNoContent(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	h.expect(t, http.MethodPost, "/api/v1/queues/inventory-sync/consume",
		map[string]any{"consumer": "warehouse-worker"}, http.StatusNoContent)
}

func TestAcknowledgingAnUnclaimedMessageConflicts(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	published := decode[engine.PublishResult](t,
		h.publish(t, map[string]any{"sku": "PALLET-EUR", "delta": 1}, http.StatusAccepted))

	h.expect(t, http.MethodPost, "/api/v1/messages/"+published.MessageIDs[0]+"/ack", nil, http.StatusConflict)
}

func TestNackWithRequeueMakesTheMessageAvailableAgain(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	published := decode[engine.PublishResult](t,
		h.publish(t, map[string]any{"sku": "PALLET-EUR", "delta": 1}, http.StatusAccepted))

	h.expect(t, http.MethodPost, "/api/v1/queues/inventory-sync/consume",
		map[string]any{"consumer": "warehouse-worker"}, http.StatusOK)

	result := decode[map[string]any](t,
		h.expect(t, http.MethodPost, "/api/v1/messages/"+published.MessageIDs[0]+"/nack",
			map[string]any{"reason": "warehouse offline", "requeue": true}, http.StatusOK))

	if result["state"] != string(message.StateQueued) {
		t.Fatalf("expected QUEUED after requeue, got %v", result["state"])
	}
}

func TestNackWithoutRequeueSchedulesARetry(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	published := decode[engine.PublishResult](t,
		h.publish(t, map[string]any{"sku": "PALLET-EUR", "delta": 1}, http.StatusAccepted))

	h.expect(t, http.MethodPost, "/api/v1/queues/inventory-sync/consume",
		map[string]any{"consumer": "warehouse-worker"}, http.StatusOK)

	result := decode[map[string]any](t,
		h.expect(t, http.MethodPost, "/api/v1/messages/"+published.MessageIDs[0]+"/nack",
			map[string]any{"reason": "downstream unavailable"}, http.StatusOK))

	if result["state"] != string(message.StateRetrying) {
		t.Fatalf("expected RETRYING, got %v", result["state"])
	}
}

func TestDeadLetterInspectionAndReplay(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	published := decode[engine.PublishResult](t,
		h.publish(t, map[string]any{"sku": "PALLET-EUR", "delta": 1}, http.StatusAccepted))

	for range 2 {
		h.expect(t, http.MethodPost, "/api/v1/queues/inventory-sync/consume",
			map[string]any{"consumer": "warehouse-worker"}, http.StatusOK)

		h.expect(t, http.MethodPost, "/api/v1/messages/"+published.MessageIDs[0]+"/nack",
			map[string]any{"reason": "erp gateway timeout"}, http.StatusOK)

		time.Sleep(5 * time.Millisecond)
	}

	listing := decode[struct {
		DeadLetters []*storage.DeadLetter `json:"dead_letters"`
		Total       int                   `json:"total"`
	}](t, h.expect(t, http.MethodGet, "/api/v1/dead-letter?queue=inventory-sync", nil, http.StatusOK))

	if listing.Total != 1 || len(listing.DeadLetters) != 1 {
		t.Fatalf("expected one dead letter, got %+v", listing)
	}

	record := listing.DeadLetters[0]

	if record.Reason != "erp gateway timeout" || record.Attempts != 2 {
		t.Fatalf("unexpected dead letter %+v", record)
	}

	h.expect(t, http.MethodGet, "/api/v1/dead-letter/"+record.ID, nil, http.StatusOK)

	replay := decode[engine.ReplayResult](t,
		h.expect(t, http.MethodPost, "/api/v1/dead-letter/"+record.ID+"/retry", nil, http.StatusAccepted))

	if replay.MessageID == record.MessageID {
		t.Fatal("expected the replay to create a new message")
	}

	replayed := decode[map[string]any](t,
		h.expect(t, http.MethodGet, "/api/v1/messages/"+replay.MessageID, nil, http.StatusOK))

	if replayed["state"] != string(message.StateQueued) {
		t.Fatalf("expected the replayed message to be queued, got %v", replayed["state"])
	}

	h.expect(t, http.MethodDelete, "/api/v1/dead-letter/"+record.ID, nil, http.StatusNoContent)
	h.expect(t, http.MethodGet, "/api/v1/dead-letter/"+record.ID, nil, http.StatusNotFound)
}

func TestSchemaListing(t *testing.T) {
	h := newHarness(t)

	listing := decode[struct {
		Schemas []schema.Definition `json:"schemas"`
	}](t, h.expect(t, http.MethodGet, "/api/v1/schemas", nil, http.StatusOK))

	if len(listing.Schemas) != 1 || listing.Schemas[0].Name != "inventory-adjustment" {
		t.Fatalf("unexpected schema listing %+v", listing.Schemas)
	}
}

func TestPurgeAndDeleteQueue(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	h.publish(t, map[string]any{"sku": "PALLET-EUR", "delta": 1}, http.StatusAccepted)

	purged := decode[map[string]int](t,
		h.expect(t, http.MethodPost, "/api/v1/queues/inventory-sync/purge", nil, http.StatusOK))

	if purged["purged"] != 1 {
		t.Fatalf("expected one purged message, got %d", purged["purged"])
	}

	h.expect(t, http.MethodDelete, "/api/v1/queues/inventory-sync", nil, http.StatusConflict)

	h.expect(t, http.MethodDelete, "/api/v1/bindings", map[string]any{
		"exchange": "erp.events", "queue": "inventory-sync", "routing_key": "inventory.#",
	}, http.StatusNoContent)

	h.expect(t, http.MethodDelete, "/api/v1/queues/inventory-sync", nil, http.StatusNoContent)
	h.expect(t, http.MethodGet, "/api/v1/queues/inventory-sync", nil, http.StatusNotFound)
}

func TestConsumersEndpointReportsRegisteredConsumers(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	if _, err := h.engine.Subscribe(engine.ConsumerSpec{
		Name:        "inventory-worker",
		Queue:       "inventory-sync",
		Concurrency: 2,
		Prefetch:    4,
		Handler:     engine.HandlerFunc(func(context.Context, engine.Delivery) error { return nil }),
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	listing := decode[struct {
		Consumers []engine.ConsumerStatus `json:"consumers"`
	}](t, h.expect(t, http.MethodGet, "/api/v1/consumers", nil, http.StatusOK))

	if len(listing.Consumers) != 1 || listing.Consumers[0].Concurrency != 2 {
		t.Fatalf("unexpected consumer listing %+v", listing.Consumers)
	}
}

func TestUnknownRouteIsNotFound(t *testing.T) {
	h := newHarness(t)

	h.expect(t, http.MethodGet, "/api/v1/nope", nil, http.StatusNotFound)
}

func TestBrokerRejectsOversizedBodies(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	oversized := make([]byte, 2<<20)
	for i := range oversized {
		oversized[i] = 'a'
	}

	h.expect(t, http.MethodPost, "/api/v1/messages", map[string]any{
		"exchange":    "erp.events",
		"routing_key": "inventory.adjusted",
		"payload":     map[string]any{"sku": string(oversized), "delta": 1},
	}, http.StatusBadRequest)
}

func TestBrokerReportsExchangeBindings(t *testing.T) {
	h := newHarness(t)
	h.declareTopology(t)

	view := decode[struct {
		Bindings []broker.Binding `json:"bindings"`
	}](t, h.expect(t, http.MethodGet, "/api/v1/exchanges/erp.events", nil, http.StatusOK))

	if len(view.Bindings) != 1 || view.Bindings[0].RoutingKey != "inventory.#" {
		t.Fatalf("unexpected bindings %+v", view.Bindings)
	}
}
