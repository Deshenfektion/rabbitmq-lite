package schema

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

const invoiceSchema = `{
	"$schema": "https://json-schema.org/draft/2020-12/schema",
	"title": "Invoice",
	"x-schema-version": 2,
	"type": "object",
	"required": ["invoice_id", "customer_id", "currency", "total_net"],
	"additionalProperties": false,
	"properties": {
		"invoice_id": {"type": "string", "pattern": "^INV-[0-9]{4,}$"},
		"customer_id": {"type": "string", "minLength": 1},
		"currency": {"type": "string", "enum": ["EUR", "USD", "CHF"]},
		"total_net": {"type": "number", "minimum": 0},
		"due_date": {"type": "string", "format": "date"}
	}
}`

func newRegistry(t *testing.T) *Registry {
	t.Helper()

	registry := NewRegistry()

	if _, err := registry.Register("invoice", []byte(invoiceSchema)); err != nil {
		t.Fatalf("register: %v", err)
	}

	return registry
}

func TestRegisterExposesMetadata(t *testing.T) {
	registry := newRegistry(t)

	definitions := registry.Definitions()
	if len(definitions) != 1 {
		t.Fatalf("expected one definition, got %d", len(definitions))
	}

	if definitions[0].Name != "invoice" || definitions[0].Title != "Invoice" {
		t.Fatalf("unexpected definition %+v", definitions[0])
	}

	if definitions[0].Version != 2 {
		t.Fatalf("expected version 2, got %d", definitions[0].Version)
	}

	if !registry.Has("invoice") || registry.Has("unknown") {
		t.Fatal("unexpected registry membership")
	}
}

func TestValidateAcceptsConformingPayload(t *testing.T) {
	registry := newRegistry(t)

	payload := json.RawMessage(`{
		"invoice_id": "INV-10422",
		"customer_id": "C-100",
		"currency": "EUR",
		"total_net": 1299.5
	}`)

	if err := registry.Validate("invoice", payload); err != nil {
		t.Fatalf("expected payload to be accepted: %v", err)
	}
}

func TestValidateReportsMissingRequiredFields(t *testing.T) {
	registry := newRegistry(t)

	err := registry.Validate("invoice", json.RawMessage(`{"invoice_id": "INV-10422"}`))

	validation, ok := AsValidationError(err)
	if !ok {
		t.Fatalf("expected ValidationError, got %v", err)
	}

	if validation.Schema != "invoice" {
		t.Fatalf("unexpected schema %q", validation.Schema)
	}

	if len(validation.Violations) == 0 {
		t.Fatal("expected at least one violation")
	}
}

func TestValidateReportsFieldLevelViolations(t *testing.T) {
	registry := newRegistry(t)

	payload := json.RawMessage(`{
		"invoice_id": "10422",
		"customer_id": "C-100",
		"currency": "GBP",
		"total_net": -5
	}`)

	err := registry.Validate("invoice", payload)

	validation, ok := AsValidationError(err)
	if !ok {
		t.Fatalf("expected ValidationError, got %v", err)
	}

	paths := make(map[string]bool, len(validation.Violations))
	for _, violation := range validation.Violations {
		paths[violation.Path] = true
	}

	for _, expected := range []string{"/invoice_id", "/currency", "/total_net"} {
		if !paths[expected] {
			t.Errorf("expected a violation for %s, got %+v", expected, validation.Violations)
		}
	}
}

func TestValidateRejectsUnknownProperties(t *testing.T) {
	registry := newRegistry(t)

	payload := json.RawMessage(`{
		"invoice_id": "INV-10422",
		"customer_id": "C-100",
		"currency": "EUR",
		"total_net": 10,
		"legacy_field": "from the old export"
	}`)

	if err := registry.Validate("invoice", payload); err == nil {
		t.Fatal("expected additional properties to be rejected")
	}
}

func TestValidateRejectsMalformedJSON(t *testing.T) {
	registry := newRegistry(t)

	err := registry.Validate("invoice", json.RawMessage(`{"invoice_id":`))

	if _, ok := AsValidationError(err); !ok {
		t.Fatalf("expected ValidationError, got %v", err)
	}
}

func TestValidateRejectsEmptyPayload(t *testing.T) {
	registry := newRegistry(t)

	if err := registry.Validate("invoice", nil); err == nil {
		t.Fatal("expected empty payload to be rejected")
	}
}

func TestValidateWithoutSchemaNameIsANoop(t *testing.T) {
	registry := newRegistry(t)

	if err := registry.Validate("", json.RawMessage(`{"anything":true}`)); err != nil {
		t.Fatalf("expected unvalidated payloads to pass, got %v", err)
	}
}

func TestValidateUnknownSchema(t *testing.T) {
	registry := newRegistry(t)

	if err := registry.Validate("purchase-order", json.RawMessage(`{}`)); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("expected ErrSchemaNotFound, got %v", err)
	}
}

func TestRegisterRejectsInvalidSchemaDocuments(t *testing.T) {
	registry := NewRegistry()

	if _, err := registry.Register("broken", []byte(`{"type": 42}`)); err == nil {
		t.Fatal("expected an invalid schema document to be rejected")
	}

	if _, err := registry.Register("", []byte(invoiceSchema)); err == nil {
		t.Fatal("expected an empty schema name to be rejected")
	}
}

func TestLoadDirectory(t *testing.T) {
	root := t.TempDir()

	files := map[string]string{
		"invoice.schema.json":  invoiceSchema,
		"customer.schema.json": `{"type": "object", "required": ["customer_id"], "properties": {"customer_id": {"type": "string"}}}`,
		"notes.txt":            "ignored",
	}

	for name, content := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	registry := NewRegistry()

	loaded, err := registry.LoadDirectory(root)
	if err != nil {
		t.Fatalf("load directory: %v", err)
	}

	if len(loaded) != 2 {
		t.Fatalf("expected 2 schemas, got %d", len(loaded))
	}

	if loaded[0].Name != "customer" || loaded[1].Name != "invoice" {
		t.Fatalf("unexpected load order %+v", loaded)
	}

	if loaded[0].Source == "" {
		t.Fatal("expected the source path to be recorded")
	}

	if err := registry.Validate("customer", json.RawMessage(`{"customer_id":"C-1"}`)); err != nil {
		t.Fatalf("expected loaded schema to validate: %v", err)
	}
}
