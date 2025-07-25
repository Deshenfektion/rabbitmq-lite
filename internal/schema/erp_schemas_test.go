package schema_test

import (
	"encoding/json"
	"testing"

	"github.com/deshenrao/rabbitmq-lite/internal/schema"
)

const schemaDirectory = "../../schemas"

func loadShippedSchemas(t *testing.T) *schema.Registry {
	t.Helper()

	registry := schema.NewRegistry()

	loaded, err := registry.LoadDirectory(schemaDirectory)
	if err != nil {
		t.Fatalf("load %s: %v", schemaDirectory, err)
	}

	if len(loaded) < 4 {
		t.Fatalf("expected the shipped erp schemas to be loaded, got %d", len(loaded))
	}

	return registry
}

func TestShippedSchemasCompile(t *testing.T) {
	registry := loadShippedSchemas(t)

	for _, name := range []string{"customer", "invoice", "inventory-adjustment", "employee"} {
		if !registry.Has(name) {
			t.Errorf("expected schema %q to be registered", name)
		}
	}
}

func TestCustomerExportPayload(t *testing.T) {
	registry := loadShippedSchemas(t)

	valid := json.RawMessage(`{
		"customer_id": "C-100244",
		"name": "Nordwind Logistik GmbH",
		"vat_id": "DE811907980",
		"country": "DE",
		"credit_limit": 25000,
		"payment_terms_days": 30,
		"blocked": false,
		"exported_at": "2025-07-25T02:15:00Z"
	}`)

	if err := registry.Validate("customer", valid); err != nil {
		t.Fatalf("expected the export payload to be accepted: %v", err)
	}

	invalid := json.RawMessage(`{
		"customer_id": "100244",
		"name": "",
		"country": "Germany",
		"exported_at": "2025-07-25T02:15:00Z"
	}`)

	if err := registry.Validate("customer", invalid); err == nil {
		t.Fatal("expected a malformed customer export to be rejected")
	}
}

func TestInvoicePayloadRequiresAtLeastOneLine(t *testing.T) {
	registry := loadShippedSchemas(t)

	empty := json.RawMessage(`{
		"invoice_id": "INV-104221",
		"customer_id": "C-100244",
		"currency": "EUR",
		"issued_on": "2025-07-25",
		"lines": []
	}`)

	if err := registry.Validate("invoice", empty); err == nil {
		t.Fatal("expected an invoice without lines to be rejected")
	}

	valid := json.RawMessage(`{
		"invoice_id": "INV-104221",
		"customer_id": "C-100244",
		"currency": "EUR",
		"issued_on": "2025-07-25",
		"due_on": "2025-08-24",
		"purchase_order": "PO-99120",
		"lines": [
			{"position": 1, "sku": "PALLET-EUR", "quantity": 12, "unit_price_net": 18.5, "tax_rate": 0.19},
			{"position": 2, "sku": "SHRINK-WRAP", "description": "500m roll", "quantity": 3, "unit_price_net": 42, "tax_rate": 0.19}
		]
	}`)

	if err := registry.Validate("invoice", valid); err != nil {
		t.Fatalf("expected the invoice to be accepted: %v", err)
	}
}

func TestInventoryAdjustmentReasonIsConstrained(t *testing.T) {
	registry := loadShippedSchemas(t)

	valid := json.RawMessage(`{
		"sku": "PALLET-EUR",
		"warehouse": "DE-01",
		"delta": -12,
		"reason": "goods_issue",
		"reference": "DN-55021",
		"recorded_at": "2025-07-25T06:00:00Z"
	}`)

	if err := registry.Validate("inventory-adjustment", valid); err != nil {
		t.Fatalf("expected the adjustment to be accepted: %v", err)
	}

	unknownReason := json.RawMessage(`{
		"sku": "PALLET-EUR",
		"warehouse": "DE-01",
		"delta": -12,
		"reason": "shrinkage",
		"recorded_at": "2025-07-25T06:00:00Z"
	}`)

	if err := registry.Validate("inventory-adjustment", unknownReason); err == nil {
		t.Fatal("expected an unknown movement reason to be rejected")
	}
}

func TestEmployeeSyncPayload(t *testing.T) {
	registry := loadShippedSchemas(t)

	valid := json.RawMessage(`{
		"employee_id": "E-004512",
		"cost_center": "CC-4200",
		"department": "Logistics",
		"status": "active",
		"weekly_hours": 38.5,
		"valid_from": "2025-08-01"
	}`)

	if err := registry.Validate("employee", valid); err != nil {
		t.Fatalf("expected the employee record to be accepted: %v", err)
	}

	overtime := json.RawMessage(`{
		"employee_id": "E-004512",
		"cost_center": "CC-4200",
		"status": "active",
		"weekly_hours": 95,
		"valid_from": "2025-08-01"
	}`)

	if err := registry.Validate("employee", overtime); err == nil {
		t.Fatal("expected an out of range working week to be rejected")
	}
}
