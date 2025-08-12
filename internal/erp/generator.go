package erp

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
)

const (
	ExchangeEvents = "erp.events"

	RoutingCustomerCreated  = "customer.created"
	RoutingCustomerUpdated  = "customer.updated"
	RoutingInvoiceIssued    = "invoice.issued"
	RoutingInventoryAdjust  = "inventory.adjusted"
	RoutingEmployeeModified = "employee.modified"
)

var (
	companySuffixes = []string{"GmbH", "AG", "SE", "KG", "BV", "Sarl"}
	companyNames    = []string{"Nordwind Logistik", "Rheinstahl", "Alpenblick Handel", "Hafenkontor", "Weserwerk", "Donaufracht"}
	countries       = []string{"DE", "AT", "CH", "NL", "FR"}
	currencies      = []string{"EUR", "EUR", "EUR", "CHF", "USD"}
	warehouses      = []string{"DE-01", "DE-02", "AT-01", "NL-03"}
	movementReasons = []string{"goods_receipt", "goods_issue", "stocktake", "scrap", "return"}
	skus            = []string{"PALLET-EUR", "SHRINK-WRAP", "CRATE-120", "LABEL-A4", "STRAP-25MM"}
	departments     = []string{"Logistics", "Finance", "Procurement", "Field Service"}
	employeeStates  = []string{"active", "active", "active", "on_leave", "terminated"}
)

type Generator struct {
	random *rand.Rand
	clock  func() time.Time
}

func NewGenerator(seed uint64) *Generator {
	return &Generator{
		random: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		clock:  func() time.Time { return time.Now().UTC() },
	}
}

func (g *Generator) WithClock(clock func() time.Time) *Generator {
	g.clock = clock
	return g
}

func (g *Generator) CustomerExport() message.Publication {
	customer := CustomerExport{
		CustomerID:       fmt.Sprintf("C-%06d", 100000+g.random.IntN(899999)),
		Name:             pick(g.random, companyNames) + " " + pick(g.random, companySuffixes),
		Country:          pick(g.random, countries),
		CreditLimit:      float64((g.random.IntN(40) + 1) * 2500),
		PaymentTermsDays: pick(g.random, []int{14, 30, 45, 60}),
		Blocked:          g.random.IntN(20) == 0,
		ExportedAt:       g.clock(),
	}

	if customer.Country == "DE" {
		customer.VatID = fmt.Sprintf("DE%09d", 100000000+g.random.IntN(899999999))
	}

	routingKey := RoutingCustomerCreated
	if g.random.IntN(3) > 0 {
		routingKey = RoutingCustomerUpdated
	}

	return g.publication(routingKey, customer, map[string]string{
		"erp-module": "SD",
		"tenant":     "acme",
	})
}

func (g *Generator) Invoice() message.Publication {
	issued := g.clock()
	lineCount := 1 + g.random.IntN(4)
	lines := make([]InvoiceLine, 0, lineCount)

	for i := range lineCount {
		lines = append(lines, InvoiceLine{
			Position:     i + 1,
			SKU:          pick(g.random, skus),
			Quantity:     float64(1 + g.random.IntN(48)),
			UnitPriceNet: float64(g.random.IntN(9000)+100) / 100,
			TaxRate:      pick(g.random, []float64{0.07, 0.19}),
		})
	}

	invoice := Invoice{
		InvoiceID:     fmt.Sprintf("INV-%07d", 1000000+g.random.IntN(8999999)),
		CustomerID:    fmt.Sprintf("C-%06d", 100000+g.random.IntN(899999)),
		Currency:      pick(g.random, currencies),
		IssuedOn:      issued.Format(time.DateOnly),
		DueOn:         issued.AddDate(0, 0, 30).Format(time.DateOnly),
		PurchaseOrder: fmt.Sprintf("PO-%05d", g.random.IntN(99999)),
		Lines:         lines,
	}

	return g.publication(RoutingInvoiceIssued, invoice, map[string]string{
		"erp-module": "FI",
		"tenant":     "acme",
	})
}

func (g *Generator) InventoryAdjustment() message.Publication {
	reason := pick(g.random, movementReasons)

	delta := 1 + g.random.IntN(80)
	if reason == "goods_issue" || reason == "scrap" {
		delta = -delta
	}

	adjustment := InventoryAdjustment{
		SKU:        pick(g.random, skus),
		Warehouse:  pick(g.random, warehouses),
		Delta:      delta,
		Reason:     reason,
		Reference:  fmt.Sprintf("DN-%05d", g.random.IntN(99999)),
		RecordedAt: g.clock(),
	}

	return g.publication(RoutingInventoryAdjust, adjustment, map[string]string{
		"erp-module": "MM",
		"tenant":     "acme",
	})
}

func (g *Generator) EmployeeChange() message.Publication {
	validFrom := g.clock().AddDate(0, 0, g.random.IntN(30))

	employee := EmployeeChange{
		EmployeeID:  fmt.Sprintf("E-%06d", 1000+g.random.IntN(98999)),
		CostCenter:  fmt.Sprintf("CC-%04d", 1000+g.random.IntN(8999)),
		Department:  pick(g.random, departments),
		Status:      pick(g.random, employeeStates),
		WeeklyHours: pick(g.random, []float64{20, 30, 38.5, 40}),
		ValidFrom:   validFrom.Format(time.DateOnly),
	}

	if employee.Status == "terminated" {
		employee.ValidTo = validFrom.AddDate(0, 1, 0).Format(time.DateOnly)
	}

	return g.publication(RoutingEmployeeModified, employee, map[string]string{
		"erp-module": "HR",
		"tenant":     "acme",
	})
}

func (g *Generator) MalformedInvoice() message.Publication {
	broken := map[string]any{
		"invoice_id":  "10422",
		"customer_id": "C-100244",
		"currency":    "GBP",
		"issued_on":   g.clock().Format(time.DateOnly),
		"lines":       []any{},
	}

	return g.publication(RoutingInvoiceIssued, broken, map[string]string{
		"erp-module":  "FI",
		"tenant":      "acme",
		"export-file": "legacy_invoice_batch.csv",
	})
}

func (g *Generator) publication(routingKey string, payload any, headers map[string]string) message.Publication {
	encoded, err := json.Marshal(payload)
	if err != nil {
		encoded = []byte(`{}`)
	}

	return message.Publication{
		Exchange:   ExchangeEvents,
		RoutingKey: routingKey,
		Payload:    encoded,
		Headers:    headers,
	}
}

func pick[T any](random *rand.Rand, values []T) T {
	return values[random.IntN(len(values))]
}
