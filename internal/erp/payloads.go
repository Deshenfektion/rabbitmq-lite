package erp

import "time"

type CustomerExport struct {
	CustomerID       string    `json:"customer_id"`
	Name             string    `json:"name"`
	VatID            string    `json:"vat_id,omitempty"`
	Country          string    `json:"country"`
	CreditLimit      float64   `json:"credit_limit"`
	PaymentTermsDays int       `json:"payment_terms_days"`
	Blocked          bool      `json:"blocked"`
	ExportedAt       time.Time `json:"exported_at"`
}

type InvoiceLine struct {
	Position     int     `json:"position"`
	SKU          string  `json:"sku"`
	Description  string  `json:"description,omitempty"`
	Quantity     float64 `json:"quantity"`
	UnitPriceNet float64 `json:"unit_price_net"`
	TaxRate      float64 `json:"tax_rate"`
}

type Invoice struct {
	InvoiceID     string        `json:"invoice_id"`
	CustomerID    string        `json:"customer_id"`
	Currency      string        `json:"currency"`
	IssuedOn      string        `json:"issued_on"`
	DueOn         string        `json:"due_on,omitempty"`
	PurchaseOrder string        `json:"purchase_order,omitempty"`
	Lines         []InvoiceLine `json:"lines"`
}

type InventoryAdjustment struct {
	SKU        string    `json:"sku"`
	Warehouse  string    `json:"warehouse"`
	Delta      int       `json:"delta"`
	Reason     string    `json:"reason"`
	Reference  string    `json:"reference,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

type EmployeeChange struct {
	EmployeeID  string  `json:"employee_id"`
	CostCenter  string  `json:"cost_center"`
	Department  string  `json:"department,omitempty"`
	Status      string  `json:"status"`
	WeeklyHours float64 `json:"weekly_hours"`
	ValidFrom   string  `json:"valid_from"`
	ValidTo     string  `json:"valid_to,omitempty"`
}

func (i Invoice) TotalNet() float64 {
	total := 0.0

	for _, line := range i.Lines {
		total += line.Quantity * line.UnitPriceNet
	}

	return total
}
