package erp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/engine"
)

var (
	ErrGatewayUnavailable  = errors.New("erp gateway unavailable")
	ErrUnsupportedCurrency = errors.New("ledger does not support this currency")
	ErrUnknownCostCenter   = errors.New("cost center is not present in the target system")
)

type DownstreamOptions struct {
	Name        string
	Latency     time.Duration
	FailureRate float64
	Seed        uint64
}

type Downstream struct {
	name        string
	latency     time.Duration
	failureRate float64

	mu     sync.Mutex
	random *rand.Rand

	handled atomic.Int64
	failed  atomic.Int64
}

type Stats struct {
	Name    string `json:"name"`
	Handled int64  `json:"handled"`
	Failed  int64  `json:"failed"`
}

func NewDownstream(opts DownstreamOptions) *Downstream {
	return &Downstream{
		name:        opts.Name,
		latency:     opts.Latency,
		failureRate: opts.FailureRate,
		random:      rand.New(rand.NewPCG(opts.Seed, opts.Seed^0x5851f42d4c957f2d)),
	}
}

func (d *Downstream) Stats() Stats {
	return Stats{Name: d.name, Handled: d.handled.Load(), Failed: d.failed.Load()}
}

func (d *Downstream) simulateCall(ctx context.Context) error {
	if d.latency > 0 {
		timer := time.NewTimer(d.latency)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	if d.unlucky() {
		d.failed.Add(1)
		return fmt.Errorf("%s: %w", d.name, ErrGatewayUnavailable)
	}

	return nil
}

func (d *Downstream) unlucky() bool {
	if d.failureRate <= 0 {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	return d.random.Float64() < d.failureRate
}

type CustomerSync struct {
	*Downstream

	mu      sync.Mutex
	records map[string]CustomerExport
}

func NewCustomerSync(opts DownstreamOptions) *CustomerSync {
	return &CustomerSync{
		Downstream: NewDownstream(opts),
		records:    make(map[string]CustomerExport),
	}
}

func (c *CustomerSync) Handle(ctx context.Context, delivery engine.Delivery) error {
	var customer CustomerExport
	if err := json.Unmarshal(delivery.Message.Payload, &customer); err != nil {
		return engine.Permanent(fmt.Errorf("customer-sync: undecodable payload: %w", err))
	}

	if err := c.simulateCall(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	c.records[customer.CustomerID] = customer
	c.mu.Unlock()

	c.handled.Add(1)

	return nil
}

func (c *CustomerSync) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.records)
}

type InvoiceLedger struct {
	*Downstream

	supported map[string]bool

	mu    sync.Mutex
	total map[string]float64
}

func NewInvoiceLedger(opts DownstreamOptions, supportedCurrencies ...string) *InvoiceLedger {
	supported := make(map[string]bool, len(supportedCurrencies))
	for _, currency := range supportedCurrencies {
		supported[currency] = true
	}

	return &InvoiceLedger{
		Downstream: NewDownstream(opts),
		supported:  supported,
		total:      make(map[string]float64),
	}
}

func (l *InvoiceLedger) Handle(ctx context.Context, delivery engine.Delivery) error {
	var invoice Invoice
	if err := json.Unmarshal(delivery.Message.Payload, &invoice); err != nil {
		return engine.Permanent(fmt.Errorf("invoice-ledger: undecodable payload: %w", err))
	}

	if len(l.supported) > 0 && !l.supported[invoice.Currency] {
		return engine.Permanent(fmt.Errorf("invoice-ledger: %w: %s", ErrUnsupportedCurrency, invoice.Currency))
	}

	if err := l.simulateCall(ctx); err != nil {
		return err
	}

	l.mu.Lock()
	l.total[invoice.Currency] += invoice.TotalNet()
	l.mu.Unlock()

	l.handled.Add(1)

	return nil
}

func (l *InvoiceLedger) Totals() map[string]float64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	totals := make(map[string]float64, len(l.total))
	for currency, amount := range l.total {
		totals[currency] = amount
	}

	return totals
}

type InventoryProjection struct {
	*Downstream

	mu    sync.Mutex
	stock map[string]int
}

func NewInventoryProjection(opts DownstreamOptions) *InventoryProjection {
	return &InventoryProjection{
		Downstream: NewDownstream(opts),
		stock:      make(map[string]int),
	}
}

func (p *InventoryProjection) Handle(ctx context.Context, delivery engine.Delivery) error {
	var adjustment InventoryAdjustment
	if err := json.Unmarshal(delivery.Message.Payload, &adjustment); err != nil {
		return engine.Permanent(fmt.Errorf("inventory-sync: undecodable payload: %w", err))
	}

	if err := p.simulateCall(ctx); err != nil {
		return err
	}

	key := adjustment.Warehouse + "/" + adjustment.SKU

	p.mu.Lock()
	p.stock[key] += adjustment.Delta
	p.mu.Unlock()

	p.handled.Add(1)

	return nil
}

func (p *InventoryProjection) Stock(warehouse, sku string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.stock[warehouse+"/"+sku]
}

type EmployeeDirectory struct {
	*Downstream

	knownCostCenters map[string]bool

	mu      sync.Mutex
	records map[string]EmployeeChange
}

func NewEmployeeDirectory(opts DownstreamOptions, costCenters ...string) *EmployeeDirectory {
	known := make(map[string]bool, len(costCenters))
	for _, costCenter := range costCenters {
		known[costCenter] = true
	}

	return &EmployeeDirectory{
		Downstream:       NewDownstream(opts),
		knownCostCenters: known,
		records:          make(map[string]EmployeeChange),
	}
}

func (d *EmployeeDirectory) Handle(ctx context.Context, delivery engine.Delivery) error {
	var employee EmployeeChange
	if err := json.Unmarshal(delivery.Message.Payload, &employee); err != nil {
		return engine.Permanent(fmt.Errorf("employee-sync: undecodable payload: %w", err))
	}

	if len(d.knownCostCenters) > 0 && !d.knownCostCenters[employee.CostCenter] {
		return engine.Permanent(fmt.Errorf("employee-sync: %w: %s", ErrUnknownCostCenter, employee.CostCenter))
	}

	if err := d.simulateCall(ctx); err != nil {
		return err
	}

	d.mu.Lock()
	d.records[employee.EmployeeID] = employee
	d.mu.Unlock()

	d.handled.Add(1)

	return nil
}

type AuditLog struct {
	*Downstream

	mu      sync.Mutex
	entries []string
}

func NewAuditLog(opts DownstreamOptions) *AuditLog {
	return &AuditLog{Downstream: NewDownstream(opts)}
}

func (a *AuditLog) Handle(ctx context.Context, delivery engine.Delivery) error {
	if err := a.simulateCall(ctx); err != nil {
		return err
	}

	a.mu.Lock()
	a.entries = append(a.entries, delivery.Message.RoutingKey+" "+delivery.Message.ID)
	a.mu.Unlock()

	a.handled.Add(1)

	return nil
}

func (a *AuditLog) Entries() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.entries)
}
