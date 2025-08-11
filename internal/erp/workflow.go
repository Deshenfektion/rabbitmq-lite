package erp

import (
	"context"
	"fmt"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/engine"
)

type Workflow struct {
	Customers *CustomerSync
	Invoices  *InvoiceLedger
	Inventory *InventoryProjection
	Employees *EmployeeDirectory
	Audit     *AuditLog
}

type WorkflowOptions struct {
	Seed             uint64
	Latency          time.Duration
	GatewayFailure   float64
	Concurrency      int
	Prefetch         int
	KnownCostCenters []string
}

func (o WorkflowOptions) withDefaults() WorkflowOptions {
	if o.Concurrency < 1 {
		o.Concurrency = 4
	}

	if o.Prefetch < 1 {
		o.Prefetch = 8
	}

	return o
}

func DeclareTopology(ctx context.Context, instance *engine.Engine, visibilityTimeout time.Duration, maxAttempts int) error {
	if _, err := instance.DeclareExchange(ctx, broker.ExchangeSpec{
		Name: ExchangeEvents, Kind: broker.ExchangeTopic, Durable: true,
	}); err != nil {
		return err
	}

	queues := []struct {
		name       string
		schema     string
		routingKey string
	}{
		{"customer-sync", "customer", "customer.#"},
		{"invoice-processing", "invoice", "invoice.#"},
		{"inventory-sync", "inventory-adjustment", "inventory.#"},
		{"employee-sync", "employee", "employee.#"},
		{"audit-log", "", "#"},
	}

	for _, declaration := range queues {
		if _, err := instance.DeclareQueue(ctx, broker.QueueSpec{
			Name:              declaration.name,
			Durable:           true,
			MaxAttempts:       maxAttempts,
			VisibilityTimeout: visibilityTimeout,
			Schema:            declaration.schema,
		}); err != nil {
			return fmt.Errorf("erp: declare %s: %w", declaration.name, err)
		}

		if _, err := instance.Bind(ctx, broker.BindingSpec{
			Exchange:   ExchangeEvents,
			Queue:      declaration.name,
			RoutingKey: declaration.routingKey,
		}); err != nil {
			return fmt.Errorf("erp: bind %s: %w", declaration.name, err)
		}
	}

	return nil
}

func Subscribe(instance *engine.Engine, opts WorkflowOptions) (*Workflow, error) {
	opts = opts.withDefaults()

	workflow := &Workflow{
		Customers: NewCustomerSync(DownstreamOptions{
			Name: "crm", Latency: opts.Latency, FailureRate: opts.GatewayFailure, Seed: opts.Seed + 1,
		}),
		Invoices: NewInvoiceLedger(DownstreamOptions{
			Name: "ledger", Latency: opts.Latency, FailureRate: opts.GatewayFailure, Seed: opts.Seed + 2,
		}, "EUR", "CHF"),
		Inventory: NewInventoryProjection(DownstreamOptions{
			Name: "wms", Latency: opts.Latency, FailureRate: opts.GatewayFailure, Seed: opts.Seed + 3,
		}),
		Employees: NewEmployeeDirectory(DownstreamOptions{
			Name: "hr", Latency: opts.Latency, FailureRate: opts.GatewayFailure, Seed: opts.Seed + 4,
		}, opts.KnownCostCenters...),
		Audit: NewAuditLog(DownstreamOptions{Name: "audit", Seed: opts.Seed + 5}),
	}

	subscriptions := []struct {
		name    string
		queue   string
		handler engine.Handler
	}{
		{"crm-writer", "customer-sync", workflow.Customers},
		{"ledger-writer", "invoice-processing", workflow.Invoices},
		{"wms-projector", "inventory-sync", workflow.Inventory},
		{"hr-replicator", "employee-sync", workflow.Employees},
		{"audit-writer", "audit-log", workflow.Audit},
	}

	for _, subscription := range subscriptions {
		if _, err := instance.Subscribe(engine.ConsumerSpec{
			Name:        subscription.name,
			Queue:       subscription.queue,
			Concurrency: opts.Concurrency,
			Prefetch:    opts.Prefetch,
			Handler:     subscription.handler,
		}); err != nil {
			return nil, fmt.Errorf("erp: subscribe %s: %w", subscription.name, err)
		}
	}

	return workflow, nil
}

func (w *Workflow) Stats() []Stats {
	return []Stats{
		w.Customers.Stats(),
		w.Invoices.Stats(),
		w.Inventory.Stats(),
		w.Employees.Stats(),
		w.Audit.Stats(),
	}
}

func (g *Generator) NightlyExport(ctx context.Context, instance *engine.Engine, count int) (int, error) {
	published := 0

	for i := range count {
		var publication = g.CustomerExport()

		switch i % 4 {
		case 1:
			publication = g.Invoice()
		case 2:
			publication = g.InventoryAdjustment()
		case 3:
			publication = g.EmployeeChange()
		}

		if _, err := instance.Publish(ctx, publication); err != nil {
			return published, err
		}

		published++
	}

	return published, nil
}
