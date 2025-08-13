package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/engine"
	"github.com/deshenrao/rabbitmq-lite/internal/erp"
	"github.com/deshenrao/rabbitmq-lite/internal/logging"
	"github.com/deshenrao/rabbitmq-lite/internal/retry"
	"github.com/deshenrao/rabbitmq-lite/internal/schema"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/memory"
)

type options struct {
	events         int
	seed           uint64
	failureRate    float64
	latency        time.Duration
	concurrency    int
	prefetch       int
	maxAttempts    int
	schemaDir      string
	logLevel       string
	includeBadFile bool
	timeout        time.Duration
}

func main() {
	opts := options{}

	flag.IntVar(&opts.events, "events", 200, "number of ERP events to publish")
	flag.Uint64Var(&opts.seed, "seed", 20250812, "seed for the deterministic payload generator")
	flag.Float64Var(&opts.failureRate, "failure-rate", 0.15, "probability that a downstream call fails transiently")
	flag.DurationVar(&opts.latency, "latency", 2*time.Millisecond, "simulated downstream call latency")
	flag.IntVar(&opts.concurrency, "concurrency", 4, "workers per consumer")
	flag.IntVar(&opts.prefetch, "prefetch", 16, "messages prefetched per consumer")
	flag.IntVar(&opts.maxAttempts, "max-attempts", 5, "delivery attempts before dead lettering")
	flag.StringVar(&opts.schemaDir, "schemas", "schemas", "directory holding the ERP JSON schemas")
	flag.StringVar(&opts.logLevel, "log-level", "warn", "log level for the embedded broker")
	flag.BoolVar(&opts.includeBadFile, "include-malformed", true, "publish a malformed legacy export to exercise validation")
	flag.DurationVar(&opts.timeout, "timeout", time.Minute, "how long to wait for the export to drain")
	flag.Parse()

	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, "erpsim:", err)
		os.Exit(1)
	}
}

func run(opts options) error {
	logger, err := logging.New(os.Stderr, logging.Options{Level: opts.logLevel, Format: "text", Service: "erpsim"})
	if err != nil {
		return err
	}

	registry := schema.NewRegistry()
	if _, err := registry.LoadDirectory(opts.schemaDir); err != nil {
		return err
	}

	store := memory.New()
	defer store.Close()

	instance, err := engine.New(engine.Options{
		Store:   store,
		Schemas: registry,
		Logger:  logger,
		Retry: retry.Policy{
			MaxAttempts:     opts.maxAttempts,
			InitialInterval: 10 * time.Millisecond,
			MaxInterval:     250 * time.Millisecond,
			Multiplier:      2,
			JitterFraction:  0.2,
		},
		ReclaimInterval: time.Second,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()

	instance.Start(ctx)

	if err := erp.DeclareTopology(ctx, instance, 15*time.Second, opts.maxAttempts); err != nil {
		return err
	}

	workflow, err := erp.Subscribe(instance, erp.WorkflowOptions{
		Seed:           opts.seed,
		Latency:        opts.latency,
		GatewayFailure: opts.failureRate,
		Concurrency:    opts.concurrency,
		Prefetch:       opts.prefetch,
	})
	if err != nil {
		return err
	}

	generator := erp.NewGenerator(opts.seed)
	started := time.Now()

	published, err := generator.NightlyExport(ctx, instance, opts.events)
	if err != nil {
		return err
	}

	rejected := 0

	if opts.includeBadFile {
		if _, err := instance.Publish(ctx, generator.MalformedInvoice()); err != nil {
			rejected++
		}
	}

	if err := waitForDrain(ctx, instance); err != nil {
		return err
	}

	elapsed := time.Since(started)

	report(instance, workflow, published, rejected, elapsed)

	shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()

	return instance.Shutdown(shutdownCtx)
}

func waitForDrain(ctx context.Context, instance *engine.Engine) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("the export did not drain within the timeout: %w", ctx.Err())
		case <-ticker.C:
			drained := true

			for _, queue := range instance.Registry().QueueNames() {
				depth, err := instance.Depth(ctx, queue)
				if err != nil {
					return err
				}

				if depth.Total != 0 {
					drained = false
					break
				}
			}

			if drained {
				return nil
			}
		}
	}
}

func report(instance *engine.Engine, workflow *erp.Workflow, published, rejected int, elapsed time.Duration) {
	ctx := context.Background()

	fmt.Printf("\nnightly ERP export replayed in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("published events   : %d\n", published)
	fmt.Printf("rejected at publish: %d\n", rejected)
	fmt.Printf("throughput         : %.0f messages/s\n\n", float64(published)/elapsed.Seconds())

	writer := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)

	fmt.Fprintln(writer, "DOWNSTREAM\tHANDLED\tTRANSIENT FAILURES")
	for _, stats := range workflow.Stats() {
		fmt.Fprintf(writer, "%s\t%d\t%d\n", stats.Name, stats.Handled, stats.Failed)
	}

	fmt.Fprintln(writer, "\nQUEUE\tREADY\tIN FLIGHT\tSCHEDULED\tDEAD LETTERED")
	for _, queue := range instance.Registry().QueueNames() {
		depth, err := instance.Depth(ctx, queue)
		if err != nil {
			continue
		}

		dead, _ := instance.CountDeadLetters(ctx, queue)

		fmt.Fprintf(writer, "%s\t%d\t%d\t%d\t%d\n", queue, depth.Ready, depth.InFlight, depth.Scheduled, dead)
	}

	_ = writer.Flush()

	entries, err := instance.DeadLetters(ctx, storage.DeadLetterFilter{Limit: 5})
	if err != nil || len(entries) == 0 {
		fmt.Println()
		return
	}

	fmt.Println("\nmost recent dead letters:")

	for _, entry := range entries {
		fmt.Printf("  %s  %-20s  %-10s  attempts=%d  %s\n",
			entry.ID[:12], entry.Queue, entry.ErrorKind, entry.Attempts, truncate(entry.Reason, 60))
	}

	fmt.Println()
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}

	return value[:limit-1] + "…"
}
