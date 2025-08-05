package metrics

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func render(t *testing.T, registry *Registry) string {
	t.Helper()

	builder := &strings.Builder{}
	if err := registry.Write(builder); err != nil {
		t.Fatalf("write: %v", err)
	}

	return builder.String()
}

func TestCounterSeriesAreKeyedByLabels(t *testing.T) {
	registry := NewRegistry()

	registry.Counter("messages_total", "Messages.", Labels{"queue": "a"}).Inc()
	registry.Counter("messages_total", "Messages.", Labels{"queue": "a"}).Add(2)
	registry.Counter("messages_total", "Messages.", Labels{"queue": "b"}).Inc()

	output := render(t, registry)

	if !strings.Contains(output, `messages_total{queue="a"} 3`) {
		t.Errorf("expected queue a to accumulate, got:\n%s", output)
	}

	if !strings.Contains(output, `messages_total{queue="b"} 1`) {
		t.Errorf("expected queue b to be tracked separately, got:\n%s", output)
	}

	if !strings.Contains(output, "# TYPE messages_total counter") {
		t.Errorf("expected a type header, got:\n%s", output)
	}
}

func TestGaugeTracksLatestValue(t *testing.T) {
	registry := NewRegistry()

	gauge := registry.Gauge("queue_depth", "Depth.", Labels{"queue": "invoice"})
	gauge.Set(10)
	gauge.Add(-4)

	if gauge.Value() != 6 {
		t.Fatalf("expected 6, got %d", gauge.Value())
	}

	if !strings.Contains(render(t, registry), `queue_depth{queue="invoice"} 6`) {
		t.Fatal("expected the gauge to be exported")
	}
}

func TestHistogramBucketsAreCumulative(t *testing.T) {
	registry := NewRegistry()

	histogram := registry.Histogram("latency_seconds", "Latency.", []float64{0.01, 0.1, 1}, nil)

	for _, value := range []float64{0.005, 0.05, 0.5, 5} {
		histogram.Observe(value)
	}

	output := render(t, registry)

	expectations := []string{
		`latency_seconds_bucket{le="0.01"} 1`,
		`latency_seconds_bucket{le="0.1"} 2`,
		`latency_seconds_bucket{le="1"} 3`,
		`latency_seconds_bucket{le="+Inf"} 4`,
		`latency_seconds_count 4`,
	}

	for _, expected := range expectations {
		if !strings.Contains(output, expected) {
			t.Errorf("expected %q in:\n%s", expected, output)
		}
	}
}

func TestHistogramQuantileUsesBucketBounds(t *testing.T) {
	registry := NewRegistry()

	histogram := registry.Histogram("latency_seconds", "Latency.", []float64{0.01, 0.05, 0.1}, nil)

	for range 90 {
		histogram.ObserveDuration(5 * time.Millisecond)
	}

	for range 10 {
		histogram.ObserveDuration(80 * time.Millisecond)
	}

	if got := histogram.Quantile(0.5); got != 0.01 {
		t.Fatalf("expected the median in the first bucket, got %v", got)
	}

	if got := histogram.Quantile(0.99); got != 0.1 {
		t.Fatalf("expected the tail in the last bucket, got %v", got)
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	registry := NewRegistry()

	registry.Counter("errors_total", "Errors.", Labels{"reason": `he said "no"`}).Inc()

	if !strings.Contains(render(t, registry), `reason="he said \"no\""`) {
		t.Fatalf("expected quotes to be escaped:\n%s", render(t, registry))
	}
}

func TestOutputIsStable(t *testing.T) {
	registry := NewRegistry()

	registry.Counter("b_total", "B.", Labels{"queue": "z"}).Inc()
	registry.Counter("a_total", "A.", Labels{"queue": "y"}).Inc()
	registry.Counter("a_total", "A.", Labels{"queue": "x"}).Inc()

	first := render(t, registry)

	for range 5 {
		if render(t, registry) != first {
			t.Fatal("expected deterministic exposition output")
		}
	}

	if strings.Index(first, "a_total") > strings.Index(first, "b_total") {
		t.Fatal("expected metric families to be sorted by name")
	}
}

func TestConcurrentUpdatesAreSafe(t *testing.T) {
	registry := NewRegistry()

	var group sync.WaitGroup

	for range 8 {
		group.Add(1)

		go func() {
			defer group.Done()

			for range 500 {
				registry.Counter("messages_total", "Messages.", Labels{"queue": "shared"}).Inc()
				registry.Histogram("latency_seconds", "Latency.", nil, nil).Observe(0.002)
			}
		}()
	}

	group.Wait()

	if got := registry.Counter("messages_total", "Messages.", Labels{"queue": "shared"}).Value(); got != 4000 {
		t.Fatalf("expected 4000 increments, got %d", got)
	}

	if got := registry.Histogram("latency_seconds", "Latency.", nil, nil).Count(); got != 4000 {
		t.Fatalf("expected 4000 observations, got %d", got)
	}
}

func TestCollectorExportsBrokerInstruments(t *testing.T) {
	collector := NewCollector(NewRegistry())

	collector.MessagePublished("invoice-processing", 3*time.Millisecond)
	collector.MessageDelivered("invoice-processing")
	collector.MessageAcknowledged("invoice-processing", 12*time.Millisecond)
	collector.MessageRetried("invoice-processing")
	collector.MessageDeadLettered("invoice-processing", "handler")
	collector.MessageRejected("invoice-processing")
	collector.QueueDepth("invoice-processing", 4, 2, 1)

	output := render(t, collector.Registry())

	expectations := []string{
		`rabbitmq_lite_messages_published_total{queue="invoice-processing"} 1`,
		`rabbitmq_lite_messages_dead_lettered_total{kind="handler",queue="invoice-processing"} 1`,
		`rabbitmq_lite_queue_depth{queue="invoice-processing"} 4`,
		`rabbitmq_lite_queue_in_flight{queue="invoice-processing"} 2`,
		`rabbitmq_lite_queue_scheduled{queue="invoice-processing"} 1`,
		"rabbitmq_lite_publish_duration_seconds_count 1",
	}

	for _, expected := range expectations {
		if !strings.Contains(output, expected) {
			t.Errorf("expected %q in:\n%s", expected, output)
		}
	}
}
