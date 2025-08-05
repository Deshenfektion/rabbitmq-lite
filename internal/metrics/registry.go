package metrics

import (
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Labels map[string]string

type Registry struct {
	mu         sync.RWMutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
	help       map[string]string
}

type Counter struct {
	name   string
	labels Labels
	value  atomic.Int64
}

type Gauge struct {
	name   string
	labels Labels
	value  atomic.Int64
}

type Histogram struct {
	name    string
	labels  Labels
	bounds  []float64
	mu      sync.Mutex
	buckets []uint64
	count   uint64
	sum     float64
}

var DefaultBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
		help:       make(map[string]string),
	}
}

func (r *Registry) Counter(name, help string, labels Labels) *Counter {
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.help[name] = help

	if existing, ok := r.counters[key]; ok {
		return existing
	}

	counter := &Counter{name: name, labels: maps.Clone(labels)}
	r.counters[key] = counter

	return counter
}

func (r *Registry) Gauge(name, help string, labels Labels) *Gauge {
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.help[name] = help

	if existing, ok := r.gauges[key]; ok {
		return existing
	}

	gauge := &Gauge{name: name, labels: maps.Clone(labels)}
	r.gauges[key] = gauge

	return gauge
}

func (r *Registry) Histogram(name, help string, bounds []float64, labels Labels) *Histogram {
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.help[name] = help

	if existing, ok := r.histograms[key]; ok {
		return existing
	}

	if len(bounds) == 0 {
		bounds = DefaultBuckets
	}

	histogram := &Histogram{
		name:    name,
		labels:  maps.Clone(labels),
		bounds:  slices.Clone(bounds),
		buckets: make([]uint64, len(bounds)),
	}

	r.histograms[key] = histogram

	return histogram
}

func (c *Counter) Inc() {
	c.value.Add(1)
}

func (c *Counter) Add(delta int64) {
	c.value.Add(delta)
}

func (c *Counter) Value() int64 {
	return c.value.Load()
}

func (g *Gauge) Set(value int64) {
	g.value.Store(value)
}

func (g *Gauge) Add(delta int64) {
	g.value.Add(delta)
}

func (g *Gauge) Value() int64 {
	return g.value.Load()
}

func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.count++
	h.sum += value

	for i, bound := range h.bounds {
		if value <= bound {
			h.buckets[i]++
		}
	}
}

func (h *Histogram) ObserveDuration(elapsed time.Duration) {
	h.Observe(elapsed.Seconds())
}

func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.count
}

func (h *Histogram) Quantile(quantile float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.count == 0 {
		return 0
	}

	target := quantile * float64(h.count)
	for i, bound := range h.bounds {
		if float64(h.buckets[i]) >= target {
			return bound
		}
	}

	return math.Inf(1)
}

func (r *Registry) Write(w io.Writer) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	builder := &strings.Builder{}

	for _, name := range r.names() {
		switch {
		case hasSeries(r.counters, name):
			writeHeader(builder, name, r.help[name], "counter")
			for _, key := range sortedKeys(r.counters, name) {
				counter := r.counters[key]
				fmt.Fprintf(builder, "%s %d\n", renderSeries(counter.name, counter.labels), counter.Value())
			}
		case hasSeries(r.gauges, name):
			writeHeader(builder, name, r.help[name], "gauge")
			for _, key := range sortedKeys(r.gauges, name) {
				gauge := r.gauges[key]
				fmt.Fprintf(builder, "%s %d\n", renderSeries(gauge.name, gauge.labels), gauge.Value())
			}
		case hasSeries(r.histograms, name):
			writeHeader(builder, name, r.help[name], "histogram")
			for _, key := range sortedKeys(r.histograms, name) {
				writeHistogram(builder, r.histograms[key])
			}
		}

		builder.WriteString("\n")
	}

	_, err := io.WriteString(w, builder.String())

	return err
}

func (r *Registry) names() []string {
	seen := make(map[string]struct{}, len(r.help))
	for name := range r.help {
		seen[name] = struct{}{}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

func writeHeader(builder *strings.Builder, name, help, kind string) {
	if help != "" {
		fmt.Fprintf(builder, "# HELP %s %s\n", name, help)
	}

	fmt.Fprintf(builder, "# TYPE %s %s\n", name, kind)
}

func writeHistogram(builder *strings.Builder, histogram *Histogram) {
	histogram.mu.Lock()
	defer histogram.mu.Unlock()

	for i, bound := range histogram.bounds {
		labels := maps.Clone(histogram.labels)
		if labels == nil {
			labels = Labels{}
		}

		labels["le"] = strconv.FormatFloat(bound, 'g', -1, 64)

		fmt.Fprintf(builder, "%s %d\n", renderSeries(histogram.name+"_bucket", labels), histogram.buckets[i])
	}

	infinite := maps.Clone(histogram.labels)
	if infinite == nil {
		infinite = Labels{}
	}

	infinite["le"] = "+Inf"

	fmt.Fprintf(builder, "%s %d\n", renderSeries(histogram.name+"_bucket", infinite), histogram.count)
	fmt.Fprintf(builder, "%s %g\n", renderSeries(histogram.name+"_sum", histogram.labels), histogram.sum)
	fmt.Fprintf(builder, "%s %d\n", renderSeries(histogram.name+"_count", histogram.labels), histogram.count)
}

func hasSeries[T any](series map[string]*T, name string) bool {
	prefix := name + "{"

	for key := range series {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	return false
}

func sortedKeys[T any](series map[string]*T, name string) []string {
	prefix := name + "{"
	keys := make([]string, 0, len(series))

	for key := range series {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	return keys
}

func seriesKey(name string, labels Labels) string {
	return name + "{" + renderLabels(labels) + "}"
}

func renderSeries(name string, labels Labels) string {
	rendered := renderLabels(labels)
	if rendered == "" {
		return name
	}

	return name + "{" + rendered + "}"
}

func renderLabels(labels Labels) string {
	if len(labels) == 0 {
		return ""
	}

	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}

	sort.Strings(names)

	pairs := make([]string, 0, len(names))
	for _, name := range names {
		pairs = append(pairs, fmt.Sprintf("%s=%q", name, labels[name]))
	}

	return strings.Join(pairs, ",")
}
