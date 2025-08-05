package metrics

import (
	"time"
)

const (
	metricPublished    = "rabbitmq_lite_messages_published_total"
	metricDelivered    = "rabbitmq_lite_messages_delivered_total"
	metricAcknowledged = "rabbitmq_lite_messages_acknowledged_total"
	metricRetried      = "rabbitmq_lite_messages_retried_total"
	metricDeadLettered = "rabbitmq_lite_messages_dead_lettered_total"
	metricRejected     = "rabbitmq_lite_messages_rejected_total"
	metricPublishTime  = "rabbitmq_lite_publish_duration_seconds"
	metricHandleTime   = "rabbitmq_lite_handler_duration_seconds"
	metricQueueDepth   = "rabbitmq_lite_queue_depth"
	metricInFlight     = "rabbitmq_lite_queue_in_flight"
	metricScheduled    = "rabbitmq_lite_queue_scheduled"
)

type Collector struct {
	registry *Registry
}

func NewCollector(registry *Registry) *Collector {
	if registry == nil {
		registry = NewRegistry()
	}

	return &Collector{registry: registry}
}

func (c *Collector) Registry() *Registry {
	return c.registry
}

func (c *Collector) MessagePublished(queue string, latency time.Duration) {
	c.registry.Counter(metricPublished, "Messages accepted by the broker and written to a queue.", Labels{"queue": queue}).Inc()
	c.registry.Histogram(metricPublishTime, "Time spent routing, validating and persisting a publication.", DefaultBuckets, nil).ObserveDuration(latency)
}

func (c *Collector) MessageDelivered(queue string) {
	c.registry.Counter(metricDelivered, "Deliveries handed to a consumer.", Labels{"queue": queue}).Inc()
}

func (c *Collector) MessageAcknowledged(queue string, latency time.Duration) {
	c.registry.Counter(metricAcknowledged, "Deliveries acknowledged by a consumer.", Labels{"queue": queue}).Inc()
	c.registry.Histogram(metricHandleTime, "Time a consumer spent handling a delivery.", DefaultBuckets, Labels{"queue": queue}).ObserveDuration(latency)
}

func (c *Collector) MessageRetried(queue string) {
	c.registry.Counter(metricRetried, "Deliveries that failed and were scheduled for another attempt.", Labels{"queue": queue}).Inc()
}

func (c *Collector) MessageDeadLettered(queue, kind string) {
	c.registry.Counter(metricDeadLettered, "Messages moved to the dead letter store.", Labels{"queue": queue, "kind": kind}).Inc()
}

func (c *Collector) MessageRejected(queue string) {
	c.registry.Counter(metricRejected, "Publications rejected before entering a queue.", Labels{"queue": queue}).Inc()
}

func (c *Collector) QueueDepth(queue string, ready, inFlight, scheduled int) {
	c.registry.Gauge(metricQueueDepth, "Messages ready for delivery.", Labels{"queue": queue}).Set(int64(ready))
	c.registry.Gauge(metricInFlight, "Messages currently leased by a consumer.", Labels{"queue": queue}).Set(int64(inFlight))
	c.registry.Gauge(metricScheduled, "Messages waiting for a retry backoff to elapse.", Labels{"queue": queue}).Set(int64(scheduled))
}
