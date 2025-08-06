package engine

import (
	"context"
	"time"
)

type Metrics interface {
	MessagePublished(queue string, latency time.Duration)
	MessageDelivered(queue string)
	MessageAcknowledged(queue string, latency time.Duration)
	MessageRetried(queue string)
	MessageDeadLettered(queue, kind string)
	MessageRejected(queue string)
	QueueDepth(queue string, ready, inFlight, scheduled int)
}

type noopMetrics struct{}

func (noopMetrics) MessagePublished(string, time.Duration)    {}
func (noopMetrics) MessageDelivered(string)                   {}
func (noopMetrics) MessageAcknowledged(string, time.Duration) {}
func (noopMetrics) MessageRetried(string)                     {}
func (noopMetrics) MessageDeadLettered(string, string)        {}
func (noopMetrics) MessageRejected(string)                    {}
func (noopMetrics) QueueDepth(string, int, int, int)          {}

func (e *Engine) RefreshQueueDepths(ctx context.Context) {
	for _, name := range e.registry.QueueNames() {
		depth, err := e.store.Depth(ctx, name)
		if err != nil {
			continue
		}

		e.metrics.QueueDepth(name, depth.Ready, depth.InFlight, depth.Scheduled)
	}
}
