package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/worker"
)

var (
	ErrConsumerExists   = errors.New("engine: consumer already registered")
	ErrConsumerNotFound = errors.New("engine: consumer not found")
	ErrHandlerRequired  = errors.New("engine: consumer requires a handler")
	ErrNotRunning       = errors.New("engine: engine is not running")
)

type Delivery struct {
	Message *message.Message
	Attempt int
	Queue   string
}

type Handler interface {
	Handle(ctx context.Context, delivery Delivery) error
}

type HandlerFunc func(ctx context.Context, delivery Delivery) error

func (f HandlerFunc) Handle(ctx context.Context, delivery Delivery) error {
	return f(ctx, delivery)
}

type PermanentError struct {
	Err error
}

func Permanent(err error) error {
	return &PermanentError{Err: err}
}

func (e *PermanentError) Error() string {
	return fmt.Sprintf("permanent failure: %v", e.Err)
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

func isPermanent(err error) bool {
	var permanent *PermanentError
	return errors.As(err, &permanent)
}

type ConsumerSpec struct {
	Name              string
	Queue             string
	Concurrency       int
	Prefetch          int
	ProcessingTimeout time.Duration
	Handler           Handler
}

type Consumer struct {
	spec    ConsumerSpec
	pool    *worker.Pool
	stop    context.CancelFunc
	done    chan struct{}
	stopped sync.Once
}

type ConsumerStatus struct {
	Name        string `json:"name"`
	Queue       string `json:"queue"`
	Concurrency int    `json:"concurrency"`
	Prefetch    int    `json:"prefetch"`
	InFlight    int    `json:"in_flight"`
	Queued      int    `json:"queued"`
}

func (s ConsumerSpec) withDefaults(visibilityTimeout time.Duration) ConsumerSpec {
	if s.Concurrency < 1 {
		s.Concurrency = 1
	}

	if s.Prefetch < 1 {
		s.Prefetch = s.Concurrency
	}

	if s.ProcessingTimeout <= 0 {
		s.ProcessingTimeout = visibilityTimeout
	}

	return s
}

func (c *Consumer) Name() string {
	return c.spec.Name
}

func (c *Consumer) Queue() string {
	return c.spec.Queue
}

func (c *Consumer) Status() ConsumerStatus {
	return ConsumerStatus{
		Name:        c.spec.Name,
		Queue:       c.spec.Queue,
		Concurrency: c.spec.Concurrency,
		Prefetch:    c.spec.Prefetch,
		InFlight:    c.pool.InFlight(),
		Queued:      c.pool.Queued(),
	}
}

func (c *Consumer) Stop() {
	c.stopped.Do(func() {
		c.stop()
		<-c.done
		c.pool.Stop()
	})
}
