package broker

import (
	"sync"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
)

type Broker struct {
	mu     sync.RWMutex
	clock  func() time.Time
	queues map[string]*queueState
}

type queueState struct {
	definition *Queue
	pending    []*message.Message
}

func New() *Broker {
	return &Broker{
		clock:  func() time.Time { return time.Now().UTC() },
		queues: make(map[string]*queueState),
	}
}

func (b *Broker) DeclareQueue(spec QueueSpec) (*Queue, error) {
	spec = spec.withDefaults()
	if err := spec.Validate(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.queues[spec.Name]; ok {
		if existing.definition.QueueSpec != spec {
			return nil, ErrQueueExists
		}

		return existing.definition, nil
	}

	queue := &Queue{QueueSpec: spec, CreatedAt: b.clock()}
	b.queues[spec.Name] = &queueState{definition: queue}

	return queue, nil
}

func (b *Broker) Queue(name string) (*Queue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	state, ok := b.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	return state.definition, nil
}

func (b *Broker) Queues() []*Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()

	queues := make([]*Queue, 0, len(b.queues))
	for _, state := range b.queues {
		queues = append(queues, state.definition)
	}

	return queues
}

func (b *Broker) DeleteQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; !ok {
		return ErrQueueNotFound
	}

	delete(b.queues, name)

	return nil
}

func (b *Broker) Enqueue(queue string, pub message.Publication) (*message.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	state, ok := b.queues[queue]
	if !ok {
		return nil, ErrQueueNotFound
	}

	now := b.clock()

	msg := message.New(pub, queue, now)
	msg.MaxAttempts = state.definition.MaxAttempts

	if err := msg.Transition(message.StateQueued, now); err != nil {
		return nil, err
	}

	state.pending = append(state.pending, msg)

	return msg, nil
}

func (b *Broker) Dequeue(queue string) (*message.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	state, ok := b.queues[queue]
	if !ok {
		return nil, ErrQueueNotFound
	}

	if len(state.pending) == 0 {
		return nil, ErrMessageNotFound
	}

	msg := state.pending[0]
	state.pending = state.pending[1:]

	if err := msg.Transition(message.StateProcessing, b.clock()); err != nil {
		return nil, err
	}

	msg.Attempts++

	return msg, nil
}

func (b *Broker) Depth(queue string) (int, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	state, ok := b.queues[queue]
	if !ok {
		return 0, ErrQueueNotFound
	}

	return len(state.pending), nil
}
