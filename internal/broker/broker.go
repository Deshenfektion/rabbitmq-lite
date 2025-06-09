package broker

import (
	"sort"
	"sync"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
)

type Broker struct {
	mu        sync.RWMutex
	clock     func() time.Time
	queues    map[string]*queueState
	exchanges map[string]*Exchange
	bindings  map[string][]Binding
}

type queueState struct {
	definition *Queue
	pending    []*message.Message
}

func New() *Broker {
	return &Broker{
		clock:     func() time.Time { return time.Now().UTC() },
		queues:    make(map[string]*queueState),
		exchanges: make(map[string]*Exchange),
		bindings:  make(map[string][]Binding),
	}
}

func (b *Broker) DeclareExchange(spec ExchangeSpec) (*Exchange, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.exchanges[spec.Name]; ok {
		if existing.ExchangeSpec != spec {
			return nil, ErrExchangeExists
		}

		return existing, nil
	}

	exchange := &Exchange{ExchangeSpec: spec, CreatedAt: b.clock()}
	b.exchanges[spec.Name] = exchange

	return exchange, nil
}

func (b *Broker) Exchange(name string) (*Exchange, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	exchange, ok := b.exchanges[name]
	if !ok {
		return nil, ErrExchangeNotFound
	}

	return exchange, nil
}

func (b *Broker) Exchanges() []*Exchange {
	b.mu.RLock()
	defer b.mu.RUnlock()

	exchanges := make([]*Exchange, 0, len(b.exchanges))
	for _, exchange := range b.exchanges {
		exchanges = append(exchanges, exchange)
	}

	return exchanges
}

func (b *Broker) Bind(spec BindingSpec) (*Binding, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	exchange, ok := b.exchanges[spec.Exchange]
	if !ok {
		return nil, ErrExchangeNotFound
	}

	if _, ok := b.queues[spec.Queue]; !ok {
		return nil, ErrQueueNotFound
	}

	if err := spec.Validate(exchange.Kind); err != nil {
		return nil, err
	}

	for i := range b.bindings[spec.Exchange] {
		if b.bindings[spec.Exchange][i].key() == spec.key() {
			return &b.bindings[spec.Exchange][i], nil
		}
	}

	binding := Binding{BindingSpec: spec, CreatedAt: b.clock()}
	b.bindings[spec.Exchange] = append(b.bindings[spec.Exchange], binding)

	return &binding, nil
}

func (b *Broker) Unbind(spec BindingSpec) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing := b.bindings[spec.Exchange]
	for i := range existing {
		if existing[i].key() == spec.key() {
			b.bindings[spec.Exchange] = append(existing[:i], existing[i+1:]...)
			return nil
		}
	}

	return ErrBindingNotFound
}

func (b *Broker) Bindings(exchange string) []Binding {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return append([]Binding(nil), b.bindings[exchange]...)
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

func (b *Broker) Publish(pub message.Publication) ([]*message.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	exchange, ok := b.exchanges[pub.Exchange]
	if !ok {
		return nil, ErrExchangeNotFound
	}

	targets := b.matchLocked(exchange, pub.RoutingKey)
	if len(targets) == 0 {
		return nil, ErrUnroutable
	}

	now := b.clock()
	routed := make([]*message.Message, 0, len(targets))

	for _, name := range targets {
		state := b.queues[name]

		msg := message.New(pub, name, now)
		msg.MaxAttempts = state.definition.MaxAttempts

		if err := msg.Transition(message.StateQueued, now); err != nil {
			return nil, err
		}

		state.pending = append(state.pending, msg)
		routed = append(routed, msg)
	}

	return routed, nil
}

func (b *Broker) matchLocked(exchange *Exchange, routingKey string) []string {
	seen := make(map[string]struct{})
	matched := make([]string, 0, 4)

	for _, binding := range b.bindings[exchange.Name] {
		if _, duplicate := seen[binding.Queue]; duplicate {
			continue
		}

		if _, live := b.queues[binding.Queue]; !live {
			continue
		}

		if !bindingMatches(exchange.Kind, binding.RoutingKey, routingKey) {
			continue
		}

		seen[binding.Queue] = struct{}{}
		matched = append(matched, binding.Queue)
	}

	sort.Strings(matched)

	return matched
}

func bindingMatches(kind ExchangeKind, pattern, routingKey string) bool {
	switch kind {
	case ExchangeFanout:
		return true
	case ExchangeDirect:
		return pattern == routingKey
	default:
		return false
	}
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
