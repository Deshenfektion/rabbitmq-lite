package broker

import (
	"sort"
	"sync"
	"time"
)

type Registry struct {
	mu      sync.RWMutex
	clock   func() time.Time
	routing *routingTable
	queues  map[string]*Queue
}

func NewRegistry() *Registry {
	return &Registry{
		clock:   func() time.Time { return time.Now().UTC() },
		routing: newRoutingTable(),
		queues:  make(map[string]*Queue),
	}
}

func (r *Registry) DeclareExchange(spec ExchangeSpec) (*Exchange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.routing.declareExchange(spec, r.clock())
}

func (r *Registry) Exchange(name string) (*Exchange, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.routing.exchange(name)
}

func (r *Registry) Exchanges() []*Exchange {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.routing.allExchanges()
}

func (r *Registry) Bind(spec BindingSpec) (*Binding, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.queues[spec.Queue]; !ok {
		return nil, ErrQueueNotFound
	}

	return r.routing.bind(spec, r.clock())
}

func (r *Registry) Unbind(spec BindingSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.routing.unbind(spec)
}

func (r *Registry) Bindings(exchange string) []Binding {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.routing.bindingsFor(exchange)
}

func (r *Registry) AllBindings() []Binding {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.routing.allBindings()
}

func (r *Registry) DeclareQueue(spec QueueSpec) (*Queue, error) {
	spec = spec.withDefaults()
	if err := spec.Validate(); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.queues[spec.Name]; ok {
		if existing.QueueSpec != spec {
			return nil, ErrQueueExists
		}

		return existing, nil
	}

	queue := &Queue{QueueSpec: spec, CreatedAt: r.clock()}
	r.queues[spec.Name] = queue

	return queue, nil
}

func (r *Registry) Queue(name string) (*Queue, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	queue, ok := r.queues[name]
	if !ok {
		return nil, ErrQueueNotFound
	}

	return queue, nil
}

func (r *Registry) Queues() []*Queue {
	r.mu.RLock()
	defer r.mu.RUnlock()

	queues := make([]*Queue, 0, len(r.queues))
	for _, queue := range r.queues {
		queues = append(queues, queue)
	}

	sort.Slice(queues, func(i, j int) bool { return queues[i].Name < queues[j].Name })

	return queues
}

func (r *Registry) QueueNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.queues))
	for name := range r.queues {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

func (r *Registry) DeleteQueue(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.queues[name]; !ok {
		return ErrQueueNotFound
	}

	if r.routing.queueReferences(name) > 0 {
		return ErrQueueInUse
	}

	delete(r.queues, name)

	return nil
}

func (r *Registry) Route(exchange, routingKey string) ([]*Queue, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	definition, err := r.routing.exchange(exchange)
	if err != nil {
		return nil, err
	}

	names := r.routing.route(definition, routingKey, r.queueExistsLocked)
	if len(names) == 0 {
		return nil, ErrUnroutable
	}

	queues := make([]*Queue, 0, len(names))
	for _, name := range names {
		queues = append(queues, r.queues[name])
	}

	return queues, nil
}

func (r *Registry) Restore(exchanges []*Exchange, queues []*Queue, bindings []Binding) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, exchange := range exchanges {
		copied := *exchange
		r.routing.exchanges[exchange.Name] = &copied
	}

	for _, queue := range queues {
		copied := *queue
		r.queues[queue.Name] = &copied
	}

	for _, binding := range bindings {
		r.routing.bindings[binding.Exchange] = append(r.routing.bindings[binding.Exchange], binding)
	}
}

func (r *Registry) queueExistsLocked(name string) bool {
	_, ok := r.queues[name]
	return ok
}
