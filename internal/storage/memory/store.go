package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

type record struct {
	msg      *message.Message
	lease    time.Time
	consumer string
}

const compactionThreshold = 64

type Store struct {
	mu          sync.RWMutex
	closed      bool
	exchanges   map[string]*broker.Exchange
	queues      map[string]*broker.Queue
	bindings    map[string]broker.Binding
	messages    map[string]*record
	order       map[string][]string
	settled     map[string]int
	deadLetters map[string]*storage.DeadLetter
	deadOrder   []string
	history     map[string][]storage.HistoryEvent
}

func New() *Store {
	return &Store{
		exchanges:   make(map[string]*broker.Exchange),
		queues:      make(map[string]*broker.Queue),
		bindings:    make(map[string]broker.Binding),
		messages:    make(map[string]*record),
		order:       make(map[string][]string),
		settled:     make(map[string]int),
		deadLetters: make(map[string]*storage.DeadLetter),
		history:     make(map[string][]storage.HistoryEvent),
	}
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

func (s *Store) SaveExchange(_ context.Context, exchange *broker.Exchange) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}

	copied := *exchange
	s.exchanges[exchange.Name] = &copied

	return nil
}

func (s *Store) Exchanges(_ context.Context) ([]*broker.Exchange, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exchanges := make([]*broker.Exchange, 0, len(s.exchanges))
	for _, exchange := range s.exchanges {
		copied := *exchange
		exchanges = append(exchanges, &copied)
	}

	sort.Slice(exchanges, func(i, j int) bool { return exchanges[i].Name < exchanges[j].Name })

	return exchanges, nil
}

func (s *Store) SaveQueue(_ context.Context, queue *broker.Queue) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}

	copied := *queue
	s.queues[queue.Name] = &copied

	return nil
}

func (s *Store) Queues(_ context.Context) ([]*broker.Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queues := make([]*broker.Queue, 0, len(s.queues))
	for _, queue := range s.queues {
		copied := *queue
		queues = append(queues, &copied)
	}

	sort.Slice(queues, func(i, j int) bool { return queues[i].Name < queues[j].Name })

	return queues, nil
}

func (s *Store) DeleteQueue(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queues[name]; !ok {
		return storage.ErrNotFound
	}

	delete(s.queues, name)

	for _, id := range s.order[name] {
		delete(s.messages, id)
	}

	delete(s.order, name)
	delete(s.settled, name)

	return nil
}

func (s *Store) SaveBinding(_ context.Context, binding broker.Binding) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}

	s.bindings[bindingKey(binding.BindingSpec)] = binding

	return nil
}

func (s *Store) Bindings(_ context.Context) ([]broker.Binding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bindings := make([]broker.Binding, 0, len(s.bindings))
	for _, binding := range s.bindings {
		bindings = append(bindings, binding)
	}

	sort.Slice(bindings, func(i, j int) bool {
		return bindingKey(bindings[i].BindingSpec) < bindingKey(bindings[j].BindingSpec)
	})

	return bindings, nil
}

func (s *Store) DeleteBinding(_ context.Context, spec broker.BindingSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := bindingKey(spec)
	if _, ok := s.bindings[key]; !ok {
		return storage.ErrNotFound
	}

	delete(s.bindings, key)

	return nil
}

func (s *Store) Append(_ context.Context, messages []*message.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}

	for _, msg := range messages {
		if _, exists := s.messages[msg.ID]; exists {
			return storage.ErrConflict
		}
	}

	for _, msg := range messages {
		s.messages[msg.ID] = &record{msg: msg.Clone()}
		s.order[msg.Queue] = append(s.order[msg.Queue], msg.ID)
	}

	return nil
}

func (s *Store) Message(_ context.Context, id string) (*message.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.messages[id]
	if !ok {
		return nil, storage.ErrNotFound
	}

	return entry.msg.Clone(), nil
}

func (s *Store) Claim(_ context.Context, req storage.ClaimRequest) ([]*message.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, storage.ErrClosed
	}

	s.compactLocked(req.Queue)

	claimed := make([]*message.Message, 0, req.Limit)

	for _, id := range s.order[req.Queue] {
		if len(claimed) >= req.Limit {
			break
		}

		entry := s.messages[id]
		if entry == nil || !claimable(entry.msg, req.Now) {
			continue
		}

		if entry.msg.State == message.StateRetrying {
			if err := entry.msg.Transition(message.StateQueued, req.Now); err != nil {
				return nil, err
			}
		}

		if err := entry.msg.Transition(message.StateProcessing, req.Now); err != nil {
			return nil, err
		}

		entry.msg.Attempts++
		entry.lease = req.Now.Add(req.Lease)
		entry.consumer = req.Consumer

		claimed = append(claimed, entry.msg.Clone())
	}

	return claimed, nil
}

func (s *Store) Acknowledge(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.leased(id)
	if err != nil {
		return err
	}

	if err := entry.msg.Transition(message.StateAcknowledged, at); err != nil {
		return err
	}

	s.settled[entry.msg.Queue]++
	entry.lease = time.Time{}
	entry.consumer = ""

	return nil
}

func (s *Store) ScheduleRetry(_ context.Context, req storage.RetryRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.leased(req.ID)
	if err != nil {
		return err
	}

	if err := entry.msg.Transition(message.StateFailed, req.Now); err != nil {
		return err
	}

	if err := entry.msg.Transition(message.StateRetrying, req.Now); err != nil {
		return err
	}

	entry.msg.AvailableAt = req.AvailableAt.UTC()
	entry.msg.LastError = req.Reason
	entry.lease = time.Time{}
	entry.consumer = ""

	return nil
}

func (s *Store) Release(_ context.Context, id string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.leased(id)
	if err != nil {
		return err
	}

	if err := entry.msg.Transition(message.StateQueued, at); err != nil {
		return err
	}

	entry.msg.AvailableAt = at.UTC()
	entry.lease = time.Time{}
	entry.consumer = ""

	return nil
}

func (s *Store) MarkDeadLettered(_ context.Context, id string, reason string, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.messages[id]
	if !ok {
		return storage.ErrNotFound
	}

	if entry.msg.State == message.StateProcessing {
		if err := entry.msg.Transition(message.StateFailed, at); err != nil {
			return err
		}
	}

	if err := entry.msg.Transition(message.StateDeadLettered, at); err != nil {
		return err
	}

	s.settled[entry.msg.Queue]++
	entry.msg.LastError = reason
	entry.lease = time.Time{}
	entry.consumer = ""

	return nil
}

func (s *Store) ExpiredLeases(_ context.Context, now time.Time, limit int) ([]*message.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	expired := make([]*message.Message, 0)

	for _, entry := range s.messages {
		if entry.msg.State != message.StateProcessing || entry.lease.IsZero() || entry.lease.After(now) {
			continue
		}

		expired = append(expired, entry.msg.Clone())
	}

	sort.Slice(expired, func(i, j int) bool { return expired[i].ID < expired[j].ID })

	if limit > 0 && len(expired) > limit {
		expired = expired[:limit]
	}

	return expired, nil
}

func (s *Store) Depth(_ context.Context, queue string) (storage.Depth, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.queues[queue]; !ok {
		return storage.Depth{}, storage.ErrNotFound
	}

	var depth storage.Depth

	for _, id := range s.order[queue] {
		entry := s.messages[id]
		if entry == nil {
			continue
		}

		switch entry.msg.State {
		case message.StateQueued:
			depth.Ready++
		case message.StateProcessing:
			depth.InFlight++
		case message.StateRetrying, message.StateFailed:
			depth.Scheduled++
		}
	}

	depth.Total = depth.Ready + depth.InFlight + depth.Scheduled

	return depth, nil
}

func (s *Store) Purge(_ context.Context, queue string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queues[queue]; !ok {
		return 0, storage.ErrNotFound
	}

	remaining := make([]string, 0, len(s.order[queue]))
	purged := 0

	for _, id := range s.order[queue] {
		entry := s.messages[id]
		if entry == nil {
			continue
		}

		if entry.msg.State.Terminal() {
			remaining = append(remaining, id)
			continue
		}

		delete(s.messages, id)
		purged++
	}

	s.order[queue] = remaining
	s.settled[queue] = len(remaining)

	return purged, nil
}

func (s *Store) compactLocked(queue string) {
	index := s.order[queue]

	head := 0
	for head < len(index) && s.isSettledLocked(index[head]) {
		head++
	}

	if head > 0 {
		index = index[head:]
		s.order[queue] = index
		s.settled[queue] = max(s.settled[queue]-head, 0)
	}

	settled := s.settled[queue]
	if settled < compactionThreshold || settled*2 < len(index) {
		return
	}

	retained := make([]string, 0, len(index)-settled)

	for _, id := range index {
		if s.isSettledLocked(id) {
			continue
		}

		retained = append(retained, id)
	}

	s.order[queue] = retained
	s.settled[queue] = 0
}

func (s *Store) isSettledLocked(id string) bool {
	entry := s.messages[id]
	return entry == nil || entry.msg.State.Terminal()
}

func (s *Store) leased(id string) (*record, error) {
	entry, ok := s.messages[id]
	if !ok {
		return nil, storage.ErrNotFound
	}

	if entry.msg.State != message.StateProcessing {
		return nil, &storage.StateError{
			MessageID: id,
			Expected:  message.StateProcessing,
			Actual:    entry.msg.State,
		}
	}

	return entry, nil
}

func claimable(msg *message.Message, now time.Time) bool {
	switch msg.State {
	case message.StateQueued, message.StateRetrying:
		return !msg.AvailableAt.After(now)
	default:
		return false
	}
}

func bindingKey(spec broker.BindingSpec) string {
	return spec.Exchange + "\x00" + spec.Queue + "\x00" + spec.RoutingKey
}

var _ storage.Store = (*Store)(nil)
