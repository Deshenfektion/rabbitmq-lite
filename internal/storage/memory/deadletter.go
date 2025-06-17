package memory

import (
	"context"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

func (s *Store) SaveDeadLetter(_ context.Context, entry *storage.DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}

	if entry.ID == "" {
		entry.ID = message.NewID()
	}

	if _, exists := s.deadLetters[entry.ID]; exists {
		return storage.ErrConflict
	}

	copied := *entry
	s.deadLetters[entry.ID] = &copied
	s.deadOrder = append(s.deadOrder, entry.ID)

	return nil
}

func (s *Store) DeadLetters(_ context.Context, filter storage.DeadLetterFilter) ([]*storage.DeadLetter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filter = filter.Normalise()

	matched := make([]*storage.DeadLetter, 0, filter.Limit)
	skipped := 0

	for i := len(s.deadOrder) - 1; i >= 0; i-- {
		entry := s.deadLetters[s.deadOrder[i]]
		if entry == nil {
			continue
		}

		if filter.Queue != "" && entry.Queue != filter.Queue {
			continue
		}

		if skipped < filter.Offset {
			skipped++
			continue
		}

		if len(matched) >= filter.Limit {
			break
		}

		copied := *entry
		matched = append(matched, &copied)
	}

	return matched, nil
}

func (s *Store) DeadLetter(_ context.Context, id string) (*storage.DeadLetter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.deadLetters[id]
	if !ok {
		return nil, storage.ErrNotFound
	}

	copied := *entry

	return &copied, nil
}

func (s *Store) DeleteDeadLetter(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.deadLetters[id]; !ok {
		return storage.ErrNotFound
	}

	delete(s.deadLetters, id)

	for i, existing := range s.deadOrder {
		if existing == id {
			s.deadOrder = append(s.deadOrder[:i], s.deadOrder[i+1:]...)
			break
		}
	}

	return nil
}

func (s *Store) CountDeadLetters(_ context.Context, queue string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0

	for _, entry := range s.deadLetters {
		if queue == "" || entry.Queue == queue {
			count++
		}
	}

	return count, nil
}

func (s *Store) AppendHistory(_ context.Context, event storage.HistoryEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}

	s.history[event.MessageID] = append(s.history[event.MessageID], event)

	return nil
}

func (s *Store) History(_ context.Context, messageID string) ([]storage.HistoryEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return append([]storage.HistoryEvent(nil), s.history[messageID]...), nil
}
