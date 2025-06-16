package storage

import (
	"errors"
	"fmt"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
)

var (
	ErrNotFound      = errors.New("storage: record not found")
	ErrConflict      = errors.New("storage: record already exists")
	ErrClosed        = errors.New("storage: store is closed")
	ErrNotClaimable  = errors.New("storage: message is not in a claimable state")
	ErrLeaseNotHeld  = errors.New("storage: message is not currently leased")
	ErrQueueNotEmpty = errors.New("storage: queue still holds messages")
)

type StateError struct {
	MessageID string
	Expected  message.State
	Actual    message.State
}

func (e *StateError) Error() string {
	return fmt.Sprintf("storage: message %s is %s, expected %s", e.MessageID, e.Actual, e.Expected)
}
