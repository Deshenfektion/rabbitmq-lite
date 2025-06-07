package broker

import (
	"errors"
	"fmt"
)

var (
	ErrQueueNotFound    = errors.New("broker: queue not found")
	ErrQueueExists      = errors.New("broker: queue already exists")
	ErrExchangeNotFound = errors.New("broker: exchange not found")
	ErrExchangeExists   = errors.New("broker: exchange already exists")
	ErrBindingNotFound  = errors.New("broker: binding not found")
	ErrMessageNotFound  = errors.New("broker: message not found")
	ErrQueueInUse       = errors.New("broker: queue still referenced by a binding")
	ErrUnroutable       = errors.New("broker: publication matched no queue")
	ErrClosed           = errors.New("broker: closed")
)

type FieldError struct {
	Field  string
	Reason string
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("broker: invalid %s: %s", e.Field, e.Reason)
}

func invalid(field, reason string) error {
	return &FieldError{Field: field, Reason: reason}
}
