package broker

import (
	"regexp"
	"time"
)

const (
	maxNameLength         = 128
	maxRoutingKeyLength   = 255
	defaultMaxAttempts    = 3
	defaultVisibilityTime = 30 * time.Second
)

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._\-]*$`)

type QueueSpec struct {
	Name              string        `json:"name"`
	Durable           bool          `json:"durable"`
	MaxAttempts       int           `json:"max_attempts"`
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	Schema            string        `json:"schema,omitempty"`
}

type Queue struct {
	QueueSpec
	CreatedAt time.Time `json:"created_at"`
}

func (s QueueSpec) Validate() error {
	if err := validateName("queue name", s.Name); err != nil {
		return err
	}

	if s.MaxAttempts < 1 {
		return invalid("max_attempts", "must be at least 1")
	}

	if s.VisibilityTimeout <= 0 {
		return invalid("visibility_timeout", "must be positive")
	}

	if s.Schema != "" {
		if err := validateName("schema", s.Schema); err != nil {
			return err
		}
	}

	return nil
}

func (s QueueSpec) withDefaults() QueueSpec {
	if s.MaxAttempts == 0 {
		s.MaxAttempts = defaultMaxAttempts
	}

	if s.VisibilityTimeout == 0 {
		s.VisibilityTimeout = defaultVisibilityTime
	}

	return s
}

func validateName(field, value string) error {
	switch {
	case value == "":
		return invalid(field, "must not be empty")
	case len(value) > maxNameLength:
		return invalid(field, "exceeds maximum length")
	case !namePattern.MatchString(value):
		return invalid(field, "must match [a-zA-Z0-9][a-zA-Z0-9._-]*")
	default:
		return nil
	}
}
