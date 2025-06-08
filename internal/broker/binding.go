package broker

import (
	"strings"
	"time"
)

type BindingSpec struct {
	Exchange   string `json:"exchange"`
	Queue      string `json:"queue"`
	RoutingKey string `json:"routing_key"`
}

type Binding struct {
	BindingSpec
	CreatedAt time.Time `json:"created_at"`
}

func (s BindingSpec) Validate(kind ExchangeKind) error {
	if err := validateName("exchange name", s.Exchange); err != nil {
		return err
	}

	if err := validateName("queue name", s.Queue); err != nil {
		return err
	}

	switch kind {
	case ExchangeFanout:
		return nil
	case ExchangeDirect:
		return validateRoutingKey(s.RoutingKey)
	case ExchangeTopic:
		return validateBindingPattern(s.RoutingKey)
	default:
		return invalid("kind", "unsupported exchange kind")
	}
}

func (s BindingSpec) key() string {
	return s.Exchange + "\x00" + s.Queue + "\x00" + s.RoutingKey
}

func validateRoutingKey(key string) error {
	switch {
	case key == "":
		return invalid("routing_key", "must not be empty")
	case len(key) > maxRoutingKeyLength:
		return invalid("routing_key", "exceeds maximum length")
	case strings.ContainsAny(key, "*#"):
		return invalid("routing_key", "wildcards are only valid on topic exchanges")
	default:
		return nil
	}
}

func validateBindingPattern(pattern string) error {
	if pattern == "" {
		return invalid("routing_key", "must not be empty")
	}

	if len(pattern) > maxRoutingKeyLength {
		return invalid("routing_key", "exceeds maximum length")
	}

	for _, segment := range strings.Split(pattern, ".") {
		if segment == "" {
			return invalid("routing_key", "must not contain empty segments")
		}

		if strings.ContainsAny(segment, "*#") && len(segment) != 1 {
			return invalid("routing_key", "wildcard segments must stand alone")
		}
	}

	return nil
}
