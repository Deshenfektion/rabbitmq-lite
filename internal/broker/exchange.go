package broker

import (
	"fmt"
	"time"
)

type ExchangeKind string

const (
	ExchangeDirect ExchangeKind = "direct"
	ExchangeFanout ExchangeKind = "fanout"
	ExchangeTopic  ExchangeKind = "topic"
)

var exchangeKinds = []ExchangeKind{ExchangeDirect, ExchangeFanout, ExchangeTopic}

func ParseExchangeKind(raw string) (ExchangeKind, error) {
	candidate := ExchangeKind(raw)
	for _, kind := range exchangeKinds {
		if kind == candidate {
			return kind, nil
		}
	}

	return "", invalid("kind", fmt.Sprintf("must be one of direct, fanout, topic, got %q", raw))
}

type ExchangeSpec struct {
	Name    string       `json:"name"`
	Kind    ExchangeKind `json:"kind"`
	Durable bool         `json:"durable"`
}

type Exchange struct {
	ExchangeSpec
	CreatedAt time.Time `json:"created_at"`
}

func (s ExchangeSpec) Validate() error {
	if err := validateName("exchange name", s.Name); err != nil {
		return err
	}

	if _, err := ParseExchangeKind(string(s.Kind)); err != nil {
		return err
	}

	return nil
}
