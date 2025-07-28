package config

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err != nil {
		return fmt.Errorf("config: %s is not a duration string", node.Value)
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("config: invalid duration %q: %w", raw, err)
	}

	*d = Duration(parsed)

	return nil
}

func (d Duration) MarshalYAML() (any, error) {
	return d.String(), nil
}

func orDefault(value Duration, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}

	return value.Duration()
}
