package retry

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

const (
	defaultMaxAttempts     = 3
	defaultInitialInterval = 500 * time.Millisecond
	defaultMaxInterval     = 5 * time.Minute
	defaultMultiplier      = 2.0
	defaultJitterFraction  = 0.2
)

type Policy struct {
	MaxAttempts     int           `json:"max_attempts"`
	InitialInterval time.Duration `json:"initial_interval"`
	MaxInterval     time.Duration `json:"max_interval"`
	Multiplier      float64       `json:"multiplier"`
	JitterFraction  float64       `json:"jitter_fraction"`
}

type Randomiser interface {
	Float64() float64
}

func Default() Policy {
	return Policy{
		MaxAttempts:     defaultMaxAttempts,
		InitialInterval: defaultInitialInterval,
		MaxInterval:     defaultMaxInterval,
		Multiplier:      defaultMultiplier,
		JitterFraction:  defaultJitterFraction,
	}
}

func (p Policy) WithDefaults() Policy {
	fallback := Default()

	if p.MaxAttempts <= 0 {
		p.MaxAttempts = fallback.MaxAttempts
	}

	if p.InitialInterval <= 0 {
		p.InitialInterval = fallback.InitialInterval
	}

	if p.MaxInterval <= 0 {
		p.MaxInterval = fallback.MaxInterval
	}

	if p.Multiplier <= 0 {
		p.Multiplier = fallback.Multiplier
	}

	if p.JitterFraction < 0 {
		p.JitterFraction = 0
	}

	return p
}

func (p Policy) Validate() error {
	switch {
	case p.MaxAttempts < 1:
		return fmt.Errorf("retry: max_attempts must be at least 1, got %d", p.MaxAttempts)
	case p.InitialInterval <= 0:
		return fmt.Errorf("retry: initial_interval must be positive, got %s", p.InitialInterval)
	case p.MaxInterval < p.InitialInterval:
		return fmt.Errorf("retry: max_interval %s must not be shorter than initial_interval %s", p.MaxInterval, p.InitialInterval)
	case p.Multiplier < 1:
		return fmt.Errorf("retry: multiplier must be at least 1, got %.2f", p.Multiplier)
	case p.JitterFraction < 0 || p.JitterFraction > 1:
		return fmt.Errorf("retry: jitter_fraction must be within [0,1], got %.2f", p.JitterFraction)
	default:
		return nil
	}
}

func (p Policy) ShouldRetry(attempts int) bool {
	return attempts < p.MaxAttempts
}

func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	scaled := float64(p.InitialInterval) * math.Pow(p.Multiplier, float64(attempt-1))
	if scaled > float64(p.MaxInterval) || math.IsInf(scaled, 0) {
		return p.MaxInterval
	}

	return time.Duration(scaled)
}

func (p Policy) BackoffWithJitter(attempt int, source Randomiser) time.Duration {
	base := p.Backoff(attempt)

	if p.JitterFraction == 0 {
		return base
	}

	if source == nil {
		source = globalRandomiser{}
	}

	spread := float64(base) * p.JitterFraction
	offset := spread * (2*source.Float64() - 1)

	jittered := time.Duration(float64(base) + offset)
	if jittered < 0 {
		return 0
	}

	if jittered > p.MaxInterval {
		return p.MaxInterval
	}

	return jittered
}

func (p Policy) NextAttemptAt(now time.Time, attempt int, source Randomiser) time.Time {
	return now.Add(p.BackoffWithJitter(attempt, source))
}

type globalRandomiser struct{}

func (globalRandomiser) Float64() float64 {
	return rand.Float64()
}
