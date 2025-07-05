package retry

import (
	"testing"
	"time"
)

type fixedRandomiser float64

func (f fixedRandomiser) Float64() float64 {
	return float64(f)
}

func testPolicy() Policy {
	return Policy{
		MaxAttempts:     5,
		InitialInterval: time.Second,
		MaxInterval:     time.Minute,
		Multiplier:      2,
		JitterFraction:  0,
	}
}

func TestBackoffGrowsExponentially(t *testing.T) {
	policy := testPolicy()

	expected := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
	}

	for i, want := range expected {
		if got := policy.Backoff(i + 1); got != want {
			t.Errorf("attempt %d: expected %s, got %s", i+1, want, got)
		}
	}
}

func TestBackoffIsCappedAtMaxInterval(t *testing.T) {
	policy := testPolicy()

	if got := policy.Backoff(20); got != time.Minute {
		t.Fatalf("expected backoff to saturate at %s, got %s", time.Minute, got)
	}

	if got := policy.Backoff(1000); got != time.Minute {
		t.Fatalf("expected no overflow for large attempt counts, got %s", got)
	}
}

func TestBackoffTreatsAttemptZeroAsFirstAttempt(t *testing.T) {
	policy := testPolicy()

	if got := policy.Backoff(0); got != time.Second {
		t.Fatalf("expected %s, got %s", time.Second, got)
	}
}

func TestJitterStaysWithinConfiguredBounds(t *testing.T) {
	policy := testPolicy()
	policy.JitterFraction = 0.25

	base := policy.Backoff(3)
	lower := time.Duration(float64(base) * 0.75)
	upper := time.Duration(float64(base) * 1.25)

	for _, value := range []float64{0, 0.25, 0.5, 0.75, 1} {
		got := policy.BackoffWithJitter(3, fixedRandomiser(value))

		if got < lower || got > upper {
			t.Errorf("randomiser %.2f produced %s outside [%s, %s]", value, got, lower, upper)
		}
	}
}

func TestJitterIsDisabledWhenFractionIsZero(t *testing.T) {
	policy := testPolicy()

	if got := policy.BackoffWithJitter(2, fixedRandomiser(1)); got != 2*time.Second {
		t.Fatalf("expected deterministic backoff, got %s", got)
	}
}

func TestShouldRetryUntilAttemptsAreExhausted(t *testing.T) {
	policy := testPolicy()

	cases := map[int]bool{0: true, 1: true, 4: true, 5: false, 6: false}

	for attempts, expected := range cases {
		if got := policy.ShouldRetry(attempts); got != expected {
			t.Errorf("attempts %d: expected %v, got %v", attempts, expected, got)
		}
	}
}

func TestNextAttemptAt(t *testing.T) {
	policy := testPolicy()
	now := time.Date(2025, time.July, 12, 18, 0, 0, 0, time.UTC)

	if got := policy.NextAttemptAt(now, 2, nil); !got.Equal(now.Add(2 * time.Second)) {
		t.Fatalf("expected %s, got %s", now.Add(2*time.Second), got)
	}
}

func TestWithDefaultsFillsMissingFields(t *testing.T) {
	policy := Policy{}.WithDefaults()

	if err := policy.Validate(); err != nil {
		t.Fatalf("defaulted policy must be valid: %v", err)
	}

	if policy.MaxAttempts != defaultMaxAttempts || policy.Multiplier != defaultMultiplier {
		t.Fatalf("unexpected defaults %+v", policy)
	}
}

func TestValidateRejectsInconsistentPolicies(t *testing.T) {
	cases := map[string]Policy{
		"zero attempts":     {MaxAttempts: 0, InitialInterval: time.Second, MaxInterval: time.Minute, Multiplier: 2},
		"negative interval": {MaxAttempts: 3, InitialInterval: -time.Second, MaxInterval: time.Minute, Multiplier: 2},
		"inverted bounds":   {MaxAttempts: 3, InitialInterval: time.Minute, MaxInterval: time.Second, Multiplier: 2},
		"small multiplier":  {MaxAttempts: 3, InitialInterval: time.Second, MaxInterval: time.Minute, Multiplier: 0.5},
		"jitter too large":  {MaxAttempts: 3, InitialInterval: time.Second, MaxInterval: time.Minute, Multiplier: 2, JitterFraction: 1.5},
	}

	for name, policy := range cases {
		if err := policy.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}
