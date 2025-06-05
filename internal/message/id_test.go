package message

import (
	"sort"
	"testing"
	"time"
)

func TestIDsAreLexicographicallyOrderedByTime(t *testing.T) {
	base := time.Date(2025, time.June, 5, 21, 0, 0, 0, time.UTC)

	ids := []string{
		newIDAt(base.Add(2*time.Second), defaultEntropy),
		newIDAt(base, defaultEntropy),
		newIDAt(base.Add(time.Second), defaultEntropy),
	}

	expected := []string{ids[1], ids[2], ids[0]}
	sort.Strings(ids)

	for i := range expected {
		if ids[i] != expected[i] {
			t.Fatalf("identifiers are not time ordered: %v", ids)
		}
	}
}

func TestTimestampOfRoundTrips(t *testing.T) {
	moment := time.Date(2025, time.June, 5, 21, 30, 15, 0, time.UTC)

	decoded, ok := TimestampOf(newIDAt(moment, defaultEntropy))
	if !ok {
		t.Fatal("expected identifier to decode")
	}

	if !decoded.Equal(moment) {
		t.Fatalf("expected %s, got %s", moment, decoded)
	}
}

func TestTimestampOfRejectsMalformedInput(t *testing.T) {
	for _, id := range []string{"", "zzz", "0197"} {
		if _, ok := TimestampOf(id); ok {
			t.Errorf("expected %q to be rejected", id)
		}
	}
}

func TestNewIDIsUnique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)

	for range 1000 {
		id := NewID()
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate identifier %s", id)
		}
		seen[id] = struct{}{}
	}
}
