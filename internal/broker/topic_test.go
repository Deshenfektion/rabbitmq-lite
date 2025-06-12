package broker

import "testing"

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		pattern    string
		routingKey string
		expected   bool
	}{
		{"invoice.issued", "invoice.issued", true},
		{"invoice.issued", "invoice.paid", false},
		{"invoice.*", "invoice.issued", true},
		{"invoice.*", "invoice.issued.eu", false},
		{"invoice.#", "invoice.issued.eu", true},
		{"invoice.#", "invoice", true},
		{"#", "anything.at.all", true},
		{"#.eu", "invoice.issued.eu", true},
		{"#.eu", "invoice.issued.us", false},
		{"erp.*.created", "erp.customer.created", true},
		{"erp.*.created", "erp.customer.updated", false},
		{"erp.#.created", "erp.customer.eu.created", true},
		{"erp.#.created", "erp.created", true},
		{"*.*", "invoice.issued", true},
		{"*.*", "invoice", false},
		{"*", "invoice.issued", false},
		{"invoice.issued.#", "invoice.issued", true},
	}

	for _, tc := range cases {
		if got := matchTopic(tc.pattern, tc.routingKey); got != tc.expected {
			t.Errorf("matchTopic(%q, %q) = %v, expected %v", tc.pattern, tc.routingKey, got, tc.expected)
		}
	}
}

func TestTopicExchangeRoutesToOverlappingBindings(t *testing.T) {
	b := New()

	if _, err := b.DeclareExchange(ExchangeSpec{Name: "erp.topic", Kind: ExchangeTopic}); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	declareQueue(t, b, "invoice-processing")
	declareQueue(t, b, "audit-log")
	declareQueue(t, b, "eu-compliance")

	bind(t, b, "erp.topic", "invoice-processing", "invoice.*")
	bind(t, b, "erp.topic", "audit-log", "#")
	bind(t, b, "erp.topic", "eu-compliance", "#.eu")

	got := routedQueues(t, b, "erp.topic", "invoice.issued")
	if len(got) != 2 || got[0] != "audit-log" || got[1] != "invoice-processing" {
		t.Fatalf("unexpected routing result %v", got)
	}
}

func TestTopicBindingPatternValidation(t *testing.T) {
	b := New()

	if _, err := b.DeclareExchange(ExchangeSpec{Name: "erp.topic", Kind: ExchangeTopic}); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}

	declareQueue(t, b, "invoice-processing")

	invalidPatterns := []string{"", "invoice..issued", "invoice.*x", "in#voice"}
	for _, pattern := range invalidPatterns {
		if _, err := b.Bind(BindingSpec{Exchange: "erp.topic", Queue: "invoice-processing", RoutingKey: pattern}); err == nil {
			t.Errorf("expected pattern %q to be rejected", pattern)
		}
	}

	validPatterns := []string{"invoice.issued", "invoice.*", "#", "erp.#.created"}
	for _, pattern := range validPatterns {
		if _, err := b.Bind(BindingSpec{Exchange: "erp.topic", Queue: "invoice-processing", RoutingKey: pattern}); err != nil {
			t.Errorf("expected pattern %q to be accepted, got %v", pattern, err)
		}
	}
}

func BenchmarkMatchTopic(b *testing.B) {
	for b.Loop() {
		matchTopic("erp.#.created", "erp.customer.eu.created")
	}
}
