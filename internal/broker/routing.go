package broker

import (
	"sort"
	"time"
)

type routingTable struct {
	exchanges map[string]*Exchange
	bindings  map[string][]Binding
}

func newRoutingTable() *routingTable {
	return &routingTable{
		exchanges: make(map[string]*Exchange),
		bindings:  make(map[string][]Binding),
	}
}

func (t *routingTable) declareExchange(spec ExchangeSpec, now time.Time) (*Exchange, error) {
	if err := spec.Validate(); err != nil {
		return nil, err
	}

	if existing, ok := t.exchanges[spec.Name]; ok {
		if existing.ExchangeSpec != spec {
			return nil, ErrExchangeExists
		}

		return existing, nil
	}

	exchange := &Exchange{ExchangeSpec: spec, CreatedAt: now}
	t.exchanges[spec.Name] = exchange

	return exchange, nil
}

func (t *routingTable) exchange(name string) (*Exchange, error) {
	exchange, ok := t.exchanges[name]
	if !ok {
		return nil, ErrExchangeNotFound
	}

	return exchange, nil
}

func (t *routingTable) allExchanges() []*Exchange {
	exchanges := make([]*Exchange, 0, len(t.exchanges))
	for _, exchange := range t.exchanges {
		exchanges = append(exchanges, exchange)
	}

	sort.Slice(exchanges, func(i, j int) bool { return exchanges[i].Name < exchanges[j].Name })

	return exchanges
}

func (t *routingTable) bind(spec BindingSpec, now time.Time) (*Binding, error) {
	exchange, err := t.exchange(spec.Exchange)
	if err != nil {
		return nil, err
	}

	if err := spec.Validate(exchange.Kind); err != nil {
		return nil, err
	}

	for i := range t.bindings[spec.Exchange] {
		if t.bindings[spec.Exchange][i].key() == spec.key() {
			return &t.bindings[spec.Exchange][i], nil
		}
	}

	binding := Binding{BindingSpec: spec, CreatedAt: now}
	t.bindings[spec.Exchange] = append(t.bindings[spec.Exchange], binding)

	return &binding, nil
}

func (t *routingTable) unbind(spec BindingSpec) error {
	existing := t.bindings[spec.Exchange]

	for i := range existing {
		if existing[i].key() == spec.key() {
			t.bindings[spec.Exchange] = append(existing[:i], existing[i+1:]...)
			return nil
		}
	}

	return ErrBindingNotFound
}

func (t *routingTable) bindingsFor(exchange string) []Binding {
	return append([]Binding(nil), t.bindings[exchange]...)
}

func (t *routingTable) allBindings() []Binding {
	bindings := make([]Binding, 0, len(t.bindings))
	for _, group := range t.bindings {
		bindings = append(bindings, group...)
	}

	sort.Slice(bindings, func(i, j int) bool {
		return bindings[i].key() < bindings[j].key()
	})

	return bindings
}

func (t *routingTable) queueReferences(queue string) int {
	references := 0

	for _, bindings := range t.bindings {
		for _, binding := range bindings {
			if binding.Queue == queue {
				references++
			}
		}
	}

	return references
}

func (t *routingTable) route(exchange *Exchange, routingKey string, deliverable func(string) bool) []string {
	seen := make(map[string]struct{})
	matched := make([]string, 0, 4)

	for _, binding := range t.bindings[exchange.Name] {
		if _, duplicate := seen[binding.Queue]; duplicate {
			continue
		}

		if !deliverable(binding.Queue) {
			continue
		}

		if !bindingMatches(exchange.Kind, binding.RoutingKey, routingKey) {
			continue
		}

		seen[binding.Queue] = struct{}{}
		matched = append(matched, binding.Queue)
	}

	sort.Strings(matched)

	return matched
}

func bindingMatches(kind ExchangeKind, pattern, routingKey string) bool {
	switch kind {
	case ExchangeFanout:
		return true
	case ExchangeDirect:
		return pattern == routingKey
	case ExchangeTopic:
		return matchTopic(pattern, routingKey)
	default:
		return false
	}
}
