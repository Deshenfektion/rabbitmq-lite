# rabbitmq-lite

A small, durable message broker in Go — exchanges, bindings, topic routing,
durable queues, retries, dead-letter queues, schema validation. Single node,
HTTP instead of AMQP, not a RabbitMQ replacement. Built to understand how a
broker actually works, after a synchronous ERP integration got taken down by
one bad record blocking a whole nightly batch.

**Stack:** Go 1.24 · SQLite (WAL) or in-memory · Prometheus · `slog` · distroless Docker

## Features

- `direct` / `fanout` / `topic` exchanges, `*`/`#` wildcard routing
- Visibility leases, at-least-once delivery, prefetch, bounded worker pools
- Exponential backoff with jitter, permanent-error classification
- Dead-letter queues — inspect, replay, discard
- JSON Schema validation at the publish boundary, field-level errors
- Two storage drivers, one shared conformance test suite

## Run it

```sh
make build && make run        # broker on :8080, ERP topology loaded from config/broker.yaml
```

```sh
curl -s -X POST localhost:8080/api/v1/messages -d '{
  "exchange": "erp.events", "routing_key": "invoice.issued",
  "payload": {"invoice_id": "INV-1", "customer_id": "C-1", "currency": "EUR",
              "issued_on": "2025-08-25", "lines": [{"position": 1, "sku": "X", "quantity": 1, "unit_price_net": 1}]}
}'
curl -s -X POST localhost:8080/api/v1/queues/invoice-processing/consume -d '{"consumer": "cli", "limit": 5}'
curl -s -X POST localhost:8080/api/v1/messages/$ID/ack
```

`./bin/erpsim -events 200 -failure-rate 0.2` replays a simulated nightly export
end to end. `make docker` builds the distroless image.

Full endpoint list, config and consumer API: [docs/architecture.md](docs/architecture.md).

## Numbers

Apple M1 Pro, in-process, `-benchtime 2000x` — full methodology in
[docs/benchmarks.md](docs/benchmarks.md):

| Path | p50 | p99 |
|---|---:|---:|
| Publish, in-memory | 0.002 ms | 0.019 ms |
| Publish, SQLite (durable) | 0.082 ms | 0.516 ms |
| Publish + schema validation | 0.006 ms | 0.042 ms |

Target was sub-50ms queue ingestion for the ERP job; the durable path clears
it by roughly two orders of magnitude.

## Testing

```sh
make test        # unit + integration
make test-race   # same, under -race
make cover        # coverage report
```

Covers: state machine transitions, exchange routing (including wildcard edge
cases), storage conformance across both drivers, worker-pool concurrency
under `-race`, retry/dead-letter/replay, schema validation, and the full HTTP
surface.

## Known limitations

At-least-once only, single node (no clustering/failover), HTTP/JSON only (no
AMQP clients), no auth, ordering not guaranteed beyond one consumer at
prefetch 1, terminal messages retained forever. Full list with reasoning in
[docs/architecture.md](docs/architecture.md).

## Docs

- [Architecture](docs/architecture.md) — layering, concurrency, delivery guarantees
- [Benchmarks](docs/benchmarks.md) — methodology, results, limitations
- [Schema evolution](docs/schema-evolution.md) — validation design, what a real registry adds
