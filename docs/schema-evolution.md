# Schema validation and evolution

## Why the broker validates at all

A broker does not need to understand payloads. RabbitMQ happily moves opaque
bytes, and that is the right default for a general purpose product. This project
takes the opposite position on purpose, because the problem it was built around
is an ERP integration where the failure mode is almost never "the broker lost a
message" — it is "the nightly export changed a field and eleven downstream
consumers started throwing".

Without validation the failure surfaces late: the message is accepted, delivered,
retried three times, and finally dead-lettered with a stack trace from whichever
consumer happened to dereference the missing field first. The producer, which is
the only party that can actually fix the problem, never learns about it.

With validation at the publish boundary the failure surfaces immediately, at the
producer, with a field-level reason:

```
422 Unprocessable Entity
{
  "error": "schema_validation_failed",
  "schema": "invoice",
  "violations": [
    {"path": "/lines/0/quantity", "detail": "got string, want number"},
    {"path": "/currency", "detail": "value must be one of \"EUR\", \"USD\", \"CHF\", \"GBP\""}
  ]
}
```

The trade-off is real and worth stating: validation costs latency on the publish
path (roughly 5–20 µs for the ERP schemas shipped here), it couples the broker to
payload structure, and it means a schema bug can block an otherwise healthy
pipeline. That price is acceptable for a small internal integration bus. It would
not be acceptable for a general purpose broker.

## How it works here

- Schemas are JSON Schema 2020-12 documents in `schemas/`, named
  `<name>.schema.json`. The file name is the schema name.
- A queue may declare `schema: <name>`. Every message routed to that queue is
  validated against it before it is written to storage.
- A publication may override the schema per message via the `schema` field.
- Validation failures are rejected (`ErrValidation` → HTTP 422) and the offending
  payload is parked in the dead-letter store with `error_kind: "validation"` and
  `attempts: 0`, so a rejected export can still be inspected and replayed after
  the schema or the producer is fixed.
- `x-schema-version` is an annotation carried in the document. The broker records
  it and exposes it on `GET /api/v1/schemas`; it does not interpret it.

Validation happens before `CREATED → QUEUED`. A rejected message never occupies
queue depth and never consumes a delivery attempt.

## Compatibility, and why it is the hard part

Registering a schema is easy. Changing one is not. The useful mental model is
that a schema change has a direction, and the direction determines who breaks.

**Backward compatible** — new consumers can read data written under the old
schema. This is what you need when consumers are upgraded first.

- adding an optional field
- widening an enum that consumers only read
- relaxing a constraint (`minLength: 3` → `minLength: 1`)

**Forward compatible** — old consumers can read data written under the new
schema. This is what you need when producers are upgraded first.

- adding a field that old consumers ignore, *provided* they ignore unknown fields
- removing an optional field

**Full compatibility** is the intersection, and in practice it means: add
optional fields, never remove required ones, never repurpose a field name.

The changes that break everything are the boring ones:

- making an optional field required — every in-flight message written before the
  change now fails validation, including messages already sitting in a queue
- narrowing a type (`string` → `integer`) — the classic "the ERP used to send
  `"12"` and now sends `12`" migration
- renaming a field — this is a delete plus an add, and it breaks both directions
  at once
- tightening an enum — the value your one weird legacy branch office still sends
  is now invalid

### The `additionalProperties: false` decision

The ERP schemas here set `additionalProperties: false`. That is a deliberate,
strict choice: it catches typos in export mappings (`custmer_id`) that would
otherwise silently produce records with missing data. The cost is that it makes
the schemas *forward incompatible by construction* — a producer adding a new
field breaks immediately, before any consumer sees it.

For an integration where the producer is a single ERP export job, failing loudly
is the right call. For a public event bus with many independent producers, the
opposite default (`additionalProperties: true`, validate only what you consume)
is almost always correct.

## What this project does not solve

Production systems solve schema evolution with a **schema registry** —
Confluent Schema Registry for Kafka being the reference implementation — which
adds several things this project deliberately does not have:

- **Versioned subjects.** Each schema has a history, not just a current
  document. A consumer can ask for the exact version a message was written with.
- **Enforced compatibility levels.** The registry rejects a schema update that
  violates the configured level (`BACKWARD`, `FORWARD`, `FULL`, `NONE`) at
  registration time, rather than at message time.
- **Schema IDs on the wire.** Kafka producers prefix the payload with the schema
  ID used to serialise it, so a consumer resolves the exact writer schema instead
  of guessing. This is what makes Avro's reader/writer schema resolution work.
- **Binary encodings.** Avro and Protobuf make the schema load-bearing: fields
  are positional, so evolution rules are enforced by the encoding itself rather
  than by convention.

Here, by contrast: registration overwrites in place, compatibility is checked by
whoever reviews the pull request, and the payload is self-describing JSON. That
is a fine trade for a single-node broker with one producer. It is exactly the set
of shortcuts that stops working once several teams publish to the same exchange.

## Practical migration recipe

The approach that works without a registry, in order:

1. Add the new field as **optional**, deploy the schema.
2. Deploy producers that populate it. Both shapes are now valid.
3. Wait until every message written under the old shape has been consumed —
   for durable queues this means draining, not just waiting.
4. Deploy consumers that depend on the new field.
5. Only now make the field required, and bump `x-schema-version`.

Step 3 is the one that gets skipped, and it is the one that causes the incident.
The dead-letter store is the safety net: messages rejected during a botched
migration are preserved with their original payload, so once the schema is
corrected they can be replayed with `POST /api/v1/dead-letter/{id}/retry`.
