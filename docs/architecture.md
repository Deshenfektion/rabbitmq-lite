# Architecture

## Layers

The broker is a single Go process. Dependencies point strictly downwards; no
package imports one that sits above it.

```
                    cmd/brokerd            cmd/erpsim
                         │                      │
                         ▼                      ▼
                    internal/app  ────────► internal/erp
                         │                      │
        ┌────────────────┼──────────────┐       │
        ▼                ▼              ▼       │
  internal/api    internal/config  internal/logging
        │                                       │
        └──────────────────┬────────────────────┘
                           ▼
                    internal/engine
                           │
   ┌───────────┬───────────┼───────────┬────────────┐
   ▼           ▼           ▼           ▼            ▼
 broker     storage      retry      schema       worker
(topology)  (Store)     (policy)  (validation)   (pool)
   │           │
   │      ┌────┴─────┐
   │      ▼          ▼
   │   memory     sqlite
   ▼
 message (model + state machine)
```

| Package | Responsibility | Knows about |
|---|---|---|
| `internal/message` | Message model, identifiers, lifecycle state machine | nothing |
| `internal/broker` | Exchanges, queues, bindings, routing table | `message` |
| `internal/storage` | `Store` interface, dead-letter and history records | `broker`, `message` |
| `internal/storage/memory` | Non-durable reference implementation | `storage` |
| `internal/storage/sqlite` | Durable implementation, migrations, lease SQL | `storage` |
| `internal/retry` | Backoff policy arithmetic | nothing |
| `internal/schema` | JSON Schema registry and validation errors | nothing |
| `internal/worker` | Bounded worker pool | nothing |
| `internal/engine` | The runtime: publish, dispatch, retry, dead-letter | all of the above |
| `internal/api` | HTTP handlers, middleware, error mapping | `engine`, `schema`, `metrics` |
| `internal/metrics` | Counters, gauges, histograms, Prometheus exposition | nothing |
| `internal/config` | YAML configuration and topology declarations | `retry`, `logging` |
| `internal/app` | Wiring: config → store → engine → server | everything |
| `internal/erp` | ERP payload generators and downstream consumer simulations | `engine` |

Two decisions shape this layout.

**The broker package does not own messages.** It answers exactly one question:
*given this exchange and routing key, which queues does the message reach?* An
earlier version kept an in-memory pending list inside the broker, which
duplicated the store and made durability impossible to reason about. Splitting
topology (`broker`) from runtime (`engine`) means routing is pure, synchronous
and trivially testable, while every piece of message state lives behind
`storage.Store`.

**The storage interface is the seam.** The engine never sees SQL or a map. Both
store implementations are verified by the same conformance suite
(`internal/storage/storagetest`), so "works in memory but not on disk" bugs
surface in CI rather than in production.

## Message lifecycle

```
                      ┌───────────────────┐
                      │      CREATED      │
                      └─────────┬─────────┘
                 validation ok  │  validation failed
                      ┌─────────┴─────────┐
                      ▼                   ▼
              ┌───────────────┐   ┌───────────────┐
        ┌────►│    QUEUED     │   │    FAILED     │──► parked in the
        │     └───────┬───────┘   └───────────────┘    dead-letter store
        │             │ claimed by a consumer
        │             ▼
        │     ┌───────────────┐
        │     │  PROCESSING   │
        │     └───┬───────┬───┘
        │  lease  │       │ handler returned
        │ expired │       │
        │  ───────┘       ├──────────► ACKNOWLEDGED   (terminal)
        │                 │  success
        │                 │  error
        │                 ▼
        │         ┌───────────────┐
        │         │    FAILED     │
        │         └───┬───────┬───┘
        │             │       │ attempts exhausted, or permanent error
        │  retryable  │       └──────────► DEAD_LETTERED  (terminal)
        │             ▼
        │     ┌───────────────┐
        └─────│   RETRYING    │  waits for the backoff to elapse
              └───────────────┘
```

The transition table lives in `internal/message/state.go` and is enforced by
`Message.Transition`, which returns `*InvalidTransitionError` for anything not in
the table. Every state change in both stores goes through it, so an illegal
transition is impossible to persist regardless of which driver is in use.

Two properties are worth calling out:

- **`ACKNOWLEDGED` and `DEAD_LETTERED` are terminal.** Replaying a dead letter
  does not resurrect the message; it publishes a *new* message carrying the
  original payload plus `x-replay-of` and `x-dead-letter-id` headers. The audit
  trail of the original failure stays intact.
- **`PROCESSING → QUEUED` is legal.** That is lease expiry and explicit requeue.
  It is the transition that makes at-least-once delivery work, and it is also
  the one that makes duplicates possible.

Every transition is appended to `message_history`, so `GET /api/v1/messages/{id}`
can show exactly how a message arrived where it is.

## Concurrency model

### The shapes involved

```
producer ──HTTP──► engine.Publish ──► store.Append ──► signal channel
                                                            │
                                                            ▼
                                            ┌───── dispatch goroutine ─────┐
                                            │  (one per consumer)          │
                                            │                              │
                                            │  store.Claim(prefetch)       │
                                            │        │                     │
                                            │        ▼                     │
                                            │  pool.Submit (blocking)      │
                                            └────────┬─────────────────────┘
                                                     │ bounded channel
                              ┌──────────────────────┼──────────────────────┐
                              ▼                      ▼                      ▼
                          worker 1               worker 2               worker N
                              │                      │                      │
                              └──── handler ─────────┴──── ack / retry ─────┘
```

Per consumer there is exactly one **dispatch goroutine** and one **worker pool**
of `Concurrency` goroutines fed by a buffered channel of capacity `Prefetch`.

### Why a channel and not a semaphore

`worker.Pool.Submit` blocks when the buffered channel is full. That single
property provides backpressure end to end: if handlers are slow, the channel
fills, `Submit` blocks, the dispatch loop stops calling `store.Claim`, and
messages stay in the queue where they are durable and visible — rather than
piling up in process memory where a crash loses them and a spike causes an OOM.

The alternative — claim everything and buffer it in a slice — is faster in a
benchmark and much worse in production. Bounded is the point.

### Synchronisation choices

| Structure | Mechanism | Why |
|---|---|---|
| `broker.Registry` | `sync.RWMutex` | Reads (routing on every publish) massively outnumber writes (declarations) |
| `memory.Store` | single `sync.RWMutex` | The store's invariants span the message map and the per-queue index; one lock is the honest way to keep them consistent |
| `sqlite.Store` | one writer connection + `BEGIN`/`COMMIT` | SQLite serialises writers anyway; making it explicit avoids `SQLITE_BUSY` retry loops |
| `worker.Pool` | buffered channel + `sync.WaitGroup` | The channel *is* the queue; the wait group makes shutdown deterministic |
| Counters and gauges | `atomic.Int64` | Hot path, no coordination needed |
| Histograms | `sync.Mutex` | Bucket updates are a short critical section; lock-free would need CAS on a float |
| Queue wake-ups | buffered `chan struct{}` of size 1, non-blocking send | A coalescing signal: N publishes between two claims produce one wake-up, and a slow dispatcher never blocks a producer |

The wake-up channel deserves a note. Publishing sends on a size-1 channel with a
`default` branch, so the send never blocks. The dispatch loop selects on that
channel and on a 250 ms timer. The timer is not a fallback for lost signals — it
is what makes retries land, since a message becoming available after a backoff
produces no publish event to signal on.

### Throughput trade-offs

Three deliberate choices cost throughput and buy something else:

1. **One SQLite writer connection.** Serialises all durable writes and caps
   publish throughput at roughly 10 000/s on the benchmark machine. It buys
   freedom from lock contention handling and from `SQLITE_BUSY` retries. A
   busier deployment would want Postgres, and the `Store` interface exists so
   that swap is contained.
2. **One transaction per publish.** Batching would recover most of the 30× gap
   between the memory and SQLite paths (see [benchmarks](benchmarks.md)), but
   publish is request-scoped: the HTTP caller is told the message is durable
   when the response returns, and batching would make that a lie without a
   group-commit protocol.
3. **A dispatch goroutine per consumer, not per queue.** Two consumers on the
   same queue means two claim loops competing. Simple, and correct because
   `Claim` is atomic; the cost is redundant polling when a queue is idle.

## Delivery guarantees

**At-least-once, and only that.**

The mechanism is a visibility lease. `Claim` atomically moves a message to
`PROCESSING`, increments its attempt counter and stamps `lease_expires_at`.
While the lease holds, no other consumer can claim the message. If the consumer
acknowledges, the message is terminal. If the lease expires — the consumer
crashed, hung, or the process was killed — a background loop detects the
expiry and applies the retry policy to it.

Duplicates are therefore possible and expected in exactly one window: the
handler succeeded, but the process died before `Acknowledge` committed. The
message is redelivered, and the handler runs twice.

**Exactly-once is not offered, because it cannot be** — not by this broker and
not by any other. The broker cannot make a handler's side effects atomic with
its own acknowledgement; those are two systems, and two systems require either a
distributed transaction or idempotency. What products advertise as exactly-once
is one of:

- *effectively-once within a closed system* (Kafka transactions, where the
  offset commit and the output write land in the same log), or
- *idempotent consumers* with deduplication, which is at-least-once plus
  bookkeeping.

The honest guidance is the second: make handlers idempotent. Every delivery
carries a stable `Message.ID` and its attempt number, which is enough to
deduplicate against a processed-message table on the consumer side.

Ordering is per-queue FIFO **only for a single consumer with prefetch 1**. Any
concurrency reorders, and a retry moves a message behind newer ones by design —
backoff would be pointless otherwise. Applications needing per-entity ordering
should partition by key across queues.

## Persistence and durability

### What is stored

| Table | Holds |
|---|---|
| `exchanges`, `queues`, `bindings` | Durable topology, restored into the registry on boot |
| `messages` | Payload, headers, state, attempts, availability, lease, consumer |
| `dead_letters` | Original payload, failure reason, error kind, attempt count, replay metadata |
| `message_history` | Every state transition with actor and detail |
| `schema_migrations` | Applied migration versions |

Ordering and claim selection are served by `idx_messages_ready` over
`(queue, state, available_at, seq)`, where `seq` is an autoincrement column that
gives a stable FIFO tiebreak. Lease reclamation uses `idx_messages_lease` over
`(state, lease_expires_at)`.

### Durability settings

The shipped configuration uses `journal_mode=WAL` and `synchronous=NORMAL`.
That combination survives a **process** crash — a killed broker loses nothing,
which the `TestMessagesSurviveReopen` test asserts directly — but a **machine**
crash or power loss can lose the last WAL frames that had not reached disk.

`synchronous=FULL` closes that window at roughly an order of magnitude in
publish latency. The default here favours throughput because the workload is an
internal ERP integration where the source system can re-export; a payments
pipeline should choose differently, and the setting is one line of config.

Non-durable exchanges and queues are registered in memory only and vanish on
restart. This is deliberate: it makes the durable/non-durable distinction a
property of the declaration rather than of the deployment, exactly as in
RabbitMQ.

### Restart behaviour

On boot the engine calls `Restore`, which reloads topology from the store into
the registry. Messages need no recovery step: they are read from the store on
demand, and anything left `PROCESSING` by a crash is picked up by the lease
reclaim loop within `reclaim_interval`, then either retried or dead-lettered
according to its attempt count.

## Retry and dead-lettering

A failed delivery is classified before anything else happens:

| Classification | Trigger | Outcome |
|---|---|---|
| Transient | Any handler error | Retry with backoff, until `max_attempts` |
| Permanent | Handler returns `engine.Permanent(err)` | Dead-letter immediately, one attempt |
| Validation | Schema rejection at publish | Never enqueued; parked with `attempts: 0` |
| Lease expiry | Consumer never acknowledged | Treated as a transient failure and counted |

Backoff is `initial × multiplier^(attempt-1)`, capped at `max_interval`, with
symmetric jitter of `±jitter_fraction`. Jitter matters when a downstream system
fails for everyone at once: without it, every retry from a batch of 500 messages
lands in the same millisecond and re-creates the outage it was waiting out.

Counting lease expiry against `max_attempts` is a deliberate correctness choice.
An earlier version requeued expired leases unconditionally, which meant a
message that reliably killed its consumer was redelivered forever and never
reached the dead-letter queue — a poison-message loop. The fix moved expiry
detection into a read-only store query (`ExpiredLeases`) so the engine, which
owns the retry policy, decides what happens rather than the storage layer.

Dead letters keep the original payload, headers, routing metadata, failure
reason, error kind, attempt count and the timestamps of first failure and
dead-lettering. Replay publishes a new message onto the original queue and
records `replayed_as`, `replayed_at` and `replay_count` against the dead letter,
so a record survives its own replay.

## Configuration and startup

`internal/app` performs the wiring in a fixed order, and each step can fail the
boot cleanly:

1. Load and validate YAML, apply `RABBITMQ_LITE_*` environment overrides.
2. Build the logger (`slog`, JSON or text).
3. Open the store; run migrations.
4. Load JSON schemas from `schemas/`.
5. Construct the engine and validate the retry policy.
6. `Restore` durable topology from the store.
7. Declare the topology from configuration (idempotent — re-declaring the same
   spec succeeds, declaring a conflicting one fails the boot).
8. Build the HTTP server and start the engine.

Shutdown runs in reverse on `SIGINT`/`SIGTERM`: stop accepting HTTP requests,
cancel dispatch loops, wait for in-flight handlers to finish within the grace
period, then close the store. Messages still leased when the grace period
expires are not lost — they are reclaimed by the next process through lease
expiry.

## Deliberate omissions

- **No AMQP.** The wire protocol is HTTP/JSON. Implementing AMQP 0-9-1 framing
  would be a project of its own and would teach little about broker semantics.
- **No clustering, replication or failover.** Single node, single writer.
- **No push-based remote consumers.** Consumers are either in-process handlers
  or HTTP pollers. There is no long-lived delivery stream.
- **No priority queues, TTL, or per-message expiry.**
- **No authentication or authorisation.** The management API is unauthenticated
  and belongs behind a trusted network boundary.
- **Terminal messages are retained forever.** Acknowledged and dead-lettered
  rows accumulate; there is no retention policy or compaction job.
