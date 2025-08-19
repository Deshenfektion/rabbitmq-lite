# Benchmarks

## What is being measured

The goal of these benchmarks is narrow: understand where time goes inside the
broker, and confirm that queue ingestion stays well below the 50 ms budget the
ERP export job needs. They are **not** a comparison against RabbitMQ, Kafka or
any other product, and the numbers below say nothing about how this broker would
behave on a loaded server with a network in the path.

Three questions:

1. How long does `Publish` take, from accepting a request to the message being
   durably queued?
2. What does the async delivery path cost per message, and does it scale with
   worker count?
3. What does a retry cost compared to a first-attempt success?

## Methodology

- Benchmarks live next to the code they measure: `internal/engine/bench_test.go`
  and `internal/storage/storagetest/bench.go`.
- Run with `make bench`, which is `go test -run '^$' -bench . -benchmem ./internal/...`.
- The figures below were produced with `-benchtime 2000x`, so every benchmark
  performs exactly 2000 iterations. Fixed iteration counts matter here: the
  end-to-end benchmarks publish `b.N` messages and wait for the last one to be
  acknowledged, so a variable `b.N` produces incomparable runs.
- Latency percentiles are collected inside the benchmark by timing each
  `Publish` call and reporting `p50_ms`, `p95_ms`, `p99_ms` and `max_ms` via
  `b.ReportMetric`.
- Everything runs in a single process. Producers, broker and consumers share a
  CPU, so there is no network, no TLS, and no serialisation across a socket.
- The payload is a realistic single-line ERP invoice, 176 bytes of JSON.

### Hardware and software

| | |
|---|---|
| Machine | Apple MacBook Pro, M1 Pro (10 cores: 8 performance, 2 efficiency) |
| Memory | 16 GB unified |
| Storage | Built-in NVMe SSD (APFS) |
| OS | macOS 15 |
| Go | 1.24.5 (darwin/arm64) |
| SQLite | `modernc.org/sqlite` v1.38.0, WAL, `synchronous=NORMAL`, one writer connection |

## Ingestion latency

`Publish` covers routing key matching, schema validation, message construction,
the durable write and the history append.

| Benchmark | ns/op | p50 | p95 | p99 | max | allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| `PublishMemory` | 3 598 | 0.002 ms | 0.006 ms | 0.025 ms | 0.195 ms | 25 |
| `PublishSQLite` | 104 624 | 0.083 ms | 0.142 ms | 0.277 ms | 1.93 ms | 76 |
| `PublishFanout` (3 queues, memory) | 5 221 | – | – | – | – | 63 |
| `PublishWithSchemaValidation` | 7 884 | 0.006 ms | 0.012 ms | 0.049 ms | 0.176 ms | 116 |

Reading these:

- **The 50 ms ingestion budget is met with a very large margin.** The durable
  path (`PublishSQLite`) sits at 0.08 ms median and 0.28 ms at p99 — roughly two
  orders of magnitude below the target. The budget was set by the ERP job's
  tolerance, not by anything the broker struggles with.
- **Durability costs about 30×.** 3.6 µs in memory versus 105 µs on SQLite. That
  gap is the fsync-adjacent cost of a WAL commit per publish, and it is the
  single biggest lever in the system. Batching publications into one transaction
  would recover most of it; the current API commits per call because publish is
  request-scoped.
- **Schema validation costs about 4 µs** on top of an in-memory publish for this
  invoice schema — around 0.004 ms, or roughly 4 % of a durable publish. This is
  cheap enough that validating on the hot path is not a real trade-off at this
  payload size. It would look different for a 100 KB document with a deeply
  nested schema.
- **Fanout is sub-linear in queue count**: 3 queues cost 1.45× a single queue,
  not 3×, because routing happens once and only message construction repeats.
- The p99/max spread on SQLite (0.28 ms vs 1.93 ms) is WAL checkpointing. It is
  visible, bounded, and would need `wal_autocheckpoint` tuning to smooth out.

## Delivery throughput

`EndToEndDelivery` publishes `b.N` messages and blocks until the consumer has
acknowledged the last one, so ns/op is the full publish → claim → handle → ack
cycle with a no-op handler on the in-memory store.

| Workers | ns/op | messages/s | allocs/op |
|---|---:|---:|---:|
| 1 | 11 489 | ~87 000 | 40 |
| 4 | 11 956 | ~84 000 | 40 |
| 16 | 13 162 | ~76 000 | 40 |

**Adding workers does not help, and past a point it hurts.** That is the honest
and expected result: the handler does no work, so the benchmark measures pure
coordination. With nothing to parallelise, extra goroutines only add scheduler
pressure and contention on the store lock. Worker pools pay off when handlers
block on I/O — which is exactly what `cmd/erpsim` demonstrates, where a 2 ms
simulated downstream call makes concurrency the dominant factor.

The number to take from this table is the coordination floor: roughly 12 µs and
40 allocations per message for dispatch, lease, handler invocation and ack.

## Retry overhead

`RetryOverhead` uses a policy with a 1 ms initial interval and a 2× multiplier,
so the expected added delay is ~1 ms for one failure and ~3 ms for two.

| Failures before success | ns/op | Δ vs no failure |
|---|---:|---:|
| 0 | 11 672 | – |
| 1 | 142 526 | +131 µs |
| 2 | 279 990 | +268 µs |

Nearly all of the added cost is deliberate waiting, not work: the backoff timer
plus one extra claim/dispatch cycle. Amortised across the 4 concurrent workers
in the benchmark, a retry costs about 131 µs of wall clock per message. The
mechanical overhead — the state transitions, the history rows, the re-claim — is
a small fraction of that.

The practical consequence: **retry cost is dominated by the policy, not the
implementation.** A 500 ms initial interval (the shipped default) makes a single
retry roughly 4 000× more expensive than the machinery around it. Tune the
policy to the downstream system's recovery time, not to the broker.

## Storage layer

Measured directly against the `storage.Store` interface, bypassing the engine.

| Operation | Memory | SQLite | Ratio |
|---|---:|---:|---:|
| `AppendSingle` | 2 470 ns | 70 879 ns | 29× |
| `AppendBatch32` | 44 809 ns (1 400 ns/msg) | 616 299 ns (19 259 ns/msg) | 14× |
| `Claim` (1 message) | 26 175 ns | 110 923 ns | 4× |
| `DeliveryCycle` (claim + ack) | 27 208 ns | 169 201 ns | 6× |

Batching is the clearest win available: appending 32 messages in one SQLite
transaction costs 19 µs per message against 71 µs for individual appends, a 3.7×
improvement, because the WAL commit is amortised.

### Known issue: memory store claim is O(pending)

`BenchmarkMemoryStore/Claim` costs 4.4 µs at `-benchtime 200x` but 26 µs at
`-benchtime 2000x`. That is not noise — the in-memory store keeps a per-queue
insertion-ordered index that it never prunes, so every claim rescans the
acknowledged messages ahead of the cursor. The cost grows with the number of
messages the queue has ever held rather than with the number currently pending.

SQLite does not have this problem: `idx_messages_ready` covers
`(queue, state, available_at, seq)`, so claims are an index range scan.

This is worth fixing rather than documenting away, and the same caveat applies
to `BenchmarkClaimBatch`, whose growth with `-benchtime` has the same cause.

## Limitations

These numbers are a floor, not a forecast. What they exclude:

- **No network.** Real producers pay HTTP request parsing, TLS and RTT. The
  management API adds roughly 60–120 µs per request on loopback, which already
  exceeds the in-memory publish cost.
- **No contention from a real workload.** Benchmarks run one operation at a
  time against a quiet machine. The single-writer SQLite connection is the first
  thing that will queue under concurrent publishers, and none of these figures
  show that.
- **macOS on Apple Silicon is not a server.** APFS fsync behaviour, unified
  memory and the P/E core split all differ from a Linux server on network
  storage. Expect SQLite writes in particular to look different — often slower
  on network-backed volumes, sometimes faster on a dedicated NVMe device.
- **No-op handlers.** Every delivery benchmark uses a handler that returns
  immediately. Real handlers dominate the measurement.
- **Single node, single process.** There is no replication, no failover and no
  cross-process coordination to pay for.
- **Small payloads.** 176 bytes. Serialisation and storage costs scale with
  payload size; validation cost scales with schema complexity.

## Reproducing

```sh
make bench

go test -run '^$' -bench 'Publish' -benchmem -benchtime 2000x ./internal/engine/
go test -run '^$' -bench . -benchmem -benchtime 2000x ./internal/storage/...
```

Benchmarks also run in CI on every push to `main`, on GitHub-hosted runners.
Those numbers are consistently slower and noisier than the figures above; treat
CI runs as regression detection, not as measurements.
