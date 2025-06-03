# rabbitmq-lite

A small message broker written in Go, built to understand how RabbitMQ-style
messaging actually works underneath the client libraries.

The motivation comes from work on an ERP integration where nightly exports were
processed synchronously: one slow downstream call blocked the whole batch, and a
single malformed record failed the entire run. The obvious answer is a message
broker. This project is an attempt to build a very small one instead of only
using one.

## Status

Early work in progress.

- [ ] message model and lifecycle
- [ ] queues, exchanges, bindings
- [ ] persistence
- [ ] worker pool and consumers
- [ ] retries and dead-letter queue
- [ ] schema validation
- [ ] management API
- [ ] benchmarks

## Non-goals

This is not a RabbitMQ replacement. There is no AMQP wire protocol, no
clustering, and no mirrored queues. It is a single-node broker used to explore
routing, delivery guarantees, retry semantics and backpressure.
