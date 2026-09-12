# Supported system shapes

These mappings describe contracts enforced by the runtime.

| Workload shape | Mapping | Boundary |
|---|---|---|
| Shuffle batch | Operations separated by materialized hash channels | At-least-once delivery; checkpoint recovery has one fixed owner |
| Pipelined processing | `Pipelined` channels with `Drain` or `Never` operations | No event time, watermarks, windows, or managed keyed state |
| Finite iteration | A synchronous feedback cycle with one scalar epoch and finite bound | The connected component advances together |
| Retained log | Named subscription cursors through the coordinator HTTP API | No declarative graph binding or normal worker client path |
| File ETL | Immutable input versions, stable splits, attempt output, and an atomic dataset manifest | Basic JSON Lines map/filter and integer aggregation only |
| Fixed-size collective | An indexed Job with rank, group size, attempt, checkpoint reference, and rendezvous | No gang placement or membership change within an attempt |
| Local application | Serial callbacks through the local HTTP runtime and Go or Python client | A transport protocol, not framework compatibility |

## Batch and streaming

Materialized channels act as stage barriers. Hash channels preserve partition
affinity under fixed membership. Active ordinary operations run at
`scaling.horizontal.max`; `slots` does not create callback concurrency.

Pipelined channels overlap producers and consumers. A finite external writer
must seal its channel. A source or sink that contacts another system owns its
offsets, retry policy, and side-effect idempotency.

## Durable logs and ETL

Configured object storage preserves segment bytes and coordinator state. Named
subscriptions keep independent absolute partition positions and idempotent
append identifiers. An explicit client can append, consume, acknowledge,
replay, and delete retained history. Current manifests and worker clients do
not bind an operation to a subscription. See [Retained
subscriptions](subscriptions.md).

The ETL helper checks immutable input versions, derives stable split IDs,
writes deterministic attempt partitions, and publishes one dataset manifest by
compare-and-swap. It is not a query engine or transaction layer for external
systems.

## Checkpoints and collectives

Single-owner operation checkpoints commit application state, input coverage,
callback completion, and durable internal output. External sinks require their
own transaction or idempotency boundary.

Collective membership is fixed for each indexed Job attempt. Internal output
requires object storage. A retrying collective connected to graph channels also
requires operation checkpointing. A framework checkpoint reference does not
fence graph records. A collective without graph channels may retry from its
framework checkpoint. Gang placement is unsupported.

## Deferred contracts

The runtime does not implement event-time processing, managed keyed state,
channel authentication, shared resource budgets, live graph replacement,
dynamic collective membership, or general external-side-effect transactions.
