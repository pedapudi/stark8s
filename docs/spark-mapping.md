# Expressing Apache Spark workloads as Workloads

Spark compiles a job into a physical plan: a directed acyclic graph of
stages, where a stage is a chain of narrow transformations (map, filter,
flatMap, and joins whose inputs are already co-partitioned) executed as
parallel tasks, and stages are separated by shuffle dependencies. Every
concept in that plan has a direct counterpart in a Workload. This document
gives the mapping and then the three places where the mapping exposes
something Spark's own execution model handles differently.

## The mapping

| Spark | Workload |
|---|---|
| job | Workload |
| stage | operation with completion `Drain` |
| narrow dependency inside a stage | none; fused into the operation's code, as Spark fuses it into a task |
| shuffle dependency (`ShuffleExchangeExec`) | channel, `partitioning: Hash` on the shuffle key, `delivery: Materialized`, `partitions` = the shuffle's partition count |
| range partitioning (sort, `RangePartitioning`) | hash channel whose producer maps keys to range buckets before emitting |
| broadcast exchange (`BroadcastExchangeExec`, broadcast joins) | channel, `partitioning: Broadcast` |
| `coalesce` without shuffle | fewer consumer replicas on a round-robin channel |
| task | one record batch delivered to one replica |
| executor pool | one pool per operation instead of one per job |
| dynamic allocation | `scaling.horizontal` with `targetBacklogPerReplica` |
| executor memory and cores | the operation's pod template resources, per stage |
| action (`collect`, `save`) | a sink operation, or a channel with no consumer read from outside |
| iterative algorithm (MLlib, GraphX Pregel) | a feedback channel with `maxEpochs` |
| driver | disappears for scheduling; remains as the planner that emits the Workload |

Worked example, a join followed by an aggregation:

```
scan A ──┐
         shuffle(hash on k) ──> join ── shuffle(hash on g) ──> aggregate ──> write
scan B ──┘
```

```yaml
operations:
  - {name: scan-a,    scaling: {horizontal: {min: 4, max: 64, targetBacklogPerReplica: 1000}}, template: ...}
  - {name: scan-b,    scaling: {horizontal: {min: 4, max: 64, targetBacklogPerReplica: 1000}}, template: ...}
  - {name: join,      scaling: {horizontal: {min: 8, max: 200, targetBacklogPerReplica: 5000}}, template: ...}   # memory-heavy template
  - {name: aggregate, scaling: {horizontal: {min: 2, max: 50,  targetBacklogPerReplica: 20000}}, template: ...}
  - {name: write,     scaling: {horizontal: {min: 1, max: 8}}, template: ...}
channels:
  - {name: a-by-k, from: scan-a, to: join,      partitioning: {mode: Hash, partitions: 200}, delivery: Materialized}
  - {name: b-by-k, from: scan-b, to: join,      partitioning: {mode: Hash, partitions: 200}, delivery: Materialized}
  - {name: by-g,   from: join,   to: aggregate, partitioning: {mode: Hash, partitions: 50},  delivery: Materialized}
  - {name: rows,   from: aggregate, to: write,  partitioning: {mode: RoundRobin, partitions: 8}, delivery: Pipelined}
```

The two scans run concurrently. The join is not started until both shuffles
are sealed, and its parallelism at start is derived from the size of the
sealed shuffles. The join's pods are a different shape from the scans'
pods. Because both inputs to the join are hashed with the same partition
count, and hash assignment is sticky, each join replica receives the
matching partitions of both inputs; this is the co-partitioning that a
shuffle-hash join relies on.

The record boundary is the one place where the mapping is not free: Spark
moves serialized rows in blocks, while a channel moves keyed JSON records.
A production transport would carry opaque byte blocks with the key
extracted by the producer, which the channel model already permits since
the exchange never inspects values.

## Where the mapping exposes something

### The plan is not known at submission

Spark materializes stages lazily as actions run, and adaptive query
execution rewrites the plan between stages: it coalesces shuffle
partitions, converts sort-merge joins to broadcast joins when one side turns
out small, and splits skewed partitions. A static graph cannot represent
this.

The controller accepts this by treating the Workload as editable while it
runs. The channel list is pushed to the exchange on every pass and new
operations are created when they appear. A planner therefore submits the
first stages, observes the sealed channels' metrics (record counts per
partition are available from the exchange), and appends the next stages
with their partition counts and join strategy chosen from those metrics.
Adaptive execution becomes a client of the API rather than something the
runtime must anticipate.

Not implemented: removing or replacing an operation or channel that has
already been created, which a planner needs when it replaces a planned
sort-merge join with a broadcast join after the join operation was already
declared. The safe protocol is to declare a stage only when its inputs are
sealed, which is when the planner has the information anyway.

### One executor pool per stage changes the failure model

Spark's executors are generic and long-lived; a stage's tasks are
scheduled onto whichever executors are free, and a lost executor loses only
the shuffle blocks it wrote, which are recomputed from lineage. Under this
mapping, an operation's pods are specific to a stage. That is the point
(each stage gets the resources it needs and nothing else), but it means:

- shuffle data must live outside the producing pods, since the producer's
  pods are gone when the consumer runs. That is the exchange in this
  implementation and a remote shuffle service (Celeborn, Uniffle, or a
  push-based service) in production. Spark already supports these, so the
  executor side needs no change;
- a stage that loses a pod mid-run loses that pod's in-memory state. For a
  map stage this is harmless; unacknowledged records are redelivered. For
  a stateful stage (an aggregation) the correct recovery is to rerun the
  stage from its sealed inputs, which is Spark's lineage recovery at stage
  granularity. The controller does not yet do this.

### Task granularity versus pod granularity

A Spark stage has as many tasks as partitions, often thousands, scheduled
onto tens of executors. Here an operation has tens of pods and a channel
has some number of partitions. Two ways to reconcile them:

- **Partition pull (implemented).** Replicas pull partitions from the
  channel like a consumer group. Hash partitions are bound to replicas;
  round-robin partitions are rebalanced. The driver's task scheduler is
  not needed.
- **Task scheduler inside the operation.** Keep Spark's driver and task
  protocol and treat each operation's pods as that stage's executor pool.
  This is a smaller change to Spark but leaves scheduling in the driver.

The partition-pull model is simpler and is what the examples use. It is
also the reason `partitions` should be sized for the maximum replica
count: with sticky hash assignment, partitions are the unit of
parallelism.

## Iteration without a driver loop

Spark expresses iteration by re-running stages from a driver loop; each
iteration is new stages, and the DAG grows with the iteration count.
GraphX's Pregel operator is such a loop with per-iteration joins. Under this
model an iterative algorithm is one operation with a feedback channel to
itself, and iteration count is a channel attribute. `examples/pagerank`
runs twenty supersteps of PageRank with one operation, one feedback
channel, and no driver.

The barrier is bulk-synchronous, which matches Pregel and Spark's
iteration exactly. Asynchronous iteration, which some graph systems offer,
would need a feedback delivery mode without the barrier, which is not
implemented.

## What a translator would do

A `spark-plan-to-workload` tool would walk the physical plan, emit one
operation per stage with the stage's fused code as the container command,
one hash channel per `ShuffleExchangeExec` with its partitioning, one
broadcast channel per `BroadcastExchangeExec`, and a sink for each action.
Resource shapes per stage come from the planner's statistics or from a
previous run's VerticalPodAutoscaler recommendations. Such a tool is not
part of this repository; the mapping above is its specification.
