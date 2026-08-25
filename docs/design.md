# Design: workloads as graphs of scaled operations

## Purpose

Kubernetes describes a workload as a set of independent controllers
(Deployments, Jobs, StatefulSets) that happen to share a namespace. The
relationships between them, which pod pool feeds which, in what order, with
what partitioning, live in application code and are invisible to the
cluster. stark8s makes those relationships a declared object so that the
cluster can schedule, scale, and isolate each part of a workload from
knowledge of the whole.

The concrete goal is to run a shuffle-based batch engine, Apache Spark being
the reference, with each stage as its own independently sized pod pool, and
to let iterative algorithms be written as cycles rather than as loops in a
driver program.

## Primitives

The model has three nouns. Everything else is an attribute of one of them.

### Workload

The graph. One Kubernetes object (`Workload`, group `stark8s.io`) holds the
complete list of operations and channels. The lifecycle of the workload is
the lifecycle of the graph: it is `Running` while any operation runs,
`Succeeded` when every batch operation has drained and no streaming
operation exists, and `Failed` when any operation fails or the graph is
invalid.

The operations and channels are embedded in the one object rather than being
separate objects, so that a partial graph is never a valid state and the
controller always sees the whole topology in one read.

### Operation

A vertex: one logical computation backed by its own pool of pods. An
operation declares:

- a pod template, exactly as in a Deployment or Job;
- a completion rule: `Drain` (finish once every inbound channel is sealed
  and consumed; batch) or `Never` (run until the workload is deleted;
  streaming);
- horizontal scaling bounds and the signal that drives them, which is the
  backlog on the operation's inbound channels, optionally with CPU
  utilization for streaming operations;
- an optional vertical scaling mode.

An operation with no inbound channels is a source: its pods run once and
exit. An operation with no outbound channels is a sink.

### Channel

A directed flow of records from one operation to another. It is the only
kind of edge. A channel declares:

- **partitioning**: `Hash` (records with equal keys always reach the same
  consumer replica), `RoundRobin` (no key affinity; freely rebalanced), or
  `Broadcast` (every replica receives every record). Hash partition
  ownership is held per consuming operation rather than per channel, so
  two hash channels with the same partition count into one operation are
  co-partitioned: the replica that owns partition p of one owns partition
  p of the other. A join with two inputs, or a loop vertex whose state
  arrives on one channel and whose updates arrive on another, depends on
  this;
- **delivery**: `Pipelined` (records are delivered as they are produced;
  the consumer runs concurrently with the producer) or `Materialized`
  (nothing is delivered until the producer has completed; the consumer is
  not even started until then);
- **durability**: `Ephemeral` (discard after acknowledgement) or
  `Retained` (keep, so the channel can be replayed or read from outside);
- optionally **feedback**, which marks the channel as closing a cycle and
  gives the loop bound.

Either end of a channel may be left empty. A channel with no producer is
fed from outside the workload through the exchange API; a channel with no
consumer is a result that is read from outside.

## Cycles

A graph may contain cycles, but a cycle without semantics is a deadlock or
an unbounded queue, so cycles are constrained: with feedback channels
removed, the graph must be acyclic, and the controller rejects a workload
that violates this.

A feedback channel runs as bulk-synchronous supersteps. Every record carries
an epoch. Records produced into a feedback channel are stamped with the
epoch after the one being consumed. The exchange delivers only records of
the current epoch and holds later ones. When every consumer replica has
reported the current epoch finished and no records of that epoch remain
pending or in flight, the exchange releases the next epoch. When the epoch
count reaches the declared bound, the channel is sealed, held records are
discarded, and the consuming operation drains and completes like any batch
operation.

The worker library exposes this as one callback, invoked once per epoch when
the loop is quiescent, in which the application emits the next epoch's
records. PageRank in `examples/pagerank` is thirty lines of application
code under this protocol.

An unbounded asynchronous loop, in which epochs are never closed, is a
streaming operation consuming a feedback channel with completion `Never`.
That configuration is accepted but the exchange still applies the barrier;
a truly asynchronous loop would need a delivery mode that skips it, which
is not implemented.

## Review of the primitive set

An earlier sketch of this design had five kinds of edge: data flow, control,
broadcast, feedback, and affinity. The review below explains why the
released model has one, and what became of the other four.

**Broadcast is a partitioning mode.** A broadcast edge carries records like
any other; the only difference is that every consumer replica sees all of
them. That is a value of the partitioning attribute, next to hash and
round-robin. Making it an edge kind would have duplicated every other
channel attribute (delivery, durability) on a second type.

**Control is a degenerate channel.** "B may start after A completes" is a
Materialized channel with no records: the consumer is gated on the seal,
which is exactly the barrier a Materialized channel already provides. A
separate control edge would need its own ordering semantics that could
disagree with the data edges. The model therefore has no control edge;
ordering is a consequence of delivery mode. Richer predicates (start B when
A reaches some state other than completion) are not expressible, and that
is a deliberate boundary: such predicates belong to the application or to
an external workflow engine, and would reintroduce a second, unrelated
notion of edge.

**Feedback is an attribute of a channel.** A feedback channel is an ordinary
channel that closes a cycle. Its records are partitioned, delivered, and
retained like any other; the epoch barrier is layered on top. Making it an
attribute also lets a cycle pass through several operations (A to B to A):
the channel that closes the cycle carries the attribute and the others are
plain channels, with the epoch propagated through them by the records.

**Affinity is a property of operations.** "A and B should share nodes" is a
scheduling constraint and carries no information flow. It belongs in the pod
template as Kubernetes pod affinity, where it already exists, selecting the
other operation's pods by the `stark8s.io/operation` label that the
controller applies. Encoding it as an edge would have created a second edge
vocabulary with no shared semantics.

**Durability stays on the channel.** The one attribute that could plausibly
move elsewhere is durability, since it is a property of the transport. It
stays on the channel because it determines recovery semantics (whether the
producer must rerun after a consumer failure), and that is a property of
the flow rather than of the implementation.

The result is that every edge answers four questions with the same
vocabulary: how are records split across the consumer's replicas, when are
they visible, what happens to them afterward, and does this edge close a
loop. Everything the earlier five kinds expressed is still expressible.

## What the explicit graph buys

- **Stage scheduling.** A consumer of a Materialized channel is not started
  until its producer completes. Pods for later stages consume no resources
  while earlier stages run, and the parallelism of a stage is chosen from
  the actual size of its input when it starts.
- **Per-operation scaling from a topology-aware signal.** The backlog on an
  operation's inbound channels measures directly whether the operation is
  under-provisioned. This needs no application metrics and no knowledge of
  what the operation computes.
- **Least-privilege networking from the topology.** Operation pods are
  permitted to reach only the exchange, and the exchange enforces per
  channel which operation may produce and which may consume. See
  [kubernetes-mapping.md](kubernetes-mapping.md) for the enforcement
  layers and their limits.
- **A target for planners.** Because the graph is data, any planner can
  emit it. Spark's physical plan is one source; the workload can also be
  edited while running, which the adaptive planners in such engines
  require. See [spark-mapping.md](spark-mapping.md).

## Failure model

Delivery is at-least-once. Records are acknowledged after processing; a
consumer replica that stops polling for twenty seconds is expired and its
unacknowledged records are returned to the queue for another replica.
Application state held by the expired replica is lost, so an operation that
accumulates state (a reducer, a loop vertex) is correct only if its pods do
not fail. Recovery from that class of failure requires checkpointed
operation state or a lineage re-execution of the producing stage. Neither is
implemented; the durability attribute on channels is the hook for the second
(a `Retained` channel can be replayed to a restarted consumer, but the
restart itself is not automated).

## Out of scope

- Scheduling across workloads (gang scheduling, queueing, quotas). A
  Workload's pods are ordinary pods; a scheduler such as Volcano or Kueue
  can manage them by label.
- Data formats. Records are opaque JSON values with a string key.
- Exactly-once processing.
