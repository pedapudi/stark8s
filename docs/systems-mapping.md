# Which distributed systems a Workload can represent

A Workload is a directed graph held in one Kubernetes object. Each vertex,
called an operation, is one computation backed by its own pool of pods. Each
edge, called a channel, carries keyed records from one operation to another.

A channel declares four things: how records are split across the consuming
operation's replicas, when those records become visible, what happens to them
after they are consumed, and whether the edge closes a cycle. Those four
attributes are the entire vocabulary of an edge. The claim this document tests
is that the same four attributes, taking different values, carry batch
engines, stream processors, log brokers, task and actor runtimes, and agent
frameworks. Where a system needs something the four attributes cannot say, the
missing piece is named.

[precedent.md](precedent.md) surveys the research and production systems that
shaped this design, so it runs backward from the model to its influences. This
document runs the other way, from the model to what someone could build on it.
The same names appear in both. Where the history matters, follow the link
instead of looking for it here.

Two companion documents walk individual systems concept by concept:
[spark-mapping.md](spark-mapping.md) for Spark and
[agent-graphs.md](agent-graphs.md) for LangGraph. Neither is restated below.
[design.md](design.md) argues the primitive set from the inside, and
[kubernetes-mapping.md](kubernetes-mapping.md) specifies what the controller
creates.

Everything here describes the implementation as it stands. Proposed work that
has not merged is collected in [Work proposed and not
merged](#work-proposed-and-not-merged) and is marked wherever it comes up.

## The four attributes, and the jobs they do

| attribute | values | what it decides |
|---|---|---|
| partitioning | `Hash`, `RoundRobin`, `Broadcast`, with a partition count | which consumer replica receives a record |
| delivery | `Pipelined`, `Materialized` | whether the consumer runs while the producer is still running |
| durability | `Ephemeral`, `Retained` | whether a record survives its acknowledgement |
| feedback | absent, `Synchronous`, `Asynchronous`, with a loop bound | whether the edge closes a cycle, and how the loop is paced |

Either endpoint may be left empty. A channel with no producing operation is
fed from outside the Workload, and a channel with no consuming operation is
read from outside it.

Two properties live on the operation rather than on the edge, and both matter
below. The completion rule says whether an operation finishes when its inputs
are exhausted (`Drain`) or runs until the Workload is deleted (`Never`). The
scaling bounds say how many replicas it may have.

### Partitioning decides which replica receives a record

`Hash` sends every record with the same key to the same replica. That one rule
does three unrelated jobs. It is a shuffle when the key is a grouping column
and the partition count is large. It is an actor when the partition count is
one, because a single replica then owns every record on the channel and can
hold state for it. It is a conversation thread when the key is a thread
identifier, because every message of one conversation reaches one replica.

Hash partition ownership is held per consuming operation rather than per
channel. Two hash channels with the same partition count into one operation
are therefore co-partitioned: the replica owning partition three of one owns
partition three of the other. A two-input join relies on this, and so does any
operation whose state arrives on one channel and whose updates arrive on
another.

`Broadcast` sends every record to every replica, which is a broadcast join in
a batch plan and a group announcement in a multi-agent graph. `RoundRobin`
spreads records with no key affinity, which suits stateless work.

### Delivery decides whether the two sides run at once

`Materialized` withholds every record until the producer completes, an event
called sealing the channel, and the controller does not create the consumer's
pods before then. `Pipelined` delivers records as they are produced.

That single choice separates a batch engine from a stream processor. In
Spark's batch execution every shuffle dependency is a stage boundary, and the
consuming stage cannot begin before the producing stage finishes. Here the
choice is per edge. Setting every channel to `Materialized` reproduces the
Spark arrangement, and setting some to `Pipelined` produces arrangements a
Spark physical plan has no way to write. One operation can take one input as a
sealed shuffle and another as a live stream, because the two edges disagree
and delivery belongs to the edge.

### Durability decides who can read a record afterwards

`Ephemeral` discards a record once every consumer has acknowledged it.
`Retained` keeps it, which makes a channel with no consuming operation into a
readable log. A channel with no consuming operation is `Retained`
automatically.

### Feedback decides how a cycle is paced, and is the constrained attribute

A cycle must be closed by a channel carrying the feedback attribute, and the
controller rejects a Workload whose graph still has a cycle once the feedback
channels are removed. The attribute has two modes and they are not
interchangeable.

`Asynchronous` gives the loop no barrier. Each record carries the number of
the iteration it belongs to, called its epoch, and the engine drops or diverts
a record whose epoch reaches the loop bound. Each key advances on its own
schedule.

`Synchronous` runs the loop as bulk-synchronous supersteps. The consuming
operation runs a per-epoch callback when the channel holds no records pending
and none in flight at the current epoch, a state the engine calls quiescent.
It then reports the epoch finished, and the coordinator releases the next
epoch once every consumer replica has reported.

**A `Synchronous` feedback channel is well defined only when its producer and
its consumer are the same operation.** `channel.quiet()`
(`pkg/coordinator/coordinator.go:740`) computes quiescence as no records
pending and none in flight, and nothing else. A channel whose producer has not
sent anything yet satisfies that condition too, and the barrier cannot
distinguish the two cases.

Within one operation the ambiguity cannot arise. The pod fills the channel
inside its per-epoch callback and only then reports the epoch finished, so the
epoch's records exist before the barrier is asked to advance. PageRank in
[examples/pagerank](../examples/pagerank) has this shape, and so does the
`Synchronous` loop exercised in `pkg/sdk`, whose feedback channel runs from
the `rank` operation back to itself.

Across two operations nothing holds the barrier, and three failures follow.
The consumer runs every epoch to the loop bound while the producer is still
starting. A record that arrives after its epoch was reported finished wedges
the barrier for good, because a pod reports each epoch once, guarded by
`lastDone` (`pkg/sdk/sdk.go:688`, `705`). A producer that falls two epochs
behind is rejected outright. `Announce`, the call by which a producer registers
a batch of records with the coordinator, returns `segment epoch %d is behind
channel epoch %d` (`pkg/coordinator/coordinator.go:458`).

The rule that follows tells a reader which iterative algorithms are cheap
here. Pregel-style bulk-synchronous iteration inside one operation is directly
expressible. Anything that iterates across two operations has to carry the
barrier in application state. The consuming operation counts what it expects
and acts when the count is complete, over an `Asynchronous` channel that
imposes no barrier of its own. The one agent example in the repository
already obeys this: both tool-return edges in
`examples/agent-loop/workload.yaml:61` and `:66` are cross-operation and both
are `Asynchronous`.

## When an edge is worth declaring

An edge costs a channel hop: a segment written to disk, an announcement, a
fetch, and an acknowledgement. It buys four things, and an edge is worth
declaring when the two sides need at least one of them.

- **Independent scaling.** The two sides have different throughput and should
  have different replica counts.
- **Different resource shapes.** One side needs memory or an accelerator that
  the other does not.
- **Separate credentials.** One side holds a secret the other should not
  reach. The controller derives one NetworkPolicy per edge, so an operation
  can reach only the operations that produce for it.
- **A queue.** The two sides should be decoupled by a buffer that absorbs
  bursts and drives autoscaling from its depth.

Everything else belongs inside one operation's record handler. This is the
answer to whether a given pipeline is one operation or five, and it decides
the agent-framework mappings below more than any topology question does.

## The transport already passes data by reference

Nothing in the API says so, which is why the point is easy to miss.

A producer does not send its records anywhere. `segmentStore.write` puts a
batch of records into a file on the producing pod's own disk, and
`segmentStore.serve` hands that file out over the pod's own HTTP server. What
reaches the coordinator is an announcement carrying an identifier, a channel
name, a partition, an epoch, a record count, a byte count and the holder's
address. The coordinator holds the location and never the bytes. A consumer
asks which segments are pending for the partitions it owns and fetches them
from the holding pods. Distributing data by locator with the bytes moving peer
to peer is the mechanism an object store needs, and it already runs underneath
every channel.

That mechanism is not reachable from a program. `Emit` JSON-encodes each value
into the segment, and a consumer decodes an entire segment before handling the
first record, so a large value is encoded, written, read and decoded in full
and buffered whole on both sides. A program that wants to move a large array
between two operations therefore pays for a shuffle of that array, which is
the single largest cost of translating a Ray program.

Closing that gap needs no new infrastructure, and the size of the change is
the argument for making it. A producer-side call can stream a payload into a
file of its own beside the segments and emit an ordinary record whose value is
a small handle holding the identifier, the holder's address and the size. A
consumer-side call opens a stream back from the holder. The worker's existing
segment server gains one route. The record carrying the handle is partitioned,
stamped with an epoch, buffered, flushed, delivered and acknowledged like any
other record, so the whole existing protocol applies to it unchanged.

The lifetime question answers itself from the same machinery. A payload can
live exactly as long as the segment carrying the record that references it.
The coordinator already reports a segment released once every consumer
delivered that segment has acknowledged it, and for a `Broadcast` channel that
means every consumer replica. Binding a payload to that event gives it a
lifetime with no reference counting across pods and no new coordinator state.

The result would stay narrower than Ray's object store, and the difference is
worth stating. A payload lives on the one pod that produced it and dies with
that pod. Nothing places a consumer near the pod holding the payloads it will
read. The bytes are invisible to the coordinator, so a channel carrying them
looks small in the backlog metrics that drive scaling. This exposure is
proposed and unmerged; the transport underneath it is present today.

## Stream processors

Apache Flink runs a fixed dataflow graph over unbounded input, with keyed
state, event-time semantics, and periodic checkpoints. This is the family that
fits worst, and saying so is more useful than stretching to claim coverage.

**What maps.** The dataflow graph maps directly, one operator to one operation
and one edge to one channel. A keyed stream is a `Hash` channel and key
affinity holds for the life of the owning replica. Completion `Never` gives
the operator a Deployment that runs until the Workload is deleted. Autoscaling
is driven by the backlog on an operation's inbound channels, which is a signal
derived from the topology and needs no application metrics. `Pipelined`
delivery is the whole execution mode.

**What maps awkwardly.** An unbounded source has to loop inside its `Source`
handler, because the worker library calls that handler once and then idles.
Restarting such a pod restarts the loop from the beginning, so the source has
to recover its own position.

**What does not map, which is most of Flink.** There is no event time and no
watermark, so a record carries no timestamp the engine understands and nothing
tells an operator that a period is complete. There is no windowing. There is
no managed keyed state: an operation that accumulates state holds it in the
pod's memory, and a replica that stops polling for twenty seconds is expired
and its state is gone. There is no checkpoint and no savepoint, so a restarted
operator starts empty. Delivery is at-least-once, and exactly-once is out of
scope by declaration. None of `watermark`, `event time`, `window`,
`checkpointing`, `state store` or `savepoint` appears in any non-test Go file
in the repository.

The distance from Flink is one of semantics rather than of transport. Records
move between independently scaled pools correctly. Everything Flink layers on
top of a transport is absent, and none of the four attributes has a value that
would supply it. The missing pieces are a per-record timestamp with a progress
frontier, and an operation-level state store with checkpointing.

## Log brokers

Apache Kafka is an append-only log, partitioned by key, that many independent
consumers read at their own positions.

**The topic-shaped API is real.** One path serves both directions
(`pkg/coordinator/api.go:62`): an external producer appends records with
`POST /channels/<name>/records`, and an external consumer reads with `GET
/channels/<name>/records?key=&after=&wait=`. That is an offset, a key filter
and a long poll. The response carries the offset to resume from in a header
(`pkg/coordinator/api.go:36`). Partitioning by key is `Hash` with a partition
count, and `Retained` durability keeps the history.

**The constraint that decides the mapping is that a channel cannot be read
from outside while an operation consumes it.** `Records` rejects any channel
that has a consuming operation
(`pkg/coordinator/coordinator.go:992`), with the message `channel %q is
consumed by operation %q; its records live on worker pods`. An edge is either
an internal edge or an external topic, and never both.

The reason is the pass-by-reference transport described above, appearing as a
limitation instead of a strength. Records on a consumed channel live on the
producing worker pods and the coordinator holds only locators, so there is
nothing central to serve. Two consequences follow for anyone building on this.
A channel that should be both watched from outside and consumed inside the
graph requires the producer to emit twice, once to each. Adding a sink
operation to an existing externally-readable channel silently removes the
external read.

So the missing primitive is a fan-out point where a channel can have more than
one reader. Consumer groups with independent offsets, compaction and a
retention policy are all things worth building once a channel can be read
twice, and none of them is reachable before that.

**Retained channels grow without bound.** A segment's bytes are freed only
when durability is not `Retained` (`pkg/coordinator/coordinator.go:853`).
There is no eviction, no size bound and no time bound anywhere, so a
`Retained` channel grows for the lifetime of the coordinator process, in
memory, in one pod. [The cost of a record](#the-cost-of-a-record) measures
what that costs.

Kafka is nevertheless closer than the list of absences suggests. Partitioned
keyed transport with retained history, external producers, and external
consumers reading from an offset is most of a topic.

## Agent frameworks

Agent systems divide into two shapes that map very differently, and the
contrast teaches the general rule better than either case alone.

### LangGraph-shaped systems map closely

A graph framework whose vertices are program steps and whose edges say which
step runs after which is the closest fit of any family here.
[agent-graphs.md](agent-graphs.md) walks the concepts one by one and
[examples/agent-loop](../examples/agent-loop) realises them, so the mapping is
not repeated. Two points belong here rather than there.

The first is that the tool-call loop must be `Asynchronous`, and the reason is
the feedback constraint above rather than anything about agents. A tool
returning to an agent is a cycle across two operations, so the engine's
`Synchronous` barrier cannot pace it. Both tool-return edges in the example
are `Asynchronous` with a loop bound and an overflow channel.

The second is that a per-conversation recursion limit and a per-conversation
overflow destination come from the loop bound and the overflow channel, so a
stalled conversation stops with its full state on a readable channel. That
behaviour is declared on the edge and costs no application code.

### LangChain-shaped systems map to one operation

An LCEL chain such as `prompt | model | parser` is function composition inside
one process. Each arrow is a data dependency with no independent scaling, no
distinct resource shape, no separate credential, and no need for a queue.
Under the rule in [When an edge is worth
declaring](#when-an-edge-is-worth-declaring), none of those arrows earns a
channel. The chain maps to a single operation whose record handler runs the
whole chain, and making each stage its own operation would buy nothing and
cost a channel hop per arrow.

That is the honest answer, and it generalises. LangGraph edges usually clear
the bar because its nodes are agents and tools with different costs and
different credentials. LangChain edges usually do not.

### Multi-agent shapes

The frameworks differ in branding more than in structure, so the shapes are
what matters.

- **Handoff and routing.** One agent transfers control to another chosen at
  runtime. This maps as a conditional edge: the producing operation picks
  which of its outbound channels to emit to, and the set of possible targets
  is the set of declared channels. It maps when the set of agents is fixed at
  submission and does not map when agents are created at runtime.
- **Group chat with a manager.** A manager selects the next speaker. The
  manager is an operation with one outbound channel per participant, or one
  `Hash` channel keyed by participant when the participants share an image. An
  announcement to every participant is a `Broadcast` channel. The replies
  return on one channel into the manager, and because that closes a cycle
  across two operations it must be `Asynchronous`.
- **Role and task pipelines.** Sequential or hierarchical task delegation is
  an ordinary chain of operations, with delegation expressed as a conditional
  edge.
- **Guardrails and critics.** A judging operation with a revision loop is a
  cycle on an `Asynchronous` feedback channel whose loop bound is the maximum
  number of revisions, and whose overflow channel receives a draft that never
  passed. The model expresses this shape with no application machinery, and it
  is worth reaching for.

### The topology maps and the source code does not

This is the gap that matters, and it is about the programming model.

The worker is serial. `Run` handles a segment's records with a plain loop,
`for _, r := range recs` calling `h.OnRecord` in turn, with no goroutine per
record (`pkg/sdk/sdk.go:636` to `648`). A handler that blocked waiting for
another operation's reply would stop its pod from doing anything else, and if
that reply is co-partitioned back to the same pod it would deadlock outright.

Every current agent SDK is written in blocking style, with a handler awaiting
a tool call and continuing on the same stack. Translating one means rewriting
it in continuation-passing form: emit the request, return, and handle the
reply as an ordinary record carrying enough state to resume. A reader
evaluating this model should know that the porting cost is a rewrite of the
control flow rather than a configuration change.

## What it would take to run an agent SDK unchanged

Five things, in dependency order. The first two are the credible short path.

**1. A worker library in Python and TypeScript.** The repository has fifteen
non-test Go files and exactly one worker library, `pkg/sdk/sdk.go`. Agent
frameworks are written in Python and TypeScript, so a node of one cannot be an
operation today without reimplementing the protocol. The protocol is small and
entirely HTTP: register, consume, fetch segments, emit, flush, acknowledge,
report done. This is a port rather than a design problem, and nothing else on
this list matters without it.

**2. Concurrent record handling with per-key ordering.** An agent handler
spends nearly all its time waiting on a model call, so serial handling means
one call in flight per pod and concurrency bought by adding pods. Three things
in `pkg/sdk/sdk.go` block it. The worker mutates a shared field per record
(`w.epoch = r.Epoch`, line 641). The emit buffer is keyed on the worker and
shared across records. `Flush` and the acknowledgement run after the whole
segment (lines 649 to 655), so the segment is the unit of acknowledgement. The
fix has four parts: a concurrency setting on the operation, an epoch and an
emit buffer per invocation, and a segment acknowledged when its last record
completes. Equal keys stay serial, so a conversation keeps its order.

**3. Blocking call-and-return, which is worth declining.** A call that emits a
request and returns the reply, over a correlation table, would give source
compatibility with every SDK written in blocking style. It requires item 2
first, because with serial handling a reply co-partitioned home deadlocks.

The argument against it is the more interesting half. Today the record is the
state. A pod that dies mid-conversation loses nothing, because the record is
still on a channel and another replica picks it up. A blocked handler holds
the conversation on a goroutine stack, which dies with the pod. Adding the
call buys source compatibility and pays with durability, and that trade is bad
for the workload this model is for. A continuation helper in the worker
library, making the rewrite mechanical, gets most of the benefit and keeps the
property.

**4. Token streaming.** A record is a discrete value with a key, an opaque
JSON payload and an epoch (`pkg/coordinator/api.go:67`). Token-level partial
output has no representation, and this one is absent rather than awkward.

**5. Shared state across conversations.** Memory stores and vector stores
assume a store every node can reach. State travels in the record here and
there is no cross-key store in the worker library. Keeping such a store
external is a defensible choice rather than an oversight, and it should be
made explicitly.

**What is already there, which is the part a reader will get wrong.** Dynamic
agent *instances* work today. One operation hosts unboundedly many logical
agents selected by record key, the same way one task type in a task runtime
hosts unboundedly many invocations. Only dynamic agent *types* are impossible,
because the set of operations is fixed when the Workload is submitted. That is
the same boundary that decides which task-runtime programs map.

Items 1 and 2 alone make LangGraph-shaped and role-pipeline-shaped systems
practical and turn the porting cost into mechanical work.

## Batch engines and task runtimes

Both of these are covered at length elsewhere or map by rules already given,
so this section states the mapping and the gap and stops.

**Batch shuffle engines.** A stage becomes an operation with completion
`Drain`, a shuffle becomes a `Hash` channel with `Materialized` delivery, a
broadcast join becomes a `Broadcast` channel, and an action becomes a channel
with no consumer. [spark-mapping.md](spark-mapping.md) works this through,
including the case for a planner that appends stages as earlier ones seal.
Adaptive replanning is the gap: an operation that has been created cannot be
removed or replaced, and a lost stage is not recomputed from its inputs.

**Task and actor runtimes.** A task type becomes an operation and an
invocation becomes a record, so the number of invocations and their timing are
runtime data. An actor becomes the replica that owns a hash partition, and the
actor handle becomes the key that hashes to it; a partition count of one gives
an actor a single owner for the whole channel. A recursive task becomes a
feedback channel with a loop bound.

A future maps outside the graph and not inside it. Outside, the record key is
the future's identity and the records API with a wait duration is the blocking
get, with the same three outcomes of a value, a seal, or a timeout. Inside, the
serial worker rules it out for the reason given under agent frameworks. Three
further things do not map: a program whose graph depends on its own results,
lineage recovery, and an actor that outlives its pod. Ownership of a partition
moves when a pod expires, and the replacement holds none of the state.

## The cost of a record

A single-process harness ran one producer pod and one consumer pod against a
coordinator on a loopback HTTP server, in the arrangement the tests in
`pkg/sdk` use. It moved eight mebibytes through one channel with a single
round-robin partition and measured the time from the producer starting to the
consumer draining, over four splits of the same total payload.

| records | bytes per record | elapsed |
|---|---|---|
| 83,886 | 100 | 1.00 s |
| 8,388 | 1,000 | 0.41 s |
| 838 | 10,000 | 0.17 s |
| 83 | 100,000 | 0.17 s |

Four runs of the whole table agreed within six percent on every row. Time
falls as the record count falls while the byte count stays fixed. The last two
rows match because below roughly a thousand records the consumer's polling
interval sets the floor.

The reason is structural. The worker library buffers per channel, partition
and epoch, and a flush writes a segment, announces it with one request, and
later acknowledges it with another. Control-plane traffic scales with records
and with segments. A program that emits one record per fact spends most of its
time in round trips, so the guidance is to put a whole unit of work in one
record.

The same harness measured what a retained record costs the coordinator. Five
hundred thousand records went to a channel with no consuming operation, each
carrying a short string key and one numeric value. The coordinator's heap grew
by 27.3 mebibytes, about 57 bytes a record, and four runs reported the same
figure. All five hundred thousand remained readable afterwards, which is the
intended behaviour and also the problem.

## What the model does not have

Collected here so the absences carry the same weight as the mappings. Several
also appear above, under the family they affect most.

**Recovery.** Delivery is at-least-once. A consumer replica that stops polling
for twenty seconds is expired and its unacknowledged records return to the
queue, so an operation that accumulates state is correct only while its pods
survive. Segments live on the pod that produced them, and when that pod
expires before a consumer acknowledged them the segments are marked lost and
counted. The producing task is not re-executed. There is no operation-state
checkpointing and no lineage re-execution.

**Time.** No event time, no watermark, no windowing. The epoch on a record
counts loop iterations and has no relation to wall-clock or event-clock time.

**A second reader on a channel.** An edge is either internal or externally
readable. Consumer groups, compaction and retention all sit behind that.

**Elastic hash partitions.** Hash partitions are assigned to consumer replicas
on first contact and stay there, so adding replicas after all partitions are
owned has no effect. The partition count should be at least the maximum
replica count.

**Graph mutation.** The channel list is pushed to the coordinator on every
reconcile and new operations are created when they appear, so a running
Workload can grow. Removing or replacing an operation or a channel is not
handled.

**Coordinator durability.** The coordinator's index is in memory on one pod,
so restarting it loses the segment index and the Workload has to be
resubmitted.

**Exactly-once processing.** Out of scope by declaration.

## Work proposed and not merged

Six pull requests are open against this repository and would change what this
document says. None has merged, and nothing above depends on any of them
except where marked.

| pull request | what it would add |
|---|---|
| #1 `feat/channel-combine` | a `combine` attribute on a channel, folding records that share a key before they go on the wire |
| #2 `feat/dependency-free-workers` | the dataflow vocabulary split into a package that imports nothing, so a worker links no Kubernetes client |
| #3 `fix/engine-correctness` | four engine fixes, including a barrier poll interval and a bounded retry on a failed segment fetch |
| #4 `feat/blob-payloads` | the pass-by-reference payload calls described above, exposing the transport's existing mechanism to programs |
| #5 `feat/paramserver-example` | a parameter-server example, a Ray mapping document, and a test pinning the cross-operation `Synchronous` behaviour described above |
| #7 `ray-fanout-example` | a fan-out and gather example with a `Broadcast` side input, and an account of what a shared object store costs to translate |

## Not verified

- No Kubernetes cluster was available while this document was written. Every
  claim about cluster behaviour, including the stage barrier delaying pod
  creation, the per-edge NetworkPolicy and scaling a completed operation to
  zero, comes from the reconciler source and its unit tests rather than from
  an observed cluster run.
- The timing and memory figures come from a single process with an in-process
  coordinator over loopback HTTP. They show how cost scales with record count
  on this engine. They do not predict throughput on a cluster, where the
  network and the pod count dominate.
- The measurement harness that produced those figures is not in the
  repository. The procedure above describes it in enough detail to rebuild.
- The three cross-operation `Synchronous` failures are stated from the code
  paths cited. The test that runs one of them arrives with pull request #5.
- The characterisations of Spark, Flink, Kafka, Ray, LangChain, LangGraph and
  the multi-agent frameworks come from their documented semantics. No
  cross-system benchmark was run, and nothing here is a performance comparison
  against any of them.
