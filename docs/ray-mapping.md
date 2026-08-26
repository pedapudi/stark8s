# Ray programs as Workloads

Ray is a distributed object store with a task scheduler on top. A program
creates tasks and actors from ordinary Python, passes large values between them
by reference, and blocks on results with `ray.get`. The graph of what ran is
discovered by running the program rather than declared in advance.

This document says which Ray programs map onto the Workload model, which do
not, and why the boundary falls where it does. The companion documents are
[spark-mapping.md](spark-mapping.md) for batch stages and
[agent-graphs.md](agent-graphs.md) for agent frameworks. The parameter server
below is [examples/paramserver](../examples/paramserver), which runs and is
tested end to end.

## The boundary

A Workload's graph is a specification. A Ray program's graph is a trace. A
trace can be written down in advance only when control flow does not depend on
values.

That single difference decides everything else:

- **Dynamic task *instances* map.** A task type becomes an operation, an
  invocation becomes a record, and recursion becomes a feedback channel. The
  number of invocations, their keys, and their timing are all runtime data.
- **Dynamic task *types* do not map.** A program that decides at runtime to
  call a function nobody declared has no counterpart. The set of operations is
  fixed when the Workload is submitted.

So Ray programs over a fixed vocabulary of task kinds are expressible, and
programs whose shape depends on results are not. Hyperparameter search whose
tree branches on scores, and reinforcement-learning rollouts that spawn
according to what the policy did, are the second kind.

## The parameter server, expressed

The canonical Ray actor program is a parameter server: one stateful actor holds
the weights, several workers compute gradients against their shard, the server
applies them, and every worker picks up the new weights.

```python
@ray.remote
class ParameterServer:
    def apply_gradients(self, *grads): ...
    def get_weights(self): ...

@ray.remote
def worker(ps, shard):
    while True:
        w = ray.get(ps.get_weights.remote())
        ps.apply_gradients.remote(gradient(w, shard))
```

As a Workload:

```yaml
apiVersion: stark8s.io/v1alpha1
kind: Workload
metadata: {name: paramserver}
spec:
  operations:
    - name: data              # emits the training set as one record per shard
      template:
        spec:
          containers: [{name: main, image: stark8s:dev, command: ["/paramserver","-shards=4","data"]}]

    - name: server            # the actor. One replica, because the weights
      slots: 1                # are one piece of state with one owner.
      scaling: {horizontal: {min: 1, max: 1}}
      template:
        spec:
          containers: [{name: main, image: stark8s:dev, command: ["/paramserver","-shards=4","-rate=1.4","server"]}]

    - name: worker            # the task pool. Each replica owns a data shard.
      slots: 1
      scaling: {horizontal: {min: 4, max: 4}}
      template:
        spec:
          containers: [{name: main, image: stark8s:dev, command: ["/paramserver","worker"]}]

  channels:
    # The shards. Hash-partitioned on the shard key, so which replica owns
    # which shard is the coordinator's decision and stays put. Materialized:
    # no worker starts until the whole training set has been written.
    - name: shards
      from: data
      to: worker
      partitioning: {mode: Hash, partitions: 4}
      delivery: Materialized

    # ray.get(ps.get_weights.remote()) — every worker needs the SAME weights,
    # so Broadcast rather than a partitioned channel. Marking it feedback is
    # what makes it a training loop instead of a single pass.
    - name: weights
      from: server
      to: worker
      partitioning: {mode: Broadcast, partitions: 1}
      feedback: {mode: Asynchronous, maxEpochs: 41}

    # ps.apply_gradients.remote(g) — every worker sends to the one server, so
    # the partition count is 1 and every gradient lands on the single owner.
    - name: gradients
      from: worker
      to: server
      partitioning: {mode: Hash, partitions: 1}

    - name: checkpoints       # readable from outside during training
      from: server
      durability: Retained
```

Forty training rounds, no driver program, and no `ray.get` anywhere. The
worker's `OnRecord` receives weights and emits a gradient; the server's
`OnRecord` collects the gradients of a round and, when the last one arrives,
applies them and broadcasts the next weight vector. The epoch of a weights
record is the round number, so `maxEpochs` is the round count plus one: the
vector the server computes after the last round is stamped past the bound, the
engine drops it, and the loop stops. That vector is on `checkpoints`, which is
where the trained model is read from.

Three details of the shape are load-bearing, and each is asserted in
[main_test.go](../examples/paramserver/main_test.go):

- **The round barrier is the server's, not the engine's.** The server counts
  gradients and does nothing until it has one from every shard. That is the
  whole synchronisation; see [Loops across two operations](#loops-across-two-operations)
  for why the engine's own barrier cannot be used here.
- **Round 0 is registration.** A worker that has just received a shard sends
  an empty gradient for it, and the server broadcasts the initial weights only
  once every shard has reported. Nothing else orders the graph, so without it
  a worker could be sent weights before it holds any data.
- **The shard count, not the replica count, is what the server waits for.**
  Hash partitions are assigned to whichever replicas are live when a partition
  is first claimed, so a worker may own two shards or none. Counting shards
  makes the round well defined whatever the assignment turns out to be.

What this buys over the Ray version: the server and the workers are separate
pod pools with separate resource shapes, so the server can hold a large model
on a memory-heavy node while the workers sit on cheaper compute. The generated
NetworkPolicy means a worker can reach the server and nothing else. Neither is
expressible to a Ray cluster, which sees a homogeneous worker pool and schedules
inside it.

What it loses: Ray can run the same program with each worker fetching whatever
weights exist and the server applying gradients as they arrive, so a slow
worker delays only itself. Here a round ends when its last gradient lands, and
`Asynchronous` on the feedback channel does not change that — it removes the
engine's barrier but the server's own wait for a full round remains. Making the
training genuinely asynchronous means changing the server's rule for when to
broadcast, not the channel.

## Concept mapping

| Ray | Workload |
|---|---|
| Task type | Operation |
| Task invocation | Record |
| Actor | Operation whose partition owner holds the state |
| Actor handle | The key that hash-partitions to that owner |
| `ObjectRef` | A record value, or a locator the application dereferences |
| `ray.get` from outside | `GET /channels/<name>/records?key=<k>&wait=<d>` |
| `ray.get` inside a task | Not expressible; see below |
| `ray.put` | Emitting to a Retained channel |
| Recursive task | A feedback channel with `maxEpochs` |
| Placement group | Pod affinity in the operation's template |
| Resource request per task | `resources` in the operation's template |
| Lineage re-execution | Not implemented |

### Futures

A future is a handle to a value that does not exist yet. In a Workload the
handle is the **record key**: emitting a request keyed `job-42` makes `job-42`
the future's identity, and the result appears on a channel under the same key.

For a caller outside the graph this is complete. The coordinator's records API
takes a key filter and a wait duration, blocking until a matching record
appears, the channel seals, or the deadline passes:

```
GET /channels/results/records?key=job-42&wait=30s
```

That is `future.get(timeout)`, with the same three outcomes.

For a caller *inside* the graph it does not work, and the reason is worth
stating. The worker library is serial: one record at a time, one channel at a
time. An `OnRecord` handler that blocked waiting for another operation's reply
would stop its pod from doing anything else, and if the reply is co-partitioned
back to the same pod it would deadlock outright. Composition inside the graph is
therefore continuation-passing: a handler emits a request and returns, and the
reply arrives as an ordinary record carrying enough state to resume. This is the
same discipline [agent-graphs.md](agent-graphs.md) describes for tool calls.

### Object references

Ray's `ObjectRef` names bytes in a distributed store, with reference counting,
locality-aware scheduling, and zero-copy reads within a node. Kubernetes has no
equivalent. Its `ObjectReference` types name API resources in etcd, which is a
different thing entirely; the closest primitives are volumes and
PersistentVolumeClaims, and those give plumbing without semantics — no
reference counting, no locality, no automatic lifetime.

Two options exist today, both application-level:

- **Put a locator in the record.** A channel value is an arbitrary JSON value,
  so it can be a URI. The engine ships the string; the application reads the
  object. Lifetime is the application's problem, because the segment carrying
  the locator is freed on acknowledgement while the object it names is not.
- **Share a volume.** A `ReadWriteMany` claim mounted into both operations lets
  a producer write and a consumer read without the bytes crossing a channel.
  The pod template already supports this.

There is a third option the engine is unusually well placed to offer, and it is
worth recording as a design note rather than a workaround. stark8s already
moves data by reference internally: a producer writes a segment to local disk,
announces it, and serves it over HTTP, while the coordinator holds only the
locator. Consumers fetch pod to pod. The mechanism an object store needs
already exists and is simply not exposed to programs. A channel mode in which
`Emit` registers a blob and passes a handle, with the existing
`HoldsUnconsumed` accounting governing its lifetime, would give Ray-style
pass-by-reference without adding a distributed store.

Until then, a Ray program that passes a large array between tasks becomes a
shuffle of that array, and that is the single largest cost of the translation.
The parameter server keeps the whole weight vector in one record for the same
reason: cost is per record and per segment rather than per byte, and the SDK is
serial, so a vector emitted parameter by parameter would be paid for parameter
by parameter.

## What does not map

- **Programs whose graph depends on results.** Search trees that branch on
  scores, rollouts that spawn according to a policy. The graph must be known
  when the Workload is submitted.
- **Blocking composition inside the graph.** See Futures above.
- **Lineage recovery.** Ray re-executes a lost task from its inputs. Segments
  here die with their producing pod and nothing recomputes them, so an
  operation that accumulates state is correct only while its pods survive.
- **Actors that outlive their pod.** An actor is modelled as a partition owner,
  and partition ownership moves when a pod expires. The replacement holds none
  of the state.

## Loops across two operations

A parameter server is a cycle between two operations, and that is a different
thing from the cycles the other examples run. PageRank's feedback channel goes
from `rank` to `rank`, and the agent loop's goes from a tool back to the agent
without a barrier. This one goes from `server` to `worker` and back, and it is
synchronous. The obvious spelling — `feedback: {mode: Synchronous}` on both
channels — does not work, and the reason is a property of the engine rather
than of the example.

`Synchronous` is a bulk-synchronous barrier. Its consumer runs `OnEpochEnd`
when the channel is **quiescent** at the current epoch, reports that epoch
finished, and the coordinator releases the next one once every consumer pod has
reported. Quiescent means nothing pending and nothing in flight. A channel
whose producer has not sent anything yet is also quiescent, and the barrier
cannot tell the two apart.

When producer and consumer are the same operation that ambiguity cannot arise.
The pod fills the channel inside `OnEpochEnd` and only then reports the epoch
finished, so by construction the epoch's records exist before the barrier is
asked to advance. When the producer is a different operation nothing holds the
barrier at all, and three things follow, all observed:

- **The consumer runs to the bound on its own.** With `server` consuming a
  Synchronous `gradients` channel from `worker`, the server's `OnEpochEnd`
  fires for epoch 0, 1, 2, … as fast as it can post, before any worker has
  computed anything. It broadcasts the same initial weights each time and the
  workers train on stale vectors. `TestSynchronousFeedbackDoesNotWaitFor`
  `AnotherOperation` in the example pins this down: the consumer of such a
  channel runs every epoch to the bound with the producer never started.
- **Late records wedge the barrier.** A pod reports epoch *e* finished exactly
  once, guarded by a `lastDone` watermark. If the producer's records for epoch
  *e* arrive after that, the consumer processes them but never reports the
  epoch again, and the channel stays at *e* for good. Runs of the
  two-feedback shape end with the gradients channel stuck mid-loop, holding
  records it will never deliver, and the server never completing.
- **Or they are rejected outright.** `Announce` fails a segment whose epoch is
  behind the channel's — `segment epoch 1 is behind channel epoch 3` — so a
  producer that falls two epochs behind a free-running barrier gets a 400 that
  the SDK retries thirty times and then aborts on.

So a Synchronous feedback channel is well defined when its producer and its
consumer are the same operation, and races when they are not. The example uses
`Asynchronous`, which carries the epoch per record and imposes no barrier, and
recovers the round barrier in the application: the server holds the round open
until every shard has reported. This is more honest about where the
synchronisation lives, and it is the same technique Ray itself uses — the
`ray.get` over all the gradient futures is the barrier there too, written in
the program rather than provided by the runtime.

What would fix the engine, if a declarative barrier across operations is
wanted: quiescence would have to mean *the producers of this epoch have all
finished producing*, not *this channel is empty right now*. The coordinator
already knows a channel's producing operation and already tracks per-pod
completion, so the barrier could require that every live producer pod has
reported the epoch produced before releasing the consumer's `OnEpochEnd`. That
is a change to `EpochDone` and a new producer-side call, not a change to the
model.
