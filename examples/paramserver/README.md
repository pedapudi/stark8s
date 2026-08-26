# paramserver

The canonical parameter-server program as a Workload: one stateful server
holding the weights, a pool of workers computing gradients against their own
data shard, and no driver loop anywhere.

```
data ──shards──> worker ──gradients──> server ──checkpoints──> (outside)
                    ^                     │
                    └──────weights────────┘   Broadcast, feedback
```

- **server** — exactly one replica. It holds the weight vector, applies the
  gradients of a round once every shard has reported, broadcasts the new
  vector, and writes each round to `checkpoints`.
- **worker** — owns whichever shards hash to it. On a shard it registers with
  the server; on a weight vector it returns that shard's gradient.
- **data** — a source that emits the training set as one record per shard.

The problem is a two-parameter least-squares fit on a fixed synthetic dataset,
trained by plain gradient descent at a fixed learning rate. Nothing is random,
so the weights after a given number of rounds are the same on every run.

## Reading it

The three things worth understanding are all consequences of the engine and
not of the maths:

**The round barrier lives in the server, not in the channel.** `weights` is an
`Asynchronous` feedback channel, which carries the round number per record and
imposes no barrier at all. The server supplies the barrier by holding a round
open until it has a gradient from every shard. The engine's own `Synchronous`
barrier cannot do this job, because it treats a channel that is empty because
its producer has not sent anything yet as a finished epoch;
[docs/ray-mapping.md](../../docs/ray-mapping.md#loops-across-two-operations)
says what goes wrong and `main_test.go` pins the behaviour down.

**Round 0 is registration.** A worker that has just received a shard sends an
empty gradient for it, and the server broadcasts the initial weights only once
every shard has reported. That is what makes the graph ordered: no worker can
be asked for a gradient over data it does not hold yet.

**`maxEpochs` is the round count plus one.** The epoch of a weights record is
its round number and rounds start at 1, so the vector the server computes after
the last round is stamped with `maxEpochs`, dropped by the engine, and counted
as overflow. That is what ends the loop. The vector itself is already on
`checkpoints`.

## Running it

```sh
go test ./examples/paramserver/
```

The test runs the whole workload in one process against a real coordinator on
an httptest server: one server pod and three worker pods exchanging real
segments over real HTTP. It asserts that the learned weights match the
closed-form least-squares solution, that every worker saw every round's
weights, that the server received one gradient per shard in every round, and
that the loop stopped at the bound.

In a cluster, `kubectl apply -f examples/paramserver/workload.yaml` against the
`stark8s:dev` image, then `hack/results.sh paramserver checkpoints` for the
trajectory.
