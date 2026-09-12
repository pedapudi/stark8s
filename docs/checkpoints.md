# Operation checkpoints

An operation with `checkpoint: true` commits application state after every
completed input segment. The same manifest records the input segments covered
by that state, the epoch, and every durable output segment produced through
that boundary. The operation must configure the workload object store. The
initial contract accepts only operations whose horizontal minimum and maximum
are at most one. Its checkpoint key uses the operation name and survives
replacement of that single pod.

Go applications provide both callbacks on `sdk.Handlers`:

```go
Snapshot func(context.Context) ([]byte, error)
Restore  func(context.Context, []byte) error
```

`Snapshot` returns a complete state image. `Restore` replaces the application's
initial state before any record callback runs. Frameworks may store model and
optimizer bytes in their own durable objects and return a small document of
framework-owned references from `Snapshot`.

For each boundary, the worker performs these actions in order:

1. Write produced segment bytes to immutable durable objects without announcing
   them to the coordinator.
2. Write the application state to another immutable object.
3. Atomically replace the checkpoint manifest with the state reference, input
   positions, epoch, and cumulative output manifest.
4. Announce the committed output segments and acknowledge the covered input.

A crash before step 3 leaves the previous checkpoint active. The replacement
worker restores that state and reprocesses the unacknowledged input. Output from
the abandoned attempt was never announced. A crash after step 3 restores the
new state, republishes the same output segment identifiers, and acknowledges
redelivered input without applying it again. Coordinator publication is
idempotent for those segment identifiers.

Input channels must remain replayable until their covering checkpoint commits.
The worker enforces this by delaying acknowledgement. Durable storage keeps the
segment bytes available if the producer pod exits. Checkpointing rejects output
to an external channel because that sink lacks an atomic publication token.
Applications may instead publish from a downstream uncheckpointed operation to
an idempotent or transactional sink. Arbitrary external side effects inside a
callback remain the application's responsibility.

The first implementation uses full snapshots and serial segment boundaries.
It does not migrate state between partition owners, create incremental
snapshots, or provide a general keyed database.
