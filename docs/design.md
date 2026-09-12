# Design

A `Workload` is a directed graph in one Kubernetes object. Operations are
logical computations backed by pod groups. Channels carry keyed records and
declare `Hash`, `RoundRobin`, or `Broadcast` partitioning; `Pipelined` or
`Materialized` delivery; and `Ephemeral` or `Retained` storage behavior.

## Execution

Producers batch records into segments and announce them to the workload
coordinator. Consumers obtain assigned segments, fetch their bytes, invoke
application callbacks serially, and acknowledge completion. A failed delivery
can be returned and delivered again. Process incarnations prevent an expired
process from completing work owned by its replacement.

The controller runs active ordinary operations at their declared horizontal
maximum. Membership remains fixed during the attempt because workers can hold
partition state and output. `slots` is a capacity hint, not callback
concurrency. Initial vertical sizing is available for `Never` operations when
the cluster provides the VerticalPodAutoscaler API; automatic vertical updates
and automatic horizontal scaling are disabled.

A `Materialized` consumer waits until production on that channel closes.
`Drain` completes after all input closes and drains, callbacks finish, and
output is published. `Never` keeps an operation active. Required-record loss or
a terminal dependency failure appears in workload status and fails execution.

## Cycles

A feedback channel gives its connected finite component one scalar epoch.
Records carry the epoch through every operation in that component. The next
epoch opens only after production closes and work drains for the current epoch.
The declared bound terminates the cycle. Independent event-time clocks,
watermarks, and windows are outside this contract.

## Storage and recovery

`Ephemeral` segments are released after required acknowledgements. `Retained`
suppresses normal release. A producer with retained internal output stays
running while those local bytes are needed. Retention alone does not make bytes
durable.

An optional workload object store holds immutable segment bytes and fenced
coordinator checkpoints. The coordinator then survives replacement with its
ownership, delivery, epoch, and subscription state. Durable output rejects
pod-local blob handles; an application may emit its own durable object
reference.

A checkpointed ordinary operation has one fixed owner. `Snapshot` and `Restore`
commit application state with covered inputs, callback completion, and durable
internal-output identifiers. A replacement restores the last committed
boundary. External output cannot join that commit, so the sink must supply a
transaction or idempotency boundary. See [Checkpoints](checkpoints.md).

The coordinator also exposes named cursors over retained history. The workload
schema and normal worker clients do not bind graph operations to those cursors.
See [Retained subscriptions](subscriptions.md).

## Application and collective processes

The Go worker and local HTTP runtime expose source, record, epoch, drain, and
tick callbacks. The local runtime waits for application readiness before it
registers or claims work. An uncheckpointed replacement session receives an
unfinished invocation. A checkpointed replacement session ends the attempt; a fresh
worker restores committed state. See [Local runtime](language-runtime.md).

A collective uses one fixed-size indexed Job with stable rank, attempt,
checkpoint reference, and rendezvous address. The controller does not provide
gang placement. Graph-connected retries require operation checkpointing and
object storage because a framework checkpoint alone does not fence graph I/O.

NetworkPolicy limits operation traffic to DNS, the coordinator, segment
holders, collective peers, and declared `Metadata` or `Internet` egress. It is
not channel authentication. Authentication, shared budgets, graph replacement,
and arbitrary side-effect recovery remain separate work.
