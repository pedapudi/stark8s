# Mapping a Workload onto Kubernetes resources

This document specifies what the controller creates for a Workload, how the
created pods find each other and exchange records, how scaling decisions
are carried out, and how network access is restricted. The reconciler in
`pkg/controller/reconciler.go` implements it; the protocol between the
controller, the coordinator, and worker pods is defined in
`pkg/coordinator/api.go`.

## Execution model

Records pass directly between operation pods. A producer pod writes records
into local segments, serves them over HTTP on the segment port (8090), and
announces each segment to the workload's coordinator. A consumer pod asks
the coordinator which partitions it owns and which segments are pending for
them, fetches those segments from the pods that hold them, processes them,
and acknowledges them. The coordinator holds metadata only: the topology,
the registry of live pods, partition ownership per consuming operation, the
segment index, seals, and loop epochs. Channels with no producer or no
consumer are the exception: their records are stored on the coordinator,
which serves them on the same segment API.

## Resources created per workload

| element of the Workload | Kubernetes resource | name |
|---|---|---|
| the workload as a whole | one Deployment (one replica, Recreate strategy) running the coordinator, plus a Service exposing the control port (8080) and the segment port (8090) | `<workload>-coordinator` |
| each operation | Deployment | `<workload>-<operation>` |
| each operation | ServiceAccount, set as the pod's `serviceAccountName` | `<workload>-<operation>` |
| `scaling.horizontal.cpuUtilizationPercent` on a `Never` operation | HorizontalPodAutoscaler (autoscaling/v2) targeting the Deployment | `<workload>-<operation>` |
| `scaling.vertical` on a `Never` operation | VerticalPodAutoscaler, only when the `autoscaling.k8s.io` API is installed | `<workload>-<operation>` |
| each channel with both a producer and a consumer | NetworkPolicy | `<workload>-edge-<channel>` |
| operation pods as a group | NetworkPolicy | `<workload>-operations` |
| the coordinator | NetworkPolicy | `<workload>-coordinator` |

Every resource carries an owner reference to the Workload, so deleting the
Workload garbage-collects everything. Pods are labelled
`stark8s.io/workload=<workload>`, `stark8s.io/operation=<operation>`, and
`stark8s.io/role=operation` or `stark8s.io/role=coordinator`.

The coordinator image is `spec.coordinator.image` when set and otherwise the
image named by the controller's `--coordinator-image` flag. The container
runs `/coordinator` and is ready when `GET /healthz` succeeds.

## Why every operation is a Deployment

Completion of a batch operation is a fact about the graph: every inbound
channel is sealed and drained, and nothing is in flight. The coordinator
observes these facts and reports them per operation as
`OperationMetrics.Complete`. The controller expresses completion by scaling
the operation's Deployment to zero replicas. A Deployment therefore serves
both completion rules: a `Drain` operation runs until the coordinator
reports it complete and then has zero replicas; a `Never` operation keeps
its replicas until the Workload is deleted.

A completed operation's pods may still hold Ephemeral segments that a
downstream consumer has not fetched. The coordinator reports this as
`OperationMetrics.HoldsUnconsumed`, and while it is true the controller
keeps the Deployment at its current replica count. On a partitioned channel
one acknowledgement settles a segment, because it went to one replica. On a
Broadcast channel the segment is settled only when acknowledgements from as
many replicas as the controller published have arrived, so a producer feeding
a Broadcast channel keeps a pod until its consumer has finished reading. A
consumer whose completion is `Never` never finishes, so such a producer is
held for the life of the workload; a replica added to it later still needs
the records. When the segments are
acknowledged the flag clears and the Deployment is scaled to zero. This
hold-until-consumed rule is what makes producer-local segment storage safe
under a Deployment, whose pods would otherwise be removed on completion.

Worker pods of a completed operation are expected to keep running and
serving segments until they are scaled away; a `Drain` operation's pod
exits only when the controller removes it.

## Operation start gating

Before creating an operation's Deployment, the controller checks every
inbound channel that is Materialized and has no feedback attribute. If any
is unsealed, the operation is reported as `Waiting` and nothing is created.
This is the stage barrier of a shuffle-based engine expressed as scheduling:
the consumer of a shuffle occupies no resources until the shuffle is
complete.

Feedback channels are excluded from the gate because they are sealed only
when the loop terminates, which requires the operation to have run.

## Sealing

A channel is sealed when its producer will produce no more records. When
the coordinator reports a `Drain` operation complete, the controller seals
every outbound channel of that operation that has no feedback attribute
and is not already sealed (`POST /channels/<name>/seal`), and only then
adjusts the replica count. Feedback channels seal themselves when the epoch
bound is reached. Channels with no producer are sealed by an external
`POST /channels/<name>/seal` to the coordinator.

## Discovery: how a pod learns its place in the graph

The controller injects the following into every pod of an operation.

Environment variables, prepended to every container's `env`:

| variable | value |
|---|---|
| `STARK8S_COORDINATOR` | `http://<workload>-coordinator:8080`, resolvable through the Service |
| `STARK8S_WORKLOAD` | workload name |
| `STARK8S_OPERATION` | operation name; sent as the `X-Stark8s-Operation` header on every coordinator request |
| `STARK8S_INSTANCE` | the pod name, from the downward API (`metadata.name`); the pod identity for registration and partition assignment |
| `STARK8S_POD_IP` | the pod IP, from the downward API (`status.podIP`); the address the pod announces for its segment server |
| `STARK8S_SLOTS` | `spec.operations[].slots`, the number of partitions one pod processes concurrently (default 1) |
| `STARK8S_INBOUND` | comma-separated inbound channel names |
| `STARK8S_OUTBOUND` | comma-separated outbound channel names |
| `STARK8S_FEEDBACK` | inbound channels that have the feedback attribute |
| `STARK8S_FEEDBACK_OUT` | outbound channels that have the feedback attribute |
| `STARK8S_SEGMENT_DIR` | `/var/lib/stark8s/segments`, the local segment store |

Pod spec additions:

- an `emptyDir` volume named `stark8s-segments`, mounted at
  `/var/lib/stark8s/segments` in every container;
- a `containerPort` of 8090 named `segments` on the first container;
- `serviceAccountName` set to `<workload>-<operation>`;
- the three `stark8s.io/*` labels.

The worker library reads the environment and needs no further
configuration. A container that does not use the library can call the
coordinator HTTP API directly with the same information.

On every reconcile pass the controller sends the complete channel list to
the coordinator (`PUT /topology`). Existing channels keep their state; new
channels are created. This is what makes the graph editable while running.

The same pass sends the replica count it is scaling each operation to
(`PUT /operations`). The coordinator needs it for Broadcast channels: every
replica of the consumer receives every record, and the coordinator sees only
the pods that have registered, so it cannot otherwise tell a segment every
replica has read from one that only the replicas started so far have read. An
operation still gated behind a Materialized channel has no Deployment and is
sent as zero, which holds its producers'"'"' segments rather than freeing them.

## Scaling

**Horizontal, from runnable tasks.** On each pass (every three seconds while
the workload runs) the controller reads `GET /metrics` from the coordinator.
For each operation it computes

    replicas = clamp(ceil(RunnableTasks / slots), min, max)

where `RunnableTasks` is the coordinator's count of the operation's
partitions with pending input, `slots` is `spec.operations[].slots`, and
`min` and `max` are `scaling.horizontal.min` and `scaling.horizontal.max`.
When the coordinator has not yet reported the operation or reports zero
runnable tasks, the count is `min`, raised to one for operations that must
be present to make progress on their own: sources (no inbound channels) and
consumers of at least one Pipelined channel. A consumer whose inbound
channels are all Materialized may sit at zero replicas until work is
pending.

The same formula chooses the initial replica count when a gated operation
is first created, so a stage consuming a sealed shuffle starts with
parallelism proportional to the number of partitions that received records.

**Horizontal, from CPU.** For `Never` operations with
`cpuUtilizationPercent` set, the controller creates a
HorizontalPodAutoscaler bounded by `min` (at least one) and `max` and leaves
the replica count to it after the Deployment exists.

**Vertical.** When `scaling.vertical.mode` is `Initial` or `Auto`, the
operation's completion is `Never`, and the VerticalPodAutoscaler API is
present, the controller creates a VerticalPodAutoscaler with that update
mode targeting the operation's Deployment. `Drain` operations receive none,
because the VPA updater evicts pods and the segments held by an evicted pod
would be lost.

**Partition count as an upper bound.** A hash-partitioned channel with
`partitions: N` can usefully feed at most `ceil(N / slots)` consumer
replicas, and partitions are bound to their first owner. `max` for a hash
consumer should be at most that value.

## Networking

NetworkPolicy restricts which pods can open connections. Three kinds of
policy are created; together they permit exactly the connections the
execution model requires.

**`<workload>-edge-<channel>`**, one per channel that has both a producer
and a consumer. It selects the producer operation's pods and permits
ingress on the segment port (8090, TCP) from the consumer operation's pods
of the same workload. A feedback channel from an operation to itself yields
a policy permitting that operation's pods to reach one another. When a
channel is removed from the spec, the controller deletes its policy; edge
policies are found through the `stark8s.io/channel` label.

**`<workload>-operations`** selects every operation pod of the workload,
declares both policy types, and permits egress to the coordinator pods on
ports 8080 and 8090, egress to any operation pod of the same workload on
port 8090, and egress to DNS (port 53, UDP and TCP). It grants no ingress,
so ingress to an operation pod is permitted only by an edge policy.

**`<workload>-coordinator`** selects the coordinator pod and permits
ingress on ports 8080 and 8090 from the workload's operation pods and from
the controller's namespace (which pushes topology, reads metrics, and seals
channels), and egress to DNS only. Reads through the API server proxy, as
in `hack/results.sh`, arrive from the API server and are subject to the
cluster's own policy for that path.

NetworkPolicy is enforced by the cluster's network plugin. Kind's default
plugin does not enforce it; `hack/local-up.sh` installs Calico when
`STARK8S_CNI=calico` is set.

The coordinator additionally checks the `X-Stark8s-Operation` header on
every request and accepts produce and consume requests only from the
channel's declared producer and consumer. The per-operation ServiceAccount
exists so that the coordinator can bind the header to the pod's projected
token; the controller creates the account and sets it on the pod, and
verification of the token is the coordinator's responsibility.

## Status

The controller writes:

- `status.phase`: `Pending` until the coordinator accepts the topology,
  then `Running`; `Succeeded` when every `Drain` operation is `Succeeded`
  and the workload has no `Never` operation; `Failed` when the graph is
  invalid. Once `Succeeded` or `Failed`, the workload is no longer
  reconciled and the coordinator remains available for reading result
  channels;
- `status.operations[]`: phase (`Waiting`, `Running`, `Succeeded`), desired
  replicas, ready pods, `runnableTasks`, and `holdsUnconsumed`, the last
  two copied from the coordinator's operation metrics. An operation is
  `Succeeded` when the coordinator reports it complete and its Deployment
  has zero desired and zero observed replicas;
- `status.channels[]`: sealed flag, pending and in-flight record counts,
  total produced, the current epoch for Synchronous feedback channels,
  `overflowed` (records diverted or dropped at the loop bound), and `lost`
  (segments whose holder pod expired before consumption).

`kubectl get workloads` shows the phase; `kubectl get workload <name> -o
yaml` shows the rest.

## Validation

The controller rejects a Workload, setting `Failed`, when:

- an operation or channel name is duplicated;
- a channel names an undeclared producer or consumer;
- a feedback channel has no consumer;
- `feedback.overflow` names an undeclared channel or the feedback channel
  itself;
- `slots` is negative (zero is treated as one);
- the graph with feedback channels removed contains a cycle. A feedback
  channel of either mode, Synchronous or Asynchronous, closes a cycle.

## RBAC

The controller needs read and status-write access to Workloads, and full
access to Deployments, Services, ServiceAccounts, NetworkPolicies,
HorizontalPodAutoscalers, and VerticalPodAutoscalers. The ClusterRole in
`config/manager/manager.yaml` grants exactly that.
