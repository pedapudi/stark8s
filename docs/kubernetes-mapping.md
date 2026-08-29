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
keeps the Deployment at its current replica count. When the segments are
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

## Sizing the segment volume

An operation's pods keep the records they produce on local disk, so the
segment volume has to be large enough to hold them. The size is **per pod**,
not per operation: every replica gets a volume this large and requests this
much disk, so an operation producing a total across N replicas needs about
total/N, and the cluster is asked for the size times the replica count.

How much has to fit depends on the outbound channels.

On an **Ephemeral** channel a segment is deleted once every consumer has
acknowledged it, so what has to fit is the peak unacknowledged output. On a
Materialized channel that is the replica's whole output, since the consumer
is not started until the channel seals, and the hold-until-consumed rule
above keeps a completed operation's pods, and their segments, in place until
the consumer has read them. While the operation is still running that rule
does not apply: `desiredReplicas` follows the runnable-task count down and
scales replicas away along with the segments they hold.

On a **Retained** channel with a consumer nothing is ever deleted.
`Released` skips retained channels, so the coordinator never tells the
producer it may drop a segment, and the volume has to hold everything the
replica produces for as long as its pod runs. Sizing such a producer for its
peak unacknowledged output will under-provision it by the whole of its
output.

A channel with **no consumer** never reaches this volume at all. The
coordinator forces `To: ""` channels to Retained, and the worker posts their
records to the coordinator rather than writing a segment, so `segments.size`
does nothing for an operation whose only output is a terminal channel — the
records accumulate in the coordinator's memory instead.

Sizing the volume is a scheduling statement, not a durability one. Segments
live and die with the pod: `HoldsUnconsumed` is set only for non-Retained
channels, so an operation whose output is Retained is scaled to zero on
completion like any other and its retained segments go with it.

`spec.operations[].segments.size` declares how much room that needs:

```yaml
- name: map
  slots: 4
  segments:
    size: 50Gi
  template: {spec: {containers: [{name: main, image: stark8s:dev}]}}
```

The controller then sets, on that operation's pods:

- `sizeLimit` on the `stark8s-segments` volume;
- an `ephemeral-storage` **request** on the first container, raised to the
  declared size if the template asks for less or asks for nothing. A larger
  request already in the template is left alone. A template naming only a
  limit counts as already asking for it, since Kubernetes defaults an absent
  request to the container's limit.

The request is what the scheduler reads when it chooses a node, so without
it the pods are placed as though they need no disk, and the node-pressure
eviction ranking — which sorts by usage over request — puts a pod holding
tens of gigabytes against a request of zero near the front of the queue.

No limit is set, deliberately. The volume's `sizeLimit` already caps the
segments, and a pod's ephemeral-storage limit is charged that volume
together with every container's writable layer and log output, so a limit
equal to the declared size would evict the pod before the volume could
reach it. A template that names its own limit keeps it; a pod budget that
does not exceed `segments.size` is rejected rather than silently raised,
since raising it could produce a pod a `LimitRange` then refuses.

A request with no limit is, however, exactly the shape a `LimitRange`
rewrites. One carrying `default` or `max` for `ephemeral-storage` injects a
limit onto a container that has none, and if that limit lands below the
injected request the kubelet's admission refuses every pod — while the
Deployment itself is admitted, so the operation reports running with no pods.
The controller cannot see a `LimitRange`, so on a namespace that has one,
name a limit above `segments.size` in the template.

The request goes on the **first container** only, matching where the segment
port is injected. The scheduler reads the sum, so which container carries it
does not affect placement — but Kubernetes refuses a container whose request
exceeds its own limit, so that container's limit has to be able to carry the
whole size, and validation checks it separately from the pod total.

That pod total follows the kubelet's arithmetic: containers that run at the
same time add up, and a container naming no limit contributes nothing rather
than leaving the budget unbounded, so a single sidecar with a small
`ephemeral-storage` limit caps the whole pod. Init containers marked
restartable are sidecars and add; a plain init container has finished before
the others start, so it counts as a floor rather than a summand.

With `segments` unset the volume is a bare `emptyDir` and nothing requests
ephemeral storage, which leaves the capacity to whatever the cluster's
defaults allow — on a cluster that defaults ephemeral storage, a cap the
workload never chose; on one that does not, no cap but no scheduling
account of it either. A producer that outgrows the space it was given is
evicted, and its unacknowledged segments are counted lost (see
[Status](#status)); the producing task is not re-executed.

A pod template may instead declare its own volume named `stark8s-segments`
— with a different `sizeLimit`, `medium: Memory`, or a PersistentVolumeClaim
— and the controller will mount that rather than creating one. Setting both
that and `segments.size` is rejected, since only one of them would apply.
Note that this route sizes the volume without requesting anything: the
controller only touches `ephemeral-storage` under `segments.size`, so a pod
template declaring its own segment volume is still scheduled as though it
needed no disk, and should carry its own request.

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
- `segments.size` is zero, negative, or below 1Mi (a quantity with no unit
  suffix is bytes, so `size: 50` asks for fifty of them); is set on an
  operation whose pod template already declares a volume named
  `stark8s-segments`; is not exceeded by the `ephemeral-storage` budget the
  template's containers add up to; or exceeds the `ephemeral-storage` limit
  on the container that carries the request;
- the graph with feedback channels removed contains a cycle. A feedback
  channel of either mode, Synchronous or Asynchronous, closes a cycle.

## RBAC

The controller needs read and status-write access to Workloads, and full
access to Deployments, Services, ServiceAccounts, NetworkPolicies,
HorizontalPodAutoscalers, and VerticalPodAutoscalers. The ClusterRole in
`config/manager/manager.yaml` grants exactly that.
