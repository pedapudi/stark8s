# Mapping a Workload onto Kubernetes resources

This document specifies what the controller creates for a Workload, how the
created pods find each other, how scaling decisions are carried out, and
how network access is restricted. The reconciler in
`pkg/controller/reconciler.go` implements it.

## Resources created per workload

| element of the Workload | Kubernetes resource | name |
|---|---|---|
| the workload's channels | one Deployment (one replica) running the exchange, plus a Service | `<workload>-exchange` |
| operation with completion `Drain` | Job with `parallelism` = replica count and no `completions` (work-queue semantics) | `<workload>-<operation>` |
| operation with completion `Never` | Deployment | `<workload>-<operation>` |
| `scaling.horizontal.cpuUtilizationPercent` on a `Never` operation | HorizontalPodAutoscaler (autoscaling/v2) targeting the Deployment | `<workload>-<operation>` |
| `scaling.vertical` | VerticalPodAutoscaler, only when the `autoscaling.k8s.io` API is installed | `<workload>-<operation>` |
| operation pods as a group | NetworkPolicy | `<workload>-operations` |
| the exchange | NetworkPolicy | `<workload>-exchange` |

Every resource carries an owner reference to the Workload, so deleting the
Workload garbage-collects everything. Pods are labelled
`stark8s.io/workload=<workload>`, `stark8s.io/operation=<operation>`, and
`stark8s.io/role=operation` or `exchange`.

## Why a Job for batch operations

A batch operation must end. A Deployment restarts exited pods, so it cannot
represent "done". A Job with `parallelism` set and `completions` unset has
the work-queue semantics Kubernetes documents: once one pod exits
successfully and every other pod has terminated, the Job is complete, and
no new pods are created after the first success. Worker pods exit zero when
every inbound channel is sealed and drained, which is the drain condition,
so Job completion is operation completion with no extra bookkeeping.

The consequence is that a Job's parallelism can be raised while it runs but
is never lowered, and is not raised once any pod has succeeded.

## Operation start gating

Before creating an operation's Job or Deployment, the controller checks
every inbound channel that is Materialized and not feedback. If any is not
yet sealed, the operation is reported as `Waiting` and nothing is created.
This is the stage barrier of a shuffle-based engine expressed as
scheduling: the consumer of a shuffle occupies no resources until the
shuffle is complete.

Feedback channels are excluded from the gate because they are sealed only
when the loop terminates, which requires the operation to have run.

## Sealing

A channel is sealed when its producer will produce no more records. The
controller seals every non-feedback outbound channel of an operation when
that operation's Job reports the `Complete` condition. Feedback channels
seal themselves when the epoch bound is reached. Channels with no producer
are sealed by an external `POST /channels/<name>/seal` to the exchange.

## Discovery: how a pod learns its place in the graph

The controller injects environment variables into every container of an
operation's pod template:

| variable | value |
|---|---|
| `STARK8S_EXCHANGE` | `http://<workload>-exchange:8080`, resolvable through the Service |
| `STARK8S_WORKLOAD` | workload name |
| `STARK8S_OPERATION` | operation name; sent as a header on every exchange request |
| `STARK8S_INSTANCE` | the pod name, from the downward API; the consumer identity for partition assignment |
| `STARK8S_INBOUND` | comma-separated inbound channel names |
| `STARK8S_OUTBOUND` | comma-separated outbound channel names |
| `STARK8S_FEEDBACK` | inbound channels that are feedback edges |
| `STARK8S_FEEDBACK_OUT` | outbound channels that are feedback edges |

The worker library reads these and needs no further configuration. A
container that does not use the library can call the exchange HTTP API
directly with the same information.

On every reconcile pass the controller sends the complete channel list to
the exchange (`PUT /topology`). Existing channels keep their state; new
channels are created. This is what makes the graph editable while running.

## Scaling

**Horizontal, from backlog.** On each pass (every three seconds while
running) the controller reads per-channel metrics from the exchange and, for
each operation with `targetBacklogPerReplica` set, computes
`ceil(pending records on inbound channels / target)`, clamped to
`[min, max]`. For a Job the parallelism is raised to that value when it is
higher than the current one; for a Deployment the replica count is set to
it in either direction. When a batch operation is first started, the same
computation chooses its initial parallelism, so a stage consuming a sealed
shuffle starts with parallelism proportional to the shuffle's size.

**Horizontal, from CPU.** For streaming operations with
`cpuUtilizationPercent` set, the controller creates a
HorizontalPodAutoscaler and leaves the replica count to it. Backlog and CPU
scaling are not combined on one operation; if both are set, CPU wins.

**Vertical.** When `scaling.vertical.mode` is `Initial` or `Auto` and the
VerticalPodAutoscaler API is present, the controller creates a
VerticalPodAutoscaler with that update mode targeting the operation's
Deployment. Job-backed operations do not receive one, because the VPA
updater evicts pods and a batch worker's in-memory state would be lost. The
recommended path for batch operations is `Initial` on a first run and
setting requests in the template afterwards; automating that is future work.

**Partition count as an upper bound.** A hash-partitioned channel with
`partitions: N` can usefully feed at most N consumer replicas, and
partitions are bound to their first owner. `max` for a hash consumer should
be at most the partition count.

## Networking

Two layers restrict communication.

**NetworkPolicy** restricts which pods can open connections. The
`<workload>-operations` policy selects every operation pod of the workload
and permits egress only to the exchange pods on port 8080 and to DNS
(port 53 UDP and TCP); it permits no ingress. The `<workload>-exchange`
policy selects the exchange pod and permits ingress only from the
workload's operation pods and from the controller's namespace (which pushes
topology, reads metrics, and seals channels), and egress only to DNS.

NetworkPolicy is enforced by the cluster's network plugin. Kind's default
plugin does not enforce it; `hack/local-up.sh` installs Calico when
`STARK8S_CNI=calico` is set.

**Exchange authorization** restricts which channels a pod may touch. Every
request carries the `X-Stark8s-Operation` header. The exchange accepts
produce requests on a channel only from the channel's declared producer and
consume requests only from its declared consumer. The header is asserted
by the pod and is spoofable by a pod that is already permitted to reach the
exchange; binding it to the pod's projected service account token is the
intended hardening.

The topology is therefore enforced at the exchange rather than in the
network: with a brokered transport, per-edge network rules have nothing to
select, because operations never connect to each other. A direct transport
(consumer pulls from producer pods) would move enforcement into
NetworkPolicy, one rule per channel; it is the design for a shuffle
service that spills to producer-local storage, and is not implemented.

## Status

The controller writes:

- `status.phase`: `Pending` until the exchange answers, then `Running`,
  `Succeeded`, or `Failed`;
- `status.operations[]`: phase (`Waiting`, `Running`, `Succeeded`,
  `Failed`), desired replicas, and active or ready pods;
- `status.channels[]`: sealed flag, pending and in-flight record counts,
  total produced, and the current epoch for feedback channels.

`kubectl get workloads` shows the phase; `kubectl get workload <name> -o
yaml` shows the rest.

## RBAC

The controller needs read and status-write access to Workloads, and full
access to Deployments, Jobs, Services, NetworkPolicies,
HorizontalPodAutoscalers, and VerticalPodAutoscalers. The ClusterRole in
`config/manager/manager.yaml` grants exactly that.
