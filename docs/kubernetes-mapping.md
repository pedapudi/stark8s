# Kubernetes mapping

The controller maps one `Workload` graph to namespaced Kubernetes resources.
Kubernetes owns placement and pod lifecycle. The coordinator owns channel
metadata, partition ownership, delivery progress, seals, and epochs.

## Resources

| Workload element | Kubernetes resources |
|---|---|
| Workload | One coordinator Deployment and Service on control port 8080 and segment port 8090 |
| Ordinary operation | One Deployment and ServiceAccount |
| Collective operation | One indexed Job attempt, headless Service, and ServiceAccount |
| Operation traffic | Workload, edge, declared-egress, and collective NetworkPolicies |
| Initial vertical sizing | A VerticalPodAutoscaler when requested and its API is installed |

Ordinary operation Deployments use the declared pod template. An active
operation runs at `scaling.horizontal.max`, with a default of one. Automatic
horizontal scaling is disabled. `Drain` operations scale to zero after
completion unless their pods still hold unconsumed ephemeral segments or local
retained internal output. `Never` operations remain active. Initial vertical
sizing applies only to `Never`; automatic vertical updates are disabled.

A collective Job has `completionMode: Indexed`, `parallelism` and `completions`
equal to `collective.size`, no in-pod restart, and one Job attempt per group
attempt. A headless Service publishes rank addresses on port 8090. Every rank
receives its completion index, group size, attempt name, framework checkpoint
reference, and the rank-zero rendezvous DNS name. Gang placement is rejected.

## Start and completion

A consumer of a non-feedback `Materialized` channel remains `Waiting` until
production closes. Pipelined consumers can start with their producers.

The coordinator reports operation completion only after input is sealed and
drained, work is no longer in flight, and the intended members finish their
callbacks. The controller then seals ordinary outbound channels. A finite
external producer must seal its channel explicitly.

A collective succeeds when its Job succeeds. A failed collective can start
another attempt only when `collective.maxAttempts` permits it and
`collective.checkpoint` names committed framework state. If the collective has
any graph channel, retry also requires operation checkpointing and the workload
object store. Internal collective output requires the object store even for one
attempt.

## Worker discovery

The controller injects these values into the worker container:

| Variable | Meaning |
|---|---|
| `STARK8S_COORDINATOR` | Coordinator Service URL |
| `STARK8S_WORKLOAD`, `STARK8S_OPERATION` | Graph identity |
| `STARK8S_INSTANCE`, `STARK8S_POD_IP` | Pod identity and segment address |
| `STARK8S_SLOTS` | Capacity hint; callbacks remain serial |
| `STARK8S_INBOUND`, `STARK8S_OUTBOUND` | Channel names |
| `STARK8S_FEEDBACK`, `STARK8S_FEEDBACK_OUT` | Feedback channel names |
| `STARK8S_SEGMENT_DIR` | Local segment directory |
| `STARK8S_TICK_INTERVAL` | Optional serial tick interval |
| `STARK8S_CHECKPOINT` | Whether operation checkpoints are enabled |
| `STARK8S_OBJECT_STORE_*` | Optional durable-store endpoint, region, credentials, and prefix |

The worker is a restartable init container named `stark8s-runtime` when the
pod template contains one; otherwise it is the first regular container. The
controller adds a startup probe to the named runtime when needed. The probe
checks the local listener while the worker waits for the application readiness
handshake before registering or claiming work. Native-sidecar operation depends
on support in the target Kubernetes version and container runtime.

The controller mounts `stark8s-segments` at `/var/lib/stark8s/segments` and
adds segment port 8090 to the worker. Other containers remain unchanged.

## Storage

Without `operation.segments`, the segment volume is an unsized `emptyDir`.
`operation.segments.size` sets its per-pod `sizeLimit` and raises the worker's
ephemeral-storage request to that size. The controller preserves explicit pod
volumes and resource limits and rejects contradictory sizing.

Local ephemeral segments are released after acknowledgement. A producer of
retained internal segments stays running because those bytes remain pod-local.
When `coordinator.objectStore` is configured, workers write immutable segment
objects and the coordinator writes fenced recovery checkpoints. The controller
validates the HTTP or HTTPS endpoint and credentials Secret name and injects
Secret references without copying credential values into status.

## Networking

NetworkPolicies allow DNS, operation-to-coordinator traffic on ports 8080 and
8090, and segment fetches inside the workload. Edge policies allow the declared
consumer to reach producer pods on port 8090. Collective peers can communicate
within their attempt.

An operation has no general external egress unless it declares `egress` or the
workload configures an object store. Object-store configuration adds an
outbound rule for the endpoint's HTTP or HTTPS port; that rule is not limited
to the endpoint address.
`Metadata` permits HTTP to the link-local metadata address. `Internet` permits
HTTPS outside private and link-local ranges. These policies restrict network
paths; they do not authenticate channel callers. Actual enforcement depends on
the cluster network plugin.

## Status and validation

Operation status reports `Waiting`, `Running`, `Succeeded`, or `Failed` with a
reason and message. Reasons distinguish materialized-input waits, dependency
startup, pod readiness, processing, recorded delivery failure, and completed
drain. Channel status reports pending, in-flight, produced, acknowledged, lost,
sealed, epoch, and delivery-failure data. Required-record loss fails the
workload instead of reporting successful completion.

Controller validation marks the workload failed for invalid graph references,
cycles without feedback, unsafe checkpoint replica counts, unsupported gang
placement, unsafe collective retry I/O, invalid object-store endpoints,
contradictory segment storage, invalid egress destinations, or invalid scaling
configuration.
