# stark8s

stark8s is a Kubernetes extension for workloads that are graphs. A workload
is a set of operations connected by channels. Each operation is one logical
computation backed by its own pool of pods, scaled horizontally and
vertically on its own. Each channel is a directed flow of records between
two operations and states how those records are partitioned, when they are
delivered, and whether they are kept. Cycles are allowed: a channel that
closes a cycle is marked as feedback and is executed as a sequence of
bulk-synchronous supersteps.

The target is to express shuffle-based engines such as Apache Spark
directly: a Spark stage becomes an operation, a shuffle becomes a
materialized hash-partitioned channel, and an iterative algorithm becomes a
feedback channel instead of a loop unrolled in a driver program.

Status: a working local runtime. Records move pod to pod: a producer pod
writes segments locally and serves them over HTTP, and a consumer pod
fetches them directly; a per-workload coordinator holds only metadata
(topology, pod registry, partition ownership, segment index, seals, loop
epochs). The controller, the coordinator, the worker SDK, and two example
workloads run end to end on a kind cluster. See
[Limitations](#limitations).

## The model in one example

```yaml
apiVersion: stark8s.io/v1alpha1
kind: Workload
metadata:
  name: wordcount
spec:
  operations:
    - name: read                       # source: no inbound channels
      template: {spec: {containers: [{name: main, image: stark8s:dev, command: ["/wordcount", "read"]}]}}
    - name: map
      scaling: {horizontal: {min: 1, max: 4}}
      template: {spec: {containers: [{name: main, image: stark8s:dev, command: ["/wordcount", "map"]}]}}
    - name: reduce
      scaling: {horizontal: {min: 1, max: 3}}
      template: {spec: {containers: [{name: main, image: stark8s:dev, command: ["/wordcount", "reduce"]}]}}
  channels:
    - {name: lines,   from: read,   to: map,    partitioning: {mode: RoundRobin, partitions: 8}, delivery: Pipelined}
    - {name: shuffle, from: map,    to: reduce, partitioning: {mode: Hash, partitions: 6},       delivery: Materialized, combine: Sum}
    - {name: totals,  from: reduce}                # no consumer: read from outside
```

`shuffle` declares `combine: Sum`, so `map` folds the counts for a word as it
buffers and puts one record per distinct word on the wire rather than one per
occurrence — the map-side half of a reduce-by-key. `Sum`, `Min`, `Max` and
`Count` are available; `Min` and `Max` are idempotent, so a consumer applying
them needs no deduplication despite at-least-once delivery.

`map` starts as soon as `read` produces lines and scales on the backlog of
`lines`. `reduce` is not started until `map` has completed, because
`shuffle` is Materialized; when it starts, its parallelism is chosen from
the size of the sealed shuffle, and each replica owns a fixed set of hash
partitions, so every count for a word lands on one replica.

A cyclic example, PageRank with a feedback channel from `rank` to itself, is
in [examples/pagerank](examples/pagerank/workload.yaml).

## Running locally

Requirements: docker, kind, kubectl, Go 1.23.

```sh
hack/local-up.sh
```

This creates a kind cluster, builds one image containing the controller,
the exchange, and the examples, installs the CRD and controller, runs both
example workloads to completion, and prints their results. `hack/local-up.sh
--no-examples` installs without running the examples;
`hack/results.sh <workload> <channel>` prints the retained records of a
channel; `hack/local-down.sh` deletes the cluster.

Kind's default network plugin does not enforce NetworkPolicy. Run with
`STARK8S_CNI=calico hack/local-up.sh` to create the cluster with Calico so
the generated policies are enforced.

## Documents

- [docs/design.md](docs/design.md) — the model, its three primitives, and
  the review that reduced the edge vocabulary to one kind of edge.
- [docs/kubernetes-mapping.md](docs/kubernetes-mapping.md) — how a Workload
  becomes Deployments, Jobs, Services, NetworkPolicies, and autoscalers,
  and how the pieces find each other.
- [docs/checkpoints.md](docs/checkpoints.md) — how finite operations commit
  state, consumed input positions, and durable output manifests together.
- [docs/language-runtime.md](docs/language-runtime.md) — the optional local
  worker runtime protocol and its Go and Python clients.
- [docs/spark-mapping.md](docs/spark-mapping.md) — how a Spark physical
  plan maps onto a Workload, and what the mapping exposes.
- [docs/precedent.md](docs/precedent.md) — prior systems and papers, and
  what this design takes from each.
- [docs/qualification.md](docs/qualification.md) — reproducible worker
  benchmarks and local-cluster correctness and restart checks.
- [web/editor.html](web/editor.html) — a single-file graph editor and
  viewer for Workloads that converts to and from the YAML;
  [web/README.md](web/README.md) describes it. The coordinator serves the
  same file at `/editor`, so a running workload can be watched by
  port-forwarding to it and opening that address.

## Layout

| path | contents |
|---|---|
| `api/v1alpha1` | the Workload type and generated CRD schema |
| `pkg/controller` | reconciler: Workload to Kubernetes resources |
| `pkg/coordinator` | the control-plane protocol (`api.go`) and the coordinator server |
| `pkg/exchange` | the earlier brokered in-memory channel runtime, kept only while `pkg/controller` still imports it |
| `pkg/sdk` | worker library: local segments, fetch, process, acknowledge, supersteps, pass-by-reference payloads (`blob.go`) |
| `pkg/runtime`, `clients/python` | local worker protocol and application clients |
| `cmd/controller`, `cmd/coordinator`, `cmd/runtime` | binaries |
| `examples/wordcount`, `examples/pagerank` | acyclic and cyclic examples |
| `config/crd`, `config/manager` | install manifests |
| `hack` | local cluster scripts |
| `web` | single-file Workload graph editor and its round-trip test |

## Limitations

- Segments live on the pod that produced them. When that pod expires before
  a consumer acknowledged them, the segments are marked lost and counted in
  the channel's `lost` metric; the consumer is not blocked, but the
  producing task is not re-executed, so the records are gone. The
  coordinator keeps its index in memory only; restarting it loses the
  segment index and the workload must be resubmitted.
- How much a producer can hold is `spec.operations[].segments.size`, which
  sizes the segment volume per pod and requests the matching ephemeral
  storage. Left unset the volume is a bare `emptyDir` whose capacity is
  whatever the cluster's defaults allow. Either way a producer that outgrows
  the space is evicted, losing its segments as above; declaring a size makes
  the eviction attributable to the operation rather than to the node filling
  up, but does not prevent it. There is no spill to external storage and no
  backpressure on a producer that fills the volume.
- Delivery is at-least-once. A consumer that expires has its unacknowledged
  records redelivered to another replica; application state on the expired
  replica is lost. There is no state checkpointing.
- Hash partition ownership is provisional until the coordinator hands the
  owner a segment for that partition, and pinned to it from then on: a
  replica accumulating state for a partition never loses it to a rebalance,
  only to its own expiry. Replicas that register later pick up whichever
  partitions have not been handed work yet, so a consumer whose pods start
  staggered still spreads over them; a partition already in use does not
  move, so `partitions` should still be at least the maximum replica count.
- The graph can be edited while a workload runs (the controller pushes the
  channel list on every pass and creates operations on demand), but
  removing an operation or channel from a running workload is not handled.
- Initial vertical sizing emits a VerticalPodAutoscaler only when that API is
  installed; the local scripts do not install it. Automatic horizontal
  scaling and automatic vertical updates are disabled while operation state
  and output remain pod-local.
