# stark8s

stark8s runs record-processing graphs on Kubernetes. A `Workload` declares
operations and the channels between them. Channels define partitioning,
delivery timing, retention, and bounded feedback. Operations use fixed replica
membership during an attempt.

The runtime supports pipelined and materialized dataflow, hash shuffles,
broadcast, finite synchronous cycles, external channels, durable coordinator
state, single-owner application checkpoints, a retained-subscription HTTP API,
replayable file ETL, and fixed-size collectives. Applications can use the Go
worker or the local HTTP runtime with Go and Python clients. Callbacks are
serial within each worker, and delivery is at-least-once.

```yaml
apiVersion: stark8s.io/v1alpha1
kind: Workload
metadata:
  name: wordcount
spec:
  operations:
    - name: read
      template:
        spec:
          containers:
            - name: main
              image: stark8s:dev
              imagePullPolicy: IfNotPresent
              command: ["/wordcount", "read"]
              env:
                - {name: STARK8S_WORDCOUNT_REPEAT, value: "200"}
    - name: map
      slots: 2
      scaling:
        horizontal: {min: 1, max: 4}
      template:
        spec:
          containers:
            - name: main
              image: stark8s:dev
              imagePullPolicy: IfNotPresent
              command: ["/wordcount", "map"]
    - name: reduce
      slots: 2
      scaling:
        horizontal: {min: 1, max: 3}
      template:
        spec:
          containers:
            - name: main
              image: stark8s:dev
              imagePullPolicy: IfNotPresent
              command: ["/wordcount", "reduce"]
  channels:
    - name: lines
      from: read
      to: map
      partitioning: {mode: RoundRobin, partitions: 8}
      delivery: Pipelined
    - name: shuffle
      from: map
      to: reduce
      partitioning: {mode: Hash, partitions: 6}
      delivery: Materialized
    - name: totals
      from: reduce
      # No consumer: results are read from outside through the coordinator API.
```

`Materialized` delays `reduce` until `shuffle` closes production. Hash
partitioning keeps equal keys on one worker. `slots` is a capacity hint exposed
to the worker and status; callbacks remain serial. The controller currently
runs an active ordinary operation at `scaling.horizontal.max` and disables
automatic horizontal scaling.

## Run locally

The development environment requires Docker, kind, kubectl, and Go 1.23.

```sh
hack/local-up.sh
```

`hack/local-up.sh --no-examples` skips examples, and `hack/local-down.sh`
removes the cluster.

## Documentation

- [Design](docs/design.md) defines execution and recovery boundaries.
- [Kubernetes mapping](docs/kubernetes-mapping.md) lists generated resources,
  discovery, storage, networking, scaling, and status.
- [Systems mapping](docs/systems-mapping.md) maps supported workload shapes.
- [Checkpoints](docs/checkpoints.md) defines single-owner state recovery.
- [Local runtime](docs/language-runtime.md) defines the serial HTTP protocol.
- [Retained subscriptions](docs/subscriptions.md) defines the coordinator API
  and its missing declarative worker binding.
- [Qualification](docs/qualification.md) records the measured test scope.
- [Custom resource schema](config/crd/stark8s.io_workloads.yaml) defines valid
  manifests.

## Boundaries

Configured object storage makes segments and coordinator state durable. Without
it, pod-local segments can be lost; required-record loss fails the workload.
`Retained` local output keeps its producer running and is not durable by itself.
Blob handles remain pod-local and are rejected for durable segment output.

Application checkpoints support one fixed ordinary owner. They commit state,
covered inputs, callback completion, and durable internal outputs. External
sinks require application-owned transactions or idempotence. Named subscription
cursors require an explicit coordinator API client because manifests and normal
workers do not bind operations to subscriptions.

Collectives use fixed-size indexed Jobs. Gang placement is unsupported.
Graph-connected retries require operation checkpointing and object storage;
internal output requires object storage even for one attempt.

The runtime does not provide event-time watermarks or windows, channel
authentication, shared resource budgets, live state migration, or automatic
recovery of arbitrary external side effects.
