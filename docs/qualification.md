# Runtime qualification

The checked-in qualification cases measure the worker protocol with fixed
workload sizes and verify the example workloads against independent result
invariants. They establish reproducible baselines for later runtime changes.
They do not define universal throughput targets.

## In-process measurements

Run the worker benchmarks once per case so each result uses the declared data
size:

```sh
go test ./pkg/sdk -run '^$' -bench BenchmarkEmitAndFlush -benchtime=1x -benchmem
```

The cases cover 83,886 records with 100-byte values, 83 records with
100,000-byte values, and 10,000 records spread over 1,024 partitions. Each
case reports records per second, payload mebibytes per second, worker HTTP
requests, segment-file growth, allocated bytes, and allocation count. The
benchmark checks the coordinator's produced-record total before reporting a
result.

The baseline below was measured at commit `e079f93` on Linux/amd64 with an AMD
Ryzen AI MAX+ 395. The coordinator and segment servers ran in process over
loopback. Each case ran once.

| case | records/s | payload MiB/s | disk MiB | HTTP requests | allocated MiB |
|---|---:|---:|---:|---:|---:|
| 83,886 100-byte values, 1 partition | 1,489,747 | 142.1 | 10.95 | 2 | 82.00 |
| 83 100,000-byte values, 1 partition | 3,733 | 356.0 | 7.92 | 2 | 63.93 |
| 10,000 100-byte values, 1,024 partitions | 567,228 | 54.1 | 1.30 | 2 | 10.36 |

These numbers measure emission, JSON encoding, local segment writes, and
coordinator announcements. They exclude segment fetching, handler execution,
acknowledgement, pod networking, and controller reconciliation. Allocated bytes
are cumulative allocation volume rather than peak resident memory. Use the
local-cluster qualification for end-to-end correctness; collect pod metrics
separately before making a cluster memory or tail-latency claim.

`TestSlowReaderPreservesEveryRecord` adds a deterministic reader delay. It
checks that all records arrive, acknowledgement leaves no pending or in-flight
records, no record is lost, and completion includes the full handler delay.

## Local-cluster qualification

Create an isolated kind cluster and kubeconfig, then pass both to the script.
The script never creates or deletes a cluster and leaves all objects available
for inspection.

```sh
qualification_dir=$(mktemp -d)
kind create cluster --name stark8s-qualification \
  --kubeconfig "$qualification_dir/kubeconfig"
STARK8S_QUALIFICATION_CLUSTER=stark8s-qualification \
STARK8S_QUALIFICATION_KUBECONFIG="$qualification_dir/kubeconfig" \
  hack/qualify-local.sh
```

The script gives every run a unique namespace and image tag, leaves prior run
objects intact, and runs three workloads. By default, the finite
map/shuffle/reduce workload repeats its corpus 20,000 times and requires 43
unique words and 1,260,000 total words. Setting
`STARK8S_QUALIFICATION_DELETE_MAP=true` selects a separate fault case: the
script saves the selected map pod and coordinator metrics, deletes the pod
while input remains, and requires the workload to report `Failed` with lost
shuffle records. This is the expected result for pod-local Ephemeral output.

The CPU PageRank workload requires the five expected vertices, finite
nonnegative ranks, and a rank sum within 0.001 of one after 20 iterations. The
deterministic CPU training workload requires exactly 24 unique updates, finite
metrics, an increased mean reward, and a final mean reward of at least 0.85. A
controller restart verifies that the controller becomes ready again while
completed workload objects remain readable.

The map-worker deletion does not qualify stateful recovery. The controller
restart occurs after workload completion and does not qualify coordinator
recovery. Qualify checkpoint restoration and coordinator replacement with a
configured object store and an exact-output application check.
