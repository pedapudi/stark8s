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

The same one-shot emission cases at `a7f7f2d`, after worker bounds and durable
recovery changes, produced these measurements on the same host:

| Case | records/s | payload MiB/s | disk MiB | HTTP requests | allocated MiB |
|---|---:|---:|---:|---:|---:|
| Small records | 1,148,350 | 109.5 | 10.95 | 3 | 94.86 |
| Large payloads | 4,018 | 383.2 | 7.918 | 3 | 67.33 |
| 1,024 partitions | 460,233 | 43.89 | 1.297 | 3 | 12.73 |

These samples show additional request and allocation cost. Repetitions and
complete processing measurements are required before choosing an optimization
or making a throughput claim.

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

## Durable ETL recovery

The durable recovery check requires an existing cluster, an object store that
is reachable from workload pods, a namespaced Secret with `accessKey` and
`secretKey` entries, and a shared volume claim mounted at `/data`. Supply the
object-store endpoint, region, Secret name, volume claim, image tag, namespace,
and run ID through the generated test manifest. Do not put credentials in the
manifest or qualification artifacts.

Populate the shared volume with `inputs/manifest.json` and 20 immutable JSON
Lines files. Each file contains 10,000 copies of these records:

```json
{"region":"north","amount":3,"status":"settled"}
{"region":"south","amount":2,"status":"settled"}
```

The manifest records each `inputs/part-N.jsonl` key and the SHA-256 version of
its exact bytes. Use the four-operation manifest in
`examples/etl/workload.json`, enable its object store, give every operation the
same run ID and volume claim, and constrain `transform` and `aggregate` to one
replica. A small CPU limit makes both fault boundaries observable without
changing their results.

After `splits.acknowledged` is greater than zero and less than 20, save the
coordinator metrics and transform Pod manifest, then delete that transform
Pod. Wait for a replacement transform Pod with a different name. While
unacknowledged splits remain, save the metrics and coordinator Pod manifest,
then delete the workload coordinator Pod. Wait for the Workload phase to become
`Succeeded`.

Read only `datasets/regional-sales/complete.json` and the two files it names.
The complete result is exactly `north = 600000` and `south = 400000`, with no
other keys. The final coordinator metrics must report zero lost records on
every channel, 20 produced and acknowledged splits, one completed record, and
two output-file records. Keep the namespace and captured artifacts when the
check fails so the pre-fault ownership, replacement identities, and restored
metrics remain inspectable.
