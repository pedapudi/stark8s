# Replayable file ETL

This example reads immutable JSON Lines files, filters and maps sales records,
sums amounts by region, and commits two output partitions. The graph has four
operations: `read`, `transform`, `aggregate`, and `publish`.

The input manifest at `/data/inputs/manifest.json` contains storage keys and
content versions:

```json
[
  {"key":"inputs/north.jsonl","version":"content-hash"},
  {"key":"inputs/south.jsonl","version":"content-hash"}
]
```

The `read` operation verifies every version before emitting a stable split ID.
Each file is one split, and each JSON line can contain up to 16 MiB. The
checkpointed `transform` operation accepts objects with string `region` and
`status` fields and an integer `amount` field. It keeps records whose status is
`settled`. The `values` channel combines sums before the checkpointed
`aggregate` operation computes final totals by region. Checkpointed operations
use one replica because one checkpoint owns their complete operation state.

The aggregate operation writes canonical JSON Lines files below
`datasets/regional-sales/attempts/<attempt>/`. An identical full-stage retry
can reuse those files. A retry that produces different bytes under the same
attempt fails. The `publish` operation verifies every required file and uses a
conditional write to create `datasets/regional-sales/complete.json`. Readers
open only the completion manifest, so files from abandoned attempts remain
invisible.

All operation pods mount the same `ReadWriteMany` persistent volume at `/data`.
Replace `replace-with-shared-files` in `workload.json` with a claim backed by a
filesystem that preserves atomic link, rename, and `fsync` behavior. Replace
`replace-with-run-id` with one attempt ID shared by all four operations.
Configure `spec.coordinator.objectStore` with durable storage for worker
segments and checkpoints. The checked-in endpoint and credentials secret are
placeholders.

Generate the same Workload API object from Python with:

```sh
python3 examples/etl/build_workload.py
```

The builder accepts ordinary code changes for per-operation CPU and memory
requests. `test_build_workload.py` checks that the generated API object equals
the checked-in manifest.

The helper covers immutable whole-file JSON Lines splits, map/filter callbacks,
integer sum aggregation, and committed JSON Lines output. It does not parse
SQL, split within a file, or provide connector and format abstractions.

The storage commit protects readers from partial output. Worker checkpoints
commit processed input segments, durable internal outputs, and application
state together. The aggregate snapshot contains one running sum per distinct
key. A replacement pod restores those sums before consuming uncommitted input.
