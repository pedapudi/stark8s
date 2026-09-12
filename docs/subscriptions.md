# Retained subscription HTTP API

The workload coordinator exposes named cursors for a `Retained` channel on its
control service. Each subscription stores one absolute position per partition.
An append identifier makes an identical retry return its committed offset and
rejects different data under the same identifier. An identical retry remains
valid after sealing; a new append does not. Explicit retention deletion removes
the append identifier with its retained entry.

| Request | Body and result |
|---|---|
| `POST /channels/{channel}/appends` | Accepts `appendId`, `partition`, and `records`; returns the partition and committed offset. |
| `PUT /channels/{channel}/subscriptions/{name}` | Accepts an operation name, or an empty operation for an external reader. |
| `GET /channels/{channel}/subscriptions/{name}/consume?pod={pod}&max={count}` | Returns ordered retained work for available partitions. |
| `POST /channels/{channel}/subscriptions/{name}/ack?pod={pod}` | Accepts `partition`, `offset`, and `appendId` acknowledgements. |
| `POST /channels/{channel}/subscriptions/{name}/replay` | Moves an idle subscription to supplied retained positions. |
| `POST /channels/{channel}/retention` | Deletes history before supplied partition positions. |

An operation client must register its pod and send the operation and process
incarnation headers. An external reader uses an empty subscription operation
and supplies a stable reader ID as `pod`. An unfinished external delivery
returns to that reader after a lost response or coordinator restart. Object
storage is required for bytes and cursor state to survive replacement.

No `Workload` field binds an operation to a subscription. The Go worker, local
runtime, and language clients use ordinary channel consumption and do not expose
these endpoints. A graph application must use an explicit coordinator client.
Declarative binding, client methods, and an end-to-end graph test remain open.
