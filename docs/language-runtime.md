# Local worker runtime protocol

The optional runtime container owns coordinator registration, record exchange,
acknowledgements, completion, bounded buffering, and cancellation. An
application connects to it over HTTP on `127.0.0.1:8081` and handles one
callback at a time. Existing applications that use `pkg/sdk.Worker` continue
to run without the runtime container.

The runtime has five endpoints:

| Request | Result |
|---|---|
| `GET /healthz` | Reports that the local HTTP server can accept an application connection. |
| `POST /v1/connect` | Starts an application session and returns its `sessionId`. |
| `POST /v1/ready` | Allows the worker to start source or record callbacks for that session. |
| `GET /v1/invocations?sessionId=...` | Waits until the next serial callback is available. |
| `POST /v1/replies` | Completes the named invocation with output records or an error. |

Every invocation has an `invocationId` and one of the kinds `source`, `record`,
`epoch`, `drain`, or `tick`. A configured operation checkpoint also uses
`snapshot` and `restore`. Snapshot replies carry JSON state. That JSON may be
the complete state or a document naming application-owned durable objects.
Restore receives the same JSON in the invocation's `data` field. Replies
contain the session and invocation identifiers. A mismatched identifier
returns HTTP 409.

`epoch` and `drain` are optional notifications; a client with no matching
callback acknowledges them without output. Source and record callbacks remain
required when the worker invokes them. Tick is sent only when the operation
sets a positive tick interval and requires an explicit callback.

A new connection replaces the previous application session. If a callback was
unfinished, the new session receives the same invocation identifier and may
complete it. Replies from the replaced session are rejected. Redelivery means
the application must make callback effects idempotent. The runtime can recover
the worker's unfinished segment delivery, but application memory is lost when
the application restarts.

For a checkpointed operation, replacing an application session instead fails
the worker attempt. A fresh worker process restores the last committed snapshot
before replaying input. Continuing inside the surviving worker process would
replay against an empty application process and violate the checkpoint
boundary.

The Go client is in `pkg/runtime.Client`. The Python client is
`clients/python/stark8s_runtime.py` and uses only the Python standard library.
Both clients connect, declare readiness, long-poll, dispatch callbacks, and
post outputs or errors. The runtime validates every output in one reply before
emitting any of them.

Use the runtime as a native sidecar so it starts before the application:

```yaml
initContainers:
  - name: stark8s-runtime
    image: example/stark8s:VERSION
    command: [/runtime]
    restartPolicy: Always
containers:
  - name: application
    image: example/application:VERSION
```

The controller adds a startup probe for `/healthz` on port 8081 to a native
sidecar named `stark8s-runtime` when the template omits one. The probe checks
only the HTTP listener. Waiting there for application readiness would prevent
the application container from starting. The worker waits for `POST /v1/ready`
before it consumes records. An arbitrary application image still needs one of
the clients or an adapter that implements this protocol.

Checkpoint callbacks are offered only when the worker has checkpoint storage.
The application must implement both. The checkpoint restrictions in
[checkpoints.md](checkpoints.md) still apply to adapter applications.
Checkpointed operations currently reject Tick because tick state and output do
not have an input-segment checkpoint boundary.

Compatible protocol additions may add optional JSON fields or new invocation
kinds. Clients must ignore unknown fields and report an unsupported invocation
kind as an application error. Removing a field, changing its meaning, or
changing redelivery rules requires a new URL version.
