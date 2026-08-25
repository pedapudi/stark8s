# agent-loop: an agent graph as a Workload

Three operations and eleven channels express a tool-calling agent with a
human-in-the-loop interrupt and a recursion limit. See
[docs/agent-graphs.md](../../docs/agent-graphs.md) for the mapping from
graph-agent concepts to Workload constructs; this file is the operator's
guide.

```
            prompts (external)          human (external)
                 |                          |
                 v                          v
   +--------------------------------------------------+
   |                      agent                        |
   +--------------------------------------------------+
     |          ^         |          ^         |     |
  to-search  search-   to-calc   calc-     ask-human  answers
     |       results      |      results   (external) (external)
     v          |         v          |
  +--------+    |     +--------+     |
  | search |----+     |  calc  |-----+
  +--------+          +--------+
     |                    |
  stuck-search         stuck-calc     (overflow of the two loops, external)
```

Every channel is hash-partitioned on the record key, which is the thread
identifier, with the same partition count. All hops of one thread therefore
reach the same `agent` replica, and `agent` scales from 1 to 8 replicas on
the backlog of its inbound channels.

The agent is a deterministic rule-based planner. A question is a list of
segments separated by `;`:

| segment | action |
|---|---|
| `search: <term>` | emit the thread to `to-search`; the result returns on `search-results` |
| `calc: <a op b>` | emit the thread to `to-calc`; the result returns on `calc-results` |
| any segment containing `confirm` | emit the thread to `ask-human` and stop until a record with the same key arrives on `human` |
| anything else, or nothing left | emit the answer to `answers` |

The segment `search: again` is never consumed, so a thread whose question
is `search: again` loops between `agent` and `search` until the loop bound
(`maxEpochs: 12` on `search-results`) diverts it to `stuck-search`.

## Prerequisites

A cluster with the stark8s controller installed (`hack/local-up.sh
--no-examples`) and an image that contains the `/agent-loop` binary. The
repository Dockerfile must build `./examples/agent-loop` alongside the other
examples:

```
 && CGO_ENABLED=0 go build -o /out/agent-loop ./examples/agent-loop
```

Submit the workload:

```sh
kubectl apply -f examples/agent-loop/workload.yaml
kubectl get workload agent-loop -w
```

## Talking to the graph with kubectl

External channels are served by the workload's coordinator on port 8080.
The Kubernetes API server proxies to that service, so `kubectl get --raw`
and `kubectl create --raw` reach it without port-forwarding. Define the base
path once:

```sh
API=/api/v1/namespaces/default/services/agent-loop-coordinator:8080/proxy
```

Post a prompt. The body is a JSON array of records; the key is the thread
identifier and the value is the question:

```sh
echo '[{"key":"t1","value":"search: stark8s; calc: 2+2; done","epoch":0}]' \
  | kubectl create --raw "$API/channels/prompts/records" -f -
```

Read the answer for that thread. The response is the retained records of
the channel; `key=` filters to one thread:

```sh
kubectl get --raw "$API/channels/answers/records?key=t1"
```

The value is the thread state: `thread`, the remaining `question` (empty
when answered), the `steps` trace, and `scratch`, which on `answers` holds
the joined trace, for example
`search(stark8s) -> search "stark8s": stark8s runs graphs ... -> calc(2+2) -> 2+2 = 4`.

Watch the hops in pod logs; every line names the operation, thread, epoch,
and channel:

```sh
kubectl logs -l stark8s.io/operation=agent --prefix -f
```

## Interrupt and resume

A question containing `confirm` parks the thread outside the graph:

```sh
echo '[{"key":"t2","value":"search: pagerank; confirm publish; done","epoch":0}]' \
  | kubectl create --raw "$API/channels/prompts/records" -f -
kubectl get --raw "$API/channels/ask-human/records?key=t2"
```

The record on `ask-human` carries the full thread state, so an operator (or
an external approval system) can inspect it. Nothing further happens on
thread `t2` until a record with the same key is posted to `human`. The value
posted back is the thread state with `scratch` set to the reply text;
copying the parked state and replacing `scratch` is the simplest form:

```sh
echo '[{"key":"t2","value":{"thread":"t2","question":"done","steps":["search(pagerank)","search \"pagerank\": ...","ask-human(confirm publish)"],"scratch":"approved by operator"},"epoch":0}]' \
  | kubectl create --raw "$API/channels/human/records" -f -
kubectl get --raw "$API/channels/answers/records?key=t2"
```

The agent appends `human: approved by operator` to the trace and emits the
answer. `demo.sh` performs the copy with `jq`.

## Recursion limit

```sh
echo '[{"key":"loop","value":"search: again","epoch":0}]' \
  | kubectl create --raw "$API/channels/prompts/records" -f -
```

The thread bounces between `agent` and `search`. Each crossing of
`search-results` increments the record epoch; when it reaches 12 the
coordinator diverts the record to `stuck-search`:

```sh
kubectl get --raw "$API/channels/stuck-search/records?key=loop"
kubectl get workload agent-loop -o jsonpath='{.status.channels[?(@.name=="search-results")].overflowed}'
```

The diverted record carries the full state, with twelve `search(again)`
entries in `steps`, so the thread can be resumed by hand (post it to
`prompts` with a different question) or discarded.

## The whole sequence

`demo.sh` applies the workload, waits for the pods, runs the three
scenarios above, and prints the answers and the stuck record. It needs
`kubectl` and `jq`.

```sh
examples/agent-loop/demo.sh
```
