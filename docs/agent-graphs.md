# Agent graphs as Workloads

An agent application built with a graph framework such as LangGraph is a
directed graph whose vertices are steps of a program (a model call, a tool,
a router) and whose edges say which step runs after which. The graph is
usually executed inside one process: the framework walks the edges, keeps
the state of each conversation in memory or in a checkpoint store, and
enforces a recursion limit by counting steps. This document maps each
concept of such a framework onto the Workload model, in which the graph is
a Kubernetes object, each vertex is a pool of pods, and each edge is a
channel with declared partitioning, delivery, and durability. The example in
[examples/agent-loop](../examples/agent-loop) realises the mapping.

## Concept mapping

| Graph framework concept | Workload construct |
|---|---|
| Node | Operation |
| Thread | Record key |
| State | Record value |
| Edge | Channel |
| Conditional edge | The producing operation chooses the channel it emits to |
| Cycle | A channel marked as feedback |
| Recursion limit | `feedback.maxEpochs` with an overflow channel |
| START | A channel with no producer |
| END | A channel with no consumer, durability Retained |
| Interrupt | Emission to a channel with no consumer |
| Resume | A channel with no producer that carries the same key back in |
| Checkpointer | The retained records of the channels the state traverses |
| Subgraph | A group of operations joined by channels; a nested Workload is equivalent |

### Node

A node is one step of the program. In a Workload it is an operation: a pod
template with its own replica count, scaled from the backlog of its inbound
channels. A model-calling agent, a search tool, and a calculator are three
operations in the example. Each has its own image, resources, secrets, and
service account, and each is scaled on its own. An agent that is slow
because it waits on a language model can run eight replicas while a
calculator runs one.

An operation with completion `Never` runs until the Workload is deleted,
which is the appropriate lifetime for an agent serving an open-ended stream
of conversations. Completion `Drain` is appropriate for a batch of
conversations that is submitted once and read out when every thread has
finished.

### Thread

A thread is one conversation, with its own state and its own position in
the graph. In a Workload the thread is the record key. Every channel of the
example is hash-partitioned on the key with the same partition count, so
every record of one thread reaches the same replica of every consuming
operation. Hash ownership is held per consuming operation, so two channels
into the agent with equal partition counts are co-partitioned: a thread's
prompt, its tool results, and its human replies all land on one agent
replica. That replica can hold per-thread memory if it wishes, though the
example keeps none.

### State

A framework's state object is the value that flows along the edges. In a
Workload it is the record value. The example carries the whole state on
every record: the thread identifier, the part of the question that has not
been planned, the trace of steps so far, and a scratch field for the
argument or result of the step in flight. Because the state is complete on
every record, any replica that owns the thread's partition can take the next
hop, and the retained channels double as a log of the state at every point
it crossed an edge.

### Edge and conditional edge

A plain edge says that the target node runs after the source node. In a
Workload it is a channel from the source operation to the target operation.
A conditional edge, a router that chooses the next node from the state, is
expressed by the producing operation choosing which of its outbound channels
to emit to. The agent in the example has five outbound channels and emits to
exactly one per hop. No routing object exists in the graph; the set of
possible routes is the set of declared channels, which the controller uses
to derive network policy and coordinator authorisation.

### Cycle

A tool-call loop is a cycle: the agent calls a tool, the tool replies, the
agent calls again. The Workload model requires that each cycle be closed by
a channel marked as feedback. The example marks the two result channels,
`search-results` and `calc-results`, as feedback with mode `Asynchronous`.
In this mode the loop has no barrier; each record carries its own epoch,
incremented each time it crosses the feedback channel, so each thread
iterates on its own schedule and a slow thread never delays a fast one.

The other mode, `Synchronous`, runs the loop as bulk-synchronous supersteps
and is suited to iterative algorithms over a whole dataset. An agent
population that should advance in lockstep, for instance a simulation in
which every agent takes one turn per round, would use it.

### Recursion limit

A framework's recursion limit bounds the number of steps a thread may take
and raises an error when it is reached. In a Workload the bound is
`feedback.maxEpochs` on the feedback channel, and the action at the bound is
declared: a record whose epoch reaches the bound is diverted to the channel
named in `feedback.overflow`, or dropped and counted when no overflow
channel is named. The example diverts to `stuck-search` and `stuck-calc`,
retained channels with no consumer, so a stuck thread stops and waits with
its full state for an operator or another operation to decide what to do. The Workload status reports the count of
diverted records per channel.

The overflow channel is declared with the tool as producer, because the
divert happens where the record is produced into the feedback channel. Each
loop therefore has its own overflow channel.

### START and END

START is the point where a new thread enters the graph. In a Workload it is
a channel with no producer, fed from outside through the coordinator's
records API. END is the point where a thread leaves the graph with its
result; it is a channel with no consumer, whose records are retained and
read from outside through the same API. The example has `prompts` as its
START and `answers` as its END.

### Interrupt and resume

A framework's interrupt suspends a thread at a node, exposes its state to an
external party, and resumes it when that party supplies a value. In a
Workload the suspension is an emission to a channel with no consumer
(`ask-human` in the example) and the resumption is a record with the same
key on a channel with no producer (`human`). The parked state is visible on
the retained interrupt channel; the resume value is delivered to the agent
replica that owns the thread's partition, by the same hashing as every other
record of that thread. No suspended computation exists inside any pod; the
agent handles the resume record as it handles any other record, and the
thread's state is entirely in the record.

### Checkpointer

A framework's checkpointer persists the state of every thread after every
step so that a thread can be resumed or inspected. In a Workload the
equivalent is durability `Retained` on the channels the state traverses.
The example retains the interrupt, answer, and overflow channels; retaining
the tool channels as well would give a complete per-hop history of every
thread, readable by key through the records API. The coordinator keeps
records for external channels in memory on one pod, so retention is a
per-channel declaration whose durability across coordinator restarts depends
on the channel transport in use.

### Subgraph

A subgraph is a graph used as a node of another graph. In a Workload a
subgraph is a set of operations joined by channels; there is no boundary
object, because the operations of the inner graph are operations of the
outer one, with their own scaling and isolation. Where a boundary is wanted
for ownership or lifecycle reasons, the inner graph can be a separate
Workload whose START and END channels are connected by an external process,
or by a bridging operation, to the outer Workload's channels.

## What the Workload representation adds

**Per-edge network isolation between agents.** The controller derives a
NetworkPolicy from the topology: an operation's pods may reach only the
coordinator and the segment servers of the operations that produce for them,
and the coordinator authorises per channel which operation may produce and
which may consume. A search tool cannot post to the calculator's channel,
and an agent cannot read another agent's inbound channel, because those
edges are absent from the graph. Each operation runs under its own service
account with its own secrets, so a tool that holds a credential does not
share a process, a pod, or a network path with the model-calling agent.

**Per-agent scaling from queue depth.** Each operation is scaled from the
backlog of its own inbound channels. The signal is topology-aware and needs
no application metrics: a growing backlog on `to-search` means the search
tool is under-provisioned, and a growing backlog on `prompts` and the result
channels means the agent is. The bounds and target backlog per replica are
declared per operation, so an expensive model-calling step and a cheap tool
step scale independently.

**Graph observability.** The Workload status reports, per channel, the
records pending, in flight, produced, and diverted at the loop bound, and
per operation the replica count and the number of partitions with work. A
thread's position in the graph is which channel currently holds a record
with its key, and its history is the retained records with that key. The
pod logs of every operation name the thread, epoch, and channel on every
hop, so the flow of one conversation can be followed across pods with a
single label selector.
