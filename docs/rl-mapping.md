# Reinforcement-learning post-training as a Workload

Post-training a language model with reinforcement learning is a loop: a
sampler draws completions from the current policy, an environment scores
them, and a trainer turns the scored completions into the next policy, which
the sampler then uses. Google's OpenRL (https://github.com/gke-labs/open-rl)
is a self-hosted training API for this loop on Kubernetes. It separates the
loop into a trainer, a sampler, and an environment, and exposes the loop to
the researcher as a small set of calls: `sample`, `forward_backward`,
`optim_step`, and `save_weights_and_get_sampling_client`. The infrastructure
behind those calls is a gateway, a trainer worker, a vLLM sampler worker, a
Redis queue, and a shared volume that carries LoRA adapters from trainer to
sampler.

This document maps that loop onto the Workload model. There the loop is a
Kubernetes object, each of trainer, sampler, and environment is a pool of
pods, and each flow between them is a channel with declared partitioning,
delivery, and durability. The algorithm mapped is Group Relative Policy
Optimization (GRPO; Shao et al., "DeepSeekMath", 2024,
https://arxiv.org/abs/2402.03300), which OpenRL ships as its example loop.
[examples/grpo](../examples/grpo) realises the mapping over a policy small
enough to run without a model server.

## The loop

GRPO trains a policy from a verifiable reward. For each prompt the sampler
draws a group of completions, typically eight, from the current policy and
records the log-probability the policy assigned to each. The environment
scores every completion; for a mathematics prompt the score is whether the
final answer is correct. Within a group, each completion's advantage is its
reward minus the group mean, divided by the group's standard deviation, so a
group whose completions all scored the same carries no signal. The trainer
maximises a clipped surrogate objective. Its main term is the probability
ratio between the current policy and the policy that drew the completion,
times the advantage, with the ratio clipped to a band around one. A penalty
for divergence from a frozen reference policy is subtracted. After one or
more gradient steps on a batch the trainer publishes the new weights, and the
sampler draws the next batch from them.

Two properties of the loop drive the mapping. All completions of one group
must meet in one place, because the advantage is computed across the group.
Every sampler replica needs the same weights, because a completion drawn from
stale weights is off-policy and the ratio must correct for it.

## Concept mapping

| GRPO or OpenRL concept | Workload construct |
|---|---|
| Prompt dataset | A source operation, or a channel with no producer fed through the coordinator's records API |
| Prompt | Record key |
| Group of completions for one prompt | Records sharing a key |
| Sampler (vLLM worker) | Operation with a GPU pod template; one replica owns the prompts that hash to it |
| Environment, reward function | Operation with CPU pods, scaled on the backlog of completions waiting to be scored |
| Group normalisation | Co-partitioning: the completions and their scores are hashed on the prompt with equal partition counts into the trainer, so a group meets on one replica |
| Trainer | Operation with a GPU pod template |
| `save_weights_and_get_sampling_client` | A record on a Broadcast feedback channel from trainer to sampler |
| Number of optimiser steps | `feedback.maxEpochs` on that channel |
| The trained weights | The channel named in `feedback.overflow`: retained, with no consumer, read from outside |
| Training metrics | A retained channel with no consumer, read while the loop runs |
| Reference policy | A frozen row in the trainer, or an operation fed by the same completions and co-partitioned into the trainer |

## The graph

```yaml
operations:
  - {name: prompts, template: ...}                                          # source
  - {name: sampler, slots: 4, scaling: {horizontal: {min: 2, max: 2}}, template: ...}
  - {name: reward,  scaling: {horizontal: {min: 1, max: 4}}, template: ...}
  - {name: trainer, scaling: {horizontal: {min: 1, max: 1}}, template: ...}
channels:
  - {name: prompt-set, from: prompts, to: sampler, partitioning: {mode: Hash, partitions: 8}, delivery: Materialized}
  - {name: rollouts,   from: sampler, to: reward,  partitioning: {mode: Hash, partitions: 8}}
  - {name: samples,    from: sampler, to: trainer, partitioning: {mode: Hash, partitions: 8}}
  - {name: rewards,    from: reward,  to: trainer, partitioning: {mode: Hash, partitions: 8}}
  - name: weights
    from: trainer
    to: sampler
    partitioning: {mode: Broadcast}
    feedback: {mode: Asynchronous, maxEpochs: 30, overflow: final-policy}
  - {name: final-policy, from: trainer, durability: Retained}
  - {name: metrics,      from: trainer, durability: Retained}
```

The prompt set is hashed on the prompt, so each sampler replica owns a fixed
share of the prompts, and Materialized, so no sampler starts before the whole
set is written. A sampler draws a group per prompt at version zero as soon as
its prompts arrive, and again for every version it receives on `weights`.
Every sample goes out twice: on `rollouts` to be scored and on `samples` to
be trained on. The reward operation scores each sample and emits the score
under the same key. The trainer receives samples and scores for the same
prompt on the same replica, joins them by (prompt, version, index), and takes
its gradient steps when every group of a version is complete. It emits the
new version on `weights` and the step's mean reward on `metrics`.

Epochs carry the version. A sample drawn under version v is stamped with
epoch v, and the score derived from it keeps that epoch. The record that
completes the batch for version v therefore has epoch v, and the worker
library stamps what the trainer emits into the feedback channel with epoch
v+1. When v+1 reaches `maxEpochs` the record is diverted to `final-policy`
instead of delivered. Nothing further enters the cycle, the coordinator seals
`weights` once every channel on the cycle is quiet, and the operations drain
and complete. `kubectl get workload grpo` reports `Succeeded`, and
`hack/results.sh grpo final-policy` prints the trained policy.

## The example

The policy in `examples/grpo` is a table of logits with one row per prompt
and one column per action, and the reward is one when the drawn action is the
prompt's target. This keeps the loop free of a model server while preserving
the objective. The trainer forms the probability ratio from the
log-probability carried on each sample, clips it, applies the
group-normalised advantage, and adds the penalty toward the initial policy.
It takes three gradient steps per batch. Sampling is seeded by (prompt,
version, index), so the result is the same whichever replica draws a sample
and in whatever order records arrive.

`main_test.go` runs the whole graph in one process against a real
coordinator: one prompts pod, two sampler pods, two reward pods, and one
trainer pod, exchanging segments over HTTP. It asserts that:

- the policy diverted to `final-policy` carries version 30 and puts at
  least 0.9 probability on the target of every prompt (measured: mean reward
  rises from 0.19 to 0.95 over the 30 steps);
- `metrics` holds one record per step and the mean reward at the last step
  exceeds the first;
- each sampler pod received every version from 1 to 29 on the Broadcast
  channel;
- both reward pods scored samples;
- `weights` sealed after producing 29 versions.

The single-goroutine loop in the same file, which calls the sampler, scorer,
and optimiser directly, reaches the same rewards at every step as the
distributed run.

## Where the engine shapes the design

### The barrier between versions is held by the trainer

A Synchronous feedback channel advances its epoch when every consumer replica
has reported the epoch finished and the channel holds nothing pending or in
flight. A channel whose producer has not yet emitted anything is also quiet.
When the producer and consumer are the same operation, as in
`examples/pagerank`, a replica fills the channel before it reports, so the
barrier holds. When the producer is another operation, as the trainer is to
the sampler, the sampler reports as soon as it has emitted. The epoch then
advances before the trainer has produced anything, and the sampler's next
epoch callback runs against stale weights.

The GRPO graph therefore uses an Asynchronous feedback channel, which carries
the version on each record and imposes no barrier, and keeps the barrier in
the trainer: it steps only when every group of a version has arrived. Because
the trainer waits for the whole batch, every sample is drawn from the version
the trainer is at, and the probability ratio is one on the first gradient
step. It departs from one on the second and third steps, which is where the
clip acts. A sampler that drew continuously while the trainer stepped would
produce samples from older versions; the ratio carried on each sample is what
would correct for them.

### Weights are records; adapters are references

A record value is JSON. The example's policy is a few hundred numbers and
travels as the value of the `weights` record. A LoRA adapter is tens to
hundreds of megabytes and a full set of parameters is gigabytes. The record
for those carries a reference, such as the version number and a path on a
volume that both the trainer and the sampler mount. OpenRL's deployment
already has that volume: the shared Filestore claim on which the trainer
writes adapter snapshots and the sampler reads them. The channel then carries
the notification that a version exists, and the Broadcast partitioning
delivers that notification to every sampler replica.

### The sampler pool is sized for the run

A Broadcast channel is a log, and a consumer replica that registers after
records were produced reads the log from its beginning. A sampler replica
added at version 300 would receive and apply versions 1 through 300 in order,
and, on an Ephemeral channel, would find that the trainer had already deleted
segments its earlier peers acknowledged. The example fixes the sampler pool
at two replicas (`min: 2, max: 2`) for the whole run. A channel that kept
only the newest record per key would let the pool grow during a run; the
model has no such retention mode.

### A record is acknowledged when its handler returns

The worker library acknowledges a segment once the record handler has
returned for each of its records. The trainer accumulates a version's samples
and scores across many segments before it steps, so those segments are
acknowledged before the step that uses them. A trainer pod that fails after
acknowledging and before stepping loses the batch; the coordinator redelivers
nothing, because it recorded the segments as processed. The example accepts
this because a lost batch costs one version of sampling. A trainer whose step
is expensive would checkpoint its policy and optimiser state to the shared
volume after each step and reload it on start.

### Reward scoring scales from the backlog

The reward operation is stateless, so its replica count follows the
partitions of `rollouts` that hold unscored samples. With `partitions: 8` and
`max: 4` it runs between one and four pods as samples arrive, and it drains
back to one. OpenRL names the environment step, often bound by slow CPU or
network work such as executing generated code, as the point at which its loop
stalls. Under this mapping that pool is the one that scales, from a signal
the coordinator already holds, while the GPU pools stay at their declared
size.

### One channel, one consumer

A channel has one consuming operation. A sample must reach both the reward
operation and the trainer, so the sampler emits it twice, on `rollouts` and
on `samples`. The two channels are declared separately, carry the same
records, and are partitioned the same way. This doubles the segments the
sampler writes; the alternative, in which the reward operation forwards each
sample with its score attached, halves them and makes `rewards` carry the
sample as well.

### Delivery is at least once

A consumer replica that stops polling is expired and its unacknowledged
segments are redelivered to another replica, so a sample or score can arrive
twice. The trainer indexes each group by sample index, so a redelivered
record overwrites the one it duplicates. A verifier that has side effects,
such as one that executes code, must be idempotent for the same reason.

### A trainer is one replica

The replicas of an operation are interchangeable pods of a Deployment. They
have no rank, no rendezvous address, and no all-or-nothing start, which
distributed training across pods needs. The example's trainer is one replica,
and OpenRL's trainer worker is one pod. A trainer split across pods by prompt
would rely on co-partitioning of `samples` and `rewards`, which the model
provides, and on the replicas averaging their gradients, which it does not.

## Placing OpenRL on a Workload

OpenRL's gateway fronts a trainer worker and a sampler worker and queues
requests through Redis. On a Workload the gateway becomes the external
producer and consumer of the graph above. It reaches the graph through the
coordinator's records API, on the channels that have one end open:

| OpenRL call | Workload interaction |
|---|---|
| `sample(prompts, n)` | `POST /channels/prompt-set/records` with one record per prompt; `GET /channels/<completions>/records?key=<prompt>&wait=` to long-poll for the group, where `<completions>` is a retained channel with no consumer that the sampler also emits to |
| `forward_backward`, `optim_step` | The trainer operation, driven by the records that reach it |
| `save_weights_and_get_sampling_client` | The record the trainer emits on `weights`; the client is the version it names |
| Reading training metrics | `GET /channels/metrics/records` |
| Several concurrent jobs | One Workload per job. A sampler that serves several adapters at once, as vLLM does, is one sampler operation with one `weights` channel per trainer operation and the adapter named in the record key |

The Workload replaces the Redis queue and the hand-written Deployments; the
shared volume for adapters stays. It adds:

- scaling of the environment pool from queue depth;
- a NetworkPolicy per edge, so that a verifier executing generated code can
  reach neither the trainer nor the sampler;
- a ServiceAccount per operation;
- the state of every channel in `kubectl get workload -o yaml`.

## Not covered

- The example has run only in-process, in the test. The Dockerfile builds
  the binary and `workload.yaml` is written for the cluster path, but the
  cluster path is unverified.
- The worker library is Go. A sampler built on vLLM and a trainer built on
  PyTorch would call the coordinator's HTTP protocol, defined in
  `pkg/coordinator/api.go`, from Python; no such client exists in this
  repository.
- The coordinator keeps its index in memory. A coordinator restart during a
  run loses the segment index and the retained channels, and the workload
  must be resubmitted.
- Strict on-policy training under a Synchronous barrier across operations
  is not expressible, for the reason given above. The engine change it
  needs is a barrier that advances when every operation on the cycle has
  finished the epoch, rather than when the feedback channel alone is
  quiet.
