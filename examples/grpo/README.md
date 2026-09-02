# grpo: reinforcement learning as a Workload

Group Relative Policy Optimization expressed as five operations and seven
channels. A group of completions is sampled per prompt, scored, centred
against its own group, and folded into one policy that is broadcast back to
every sampler.

```
prompts --batch--> rollout --completions--> reward --scored--> advantage
                      ^                                            |
                      \--------------weights--------------\   advantages
                           (Broadcast, feedback)           \        |
                                                            \-- learner
                                                 metrics, checkpoints (Retained)
```

- **prompts** — a source, one record per task. Hash-partitioned, so each
  rollout replica owns a disjoint set.
- **rollout** — holds the current policy and the tasks it owns. For each task
  it draws `G` completions and emits them, and it redraws every time new
  weights arrive.
- **reward** — scores one completion. Stateless, and any replica will do.
- **advantage** — gathers the `G` scores of one task and centres them:
  `(r - mean) / std`.
- **learner** — the one owner of theta. It gathers a group from every task,
  applies the update, and broadcasts the result.

## Why this maps when a rollout tree does not

Reinforcement learning is often filed under what a fixed graph cannot express,
on the grounds that rollouts spawn according to the policy. That is right
about a search tree and wrong about GRPO, and the difference is worth being
precise about.

GRPO's graph is known before it runs. The group size is a hyperparameter, the
task set is fixed, and nothing about the shape of the computation depends on
what a rollout came back with — only the numbers change. A tree search that
expands the branches that scored well needs a graph that is a function of its
own results, and that is the thing a Workload cannot be.

## The two barriers, and why neither is a channel

GRPO needs to wait twice: for all `G` rewards of one prompt before an
advantage can be computed, and for every prompt's group before a step can be
taken. Neither is `delivery: Materialized`.

Materialized seals a channel when its **producing operation completes**. In a
training loop no operation ever completes — `rollout` and `reward` run for the
whole run — so a Materialized edge here would deliver nothing until the end.

Both barriers therefore live in application code, counting to a constant:
`advantage` holds a group open until it has `G` records for one `(step, task)`,
and `learner` holds a step open until it has a group from every task. That the
counts *are* constants is the same fact that makes the graph expressible at
all.

## The epoch is the step

The epoch enters the cycle on a weights record and every downstream emit
inherits it, so a completion, its score and its advantage all carry the step
they belong to. `advantage` keys its groups by it; `learner` keys its steps by
it.

That also ends the run. An update at epoch `e` emits weights stamped `e+1`, so
with `maxEpochs: 24` the update at epoch 23 emits epoch 24, the engine drops
it, and no further rollouts are triggered. **maxEpochs is the number of
updates.**

## The policy

A table of logits per prompt: `theta[q][t][v]` scores token `v` at position
`t` of a completion for prompt `q`. Sampling is `T` independent draws.

The conditioning is not decoration. One table shared across prompts that want
different answers has no setting that satisfies them — the gradients cancel,
and the reward sits flat while everything else in the graph appears to work.

Every gradient is analytic, so the example needs no autodiff and no
accelerator:

- `d logP(o) / d theta[q][t][v] = 1[v == o_t] - p_t(v)`
- the KL to the frozen reference differentiates in closed form, because a
  categorical KL is a sum rather than the sampled estimator a language model
  would need.

Two things a reader should expect to look wrong and do not:

- **The reported objective is about zero.** With one gradient step per batch
  the importance ratio is exactly 1 at the sampling point and the advantages
  of a group sum to zero. The gradient is what carries the update. The ratio
  and the clip are written out in full because both stop being trivial the
  moment a second inner step is taken.
- **The step is normalised per group, not per batch.** Each prompt's
  parameters see only its own group, so dividing by the total sample count
  would make the effective step size depend on how many prompts happen to be
  batched together.

## The task

Each task asks for one fixed sequence; a completion scores the fraction of
positions it got right. The optimum is therefore known, and the test checks
against it rather than eyeballing a reward curve. It is the reinforcement
learning analogue of fitting a line to points you chose.

Sampling is seeded from `(prompt, step, index)`, so which replica happens to
run a rollout cannot change what it draws: the run is reproducible at any pod
count.

## Running it

```sh
go test ./examples/grpo/
```

The test runs the whole workload in one process against a real coordinator on
an httptest server: one prompts pod, two rollout pods, two reward pods, two
advantage pods and one learner, exchanging real segments over real HTTP
through the real Broadcast feedback channel. It asserts that the loop stopped
at the bound after exactly `maxEpochs` updates, that every step reported a
metric, that the mean reward rose, and that the learned policy puts its mass
on the right token at every position of every task.

A recent run: mean reward `0.302 -> 0.906` over 24 steps.

On a cluster, watch it while it runs:

```sh
kubectl -n <namespace> port-forward svc/grpo-coordinator 18080:8080
curl -s 'http://127.0.0.1:18080/channels/metrics/records?after=0'
```

`metrics` carries the mean reward, the objective and the KL for each step;
`checkpoints` carries theta. Both are `Retained`, so they are readable from
outside while training is still in flight.

## Swapping in a real model

The graph does not change. `rollout` gets a GPU request and generates with a
language model instead of a table; `reward` keeps a verifiable scorer or calls
a reward model; `learner` runs an optimizer step. What stays fixed is the part
this example exists to show: the group is a Hash partition on the prompt, the
policy is a Broadcast feedback edge, and the barriers are counts in
application code.
