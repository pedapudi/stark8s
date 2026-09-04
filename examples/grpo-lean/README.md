# grpo-lean: the stages want different machines

GRPO over a theorem prover. The reward is the Lean kernel, so there is no
judge, no reward model, and nothing to hack: a tactic closes the goal or the
compiler says why it did not.

The example exists because its two expensive stages want **different hardware
at different replica counts**. Generation needs an accelerator and finishes in
seconds. Verification needs no accelerator, costs about twelve seconds of CPU
per completion, and is embarrassingly parallel. A graph is what lets each have
what it wants.

```
prompts --batch--> rollout --completions--> reward --scored--> advantage
                      ^      (1x L4)         (Nx CPU)               |
                      \-------------weights------------\       advantages
                         (Broadcast, feedback: a URI)   \            |
                                                         \----- learner
                                                       metrics (Retained)
                                                                 (1x L4)
```

## Scaling, measured on the graph

One step of the real workload — 17 statements, 8 completions each, 136 proofs
through the kernel — with only the `reward` replica count changed:

| reward replicas | step wall s | speedup |
|---|---|---|
| 1 | 1768 | 1.00x |
| 2 | 992 | 1.78x |
| 4 | 553 | 3.20x |
| 8 | 311 | 5.69x |

Those are end-to-end step times, and the speedup is sublinear because part of a
step does not scale with the reward count. Fitting `wall = C/R + T` separates
the two: `C = 1653 s` of serial verification and `T = 131 s` for the advantage
stage, the training step and the checkpoint upload, at `R^2 = 0.998`. The
verification work itself is therefore close to linear in the replica count, and
`T` is what caps the end-to-end gain. Verification is 93% of a single-replica
step and 58% of an eight-replica one, at 12.2 s per proof serially.

Generation is the other side of the trade, and it is not the expensive part
here: the accelerator produced all 136 completions in **16 to 18 seconds** in
every configuration. Left in one process with the verifier, that accelerator
would sit idle for almost the whole step. The replica count on a CPU-only
operation is what buys it back. Nothing in that trade is available to a single
process without hand-rolling a work queue, a scheduler, and a pool of machines
that are not the machine the model is on.

One caveat on the per-proof cost. The twelve seconds is mostly
`import Mathlib`, identical warm or cold, so a persistent process with Mathlib
already loaded would cut it — at the cost of holding state in an operation that
is otherwise stateless. The scaling argument survives that change, because the
work stays CPU-bound, independent per completion, and off the accelerator.

## A binary reward trains on nothing

A proof compiles or it does not, so an eight-completion group collapses to
all-zero or all-one, every advantage is `(r - mean) / std = 0`, and GRPO trains
on nothing while every pod, epoch and metric looks healthy.

That is not hypothetical. Sampling `gemma-4-E2B-it` over 17 statements, 8
completions each, and running all 136 through the kernel:

| reward | groups with no gradient |
|---|---|
| binary: proved or not | 15 of 17 |
| graded, plain prompt | 5 of 17 |
| graded, prompt naming the tactics | 2 of 17 |

So the reward reports what the compiler said, in tiers:

| kernel output | reward |
|---|---|
| compiles, no `sorry` | 1.0 |
| `unsolved goals` — a well-formed tactic that did not close it | 0.4 |
| `unknown identifier` / `unknown constant` — an invented lemma | 0.1 |
| `unexpected token` — not Lean | 0.0 |

The tiers are not a judgment about proof quality; each is a distinct thing the
compiler reported, and they order the ways a candidate can be wrong. A tactic
that parses and leaves goals open is nearer a proof than one naming a lemma
that does not exist, which is nearer than text that is not Lean at all.

The prompt names the available tactics for the same reason. Without that line
the model reaches for lemma names it invents — `mul_nonneg_mul_nonneg`,
`nonneg_of_square` — and almost never for the automation that closes these
goals.

## The run

Eight steps to `Succeeded` on one L4 for `rollout`, one L4 for `learner`, eight
CPU replicas for `reward` and two for `advantage`. Read back from the Retained
`metrics` channel:

| step | mean reward | proved / 136 | groups with no gradient |
|---|---|---|---|
| 0 | 0.362 | 25 | 3 |
| 1 | 0.356 | 26 | 4 |
| 2 | 0.343 | 21 | 3 |
| 3 | 0.367 | 25 | 4 |
| 4 | 0.389 | 25 | 4 |
| 5 | 0.396 | 25 | 5 |
| 6 | 0.444 | 29 | 5 |
| 7 | 0.471 | 29 | 7 |

Mean reward rises from 0.362 to 0.471, and steps 2 through 7 increase
monotonically. Six values land in ascending order by chance with probability
1/720, so the trend is unlikely to be noise. Eight steps is still a short run,
and the honest claim is a trend in the right direction rather than a converged
result.

What moved is worth reading carefully, because the proved count barely changed:
25 to 29 of 136. Total reward per step is `1.0 * proved` plus whatever the
remaining completions earned, so the non-proved mass rose from 24.2 to 35.1
while proofs added four. The model learned mostly to emit **well-formed tactics
that fail** rather than to prove more theorems, moving from the 0.0 and 0.1
tiers into the 0.4 tier. That is real progress up the reward the graph defines,
and it is not the same thing as learning to prove.

Groups with no gradient also rose, 3 to 7. As the model concentrates on one
tier, more groups become uniform within themselves, so the gradient thins as
the reward improves. A longer run would want more tiers or harder statements.

## What else falls out of declaring the edge

**The sandbox.** A Lean proof is a program, and `reward` executes model-written
programs. Because the graph declares which operations talk to which, the
controller writes a NetworkPolicy per edge: a reward pod may reach the
coordinator and the pods that produce for it, and nothing else. The isolation
is a consequence of the declaration.

**Barriers only where GRPO needs them.** `completions` is Pipelined, so nothing
waits; a completion is verified while the next is still being sampled. GRPO must wait
in exactly two places: for all `G` rewards of a statement, and for every
statement before a step. Both are counted in application code, because
`Materialized` seals on producer completion and in a training loop no operation
ever completes.

**Wait for the sidecar before consuming.** A worker that starts first takes a
segment, fails its request to a port nothing is listening on, and exits. The
container restarts inside a pod that is still alive, and the coordinator
returns in-flight segments to the pending set only when a pod is gone, so those
records are never redelivered. The worker therefore dials the sidecar port and
blocks until it answers, which is exact because the sidecar binds only after
the weights are on the device.

## The rule this example earns

An earlier version of this reward read `0/64` with every group degenerate,
which looks like a 2B model failing at proofs. Categorizing the completions
showed 67% truncated and 0% with a wrong theorem header: the prompt asked the
model to restate the goal inside a 256-token budget, and the header consumed
it. `unexpected end of input` was in the error output all along.

**When a verifiable reward reads zero, suspect the harness before the model.**
A reward that cannot distinguish a wrong proof from one cut off mid-proof is
not yet a reward. Two things follow, and both are in the prompt rather than the
graph: ask for the tactic alone, since the statement is already known to the
graph; and budget tokens against the longest thing the model must emit.

## Running it

```sh
go test ./examples/grpo-lean/
```

Three images. The worker image from the repository `Dockerfile`, which contains
`/grpo-lean`. The sidecar image from [examples/sidecar](../sidecar), shared with
[examples/grpo-gemma](../grpo-gemma). And a reward image carrying a Mathlib
checkout with prebuilt `.olean` files, which `lake exe cache get` fetches in
75 s — against hours to compile — for a 3.9 GB image. Pin the toolchain to the
one the statements target; a mismatch fails proofs that are correct.

`problems.go` holds 17 statements inline so the example runs without a dataset
download. Every statement was compiled against Mathlib v4.8.0 with a reference
tactic before being added, because a statement that does not parse scores zero
for every completion and reads as a model failure. The whole set is used rather
than a selection from it: choosing which goals to train on by how the reward
comes out is how a measurement stops measuring anything.

One decision worth knowing about. The reward scores the **first line** of the
completion after stripping any code fence. Over 136 completions the first line
and the full indented block scored identically, so nothing is lost, and taking
the whole completion would let trailing prose fail an otherwise correct proof.
