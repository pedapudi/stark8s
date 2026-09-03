# grpo-lean: why the stages want different machines

GRPO over a theorem prover. The reward is the Lean kernel: a proof compiles or
it does not, so there is no judge, no reward model, and nothing to hack.

This example exists for one reason, and it is not the learning — see
[examples/grpo](../grpo) for that. It is that the two expensive stages want
**different hardware and different replica counts**, and a graph is what lets
them have both.

```
prompts --batch--> rollout --completions--> reward --scored--> advantage
                      ^      (1x GPU)       (8x CPU)               |
                      \-------------weights------------\      advantages
                         (Broadcast, feedback: a URI)   \           |
                                                         \----- learner
                                                       metrics (Retained)
```

## The measurement

One L4 and eight CPUs, 64 completions per step, real Lean 4 with Mathlib:

| stage | hardware | cost |
|---|---|---|
| generation | GPU | **178 s** for 64 completions (2.8 s each) |
| verification | CPU | **419 s** serial (6.5 s each) |

Verification is more than twice the cost of generation, and it does not want
an accelerator at all. In one process those add:

```
monolith:  178 s generate  +  419 s verify  =  597 s per step
                              ^^^^^^^^^^^^ accelerator idle for 70% of the step
```

Verification is embarrassingly parallel — each completion is independent — so
as its own operation it takes a replica count:

```
workers   wall s  proofs/s  speedup  per-proof s
      1    418.9      0.15    1.00x        6.54
      2    176.2      0.36    2.38x        2.75
      4     94.2      0.68    4.45x        1.47
      8     72.9      0.88    5.74x        1.14
```

At eight-way the step becomes `178 + 73`, and because `completions` is
`Pipelined` rather than `Materialized` the two overlap — a completion is
verified while the next is still being sampled — so it tends toward
`max(178, 73)`. The accelerator stops waiting.

Nothing in that trade is available to a single process without hand-rolling a
work queue, a scheduler and a pool of machines that are not the machine the
model is on. That is the thing the graph replaces.

Two details behind the numbers. Scaling flattens after four workers (4.45x to
5.74x) because Lean is not purely CPU-bound at this size. And the 6.5 s is
almost entirely `import Mathlib`, identical warm or cold — a persistent REPL
with Mathlib already loaded would cut it, at the cost of putting state in an
operation that is otherwise stateless.

## What else falls out of declaring the edge

**The sandbox.** A Lean proof is a program, and `reward` executes model-written
programs. Because the graph declares which operations talk to which, the
controller writes a NetworkPolicy per edge: a reward pod may reach the
coordinator and the pods that produce for it, and nothing else. The isolation
is not added, it is a consequence of the declaration.

**The barriers, and only where GRPO needs them.** `completions` is Pipelined,
so nothing waits. The two places GRPO must wait — for all `G` rewards of
a prompt, and for every prompt before a step — are counted in application
code, because `Materialized` seals on producer completion and in a training
loop no operation ever completes.

## What this example does *not* show

**It does not learn — but the first measurement of that was wrong, and the
correction is the more useful result.**

Sampling `gemma-4-E2B-it` on eight Lean-Workbook problems, eight completions
each, and running all 64 through the kernel gave `0/64` and eight degenerate
groups out of eight. The obvious reading is that a 2B model cannot prove these.

Categorising the 64 completions says otherwise:

```
across 64 completions:
  truncated         43  (67%)
  header wrong       0  ( 0%)
  header ok         49  (77%)
```

The prompt asked the model to reproduce the theorem statement verbatim — up to
224 characters — and then prove it, inside a 256-token budget. The header alone
consumes most of that. Note the second row: whenever a theorem line was
emitted, it was character-perfect. The model was not failing at restatement, it
was running out of tokens mid-proof. `unexpected end of input` was in the error
output all along.

So `0/64` was a measurement of the harness rather than of the model. Two
changes follow, and both are in the prompt rather than the graph:

- **Ask for the tactic alone.** The statement is already known, so
  splice the generated tactic into it and compile that. Nothing is spent
  re-emitting a header, and 80 tokens is ample.
- **Budget tokens against the longest statement**, or truncation silently
  becomes the dominant failure mode.

The general rule this example earns: **when a verifiable reward reads zero,
suspect the harness before the model.** A reward that cannot distinguish "wrong
proof" from "cut off mid-proof" is not yet a reward.

The binary-reward warning still stands on its own merits. A proof compiles or
it does not, so a group collapses to all-zero or all-one and GRPO trains on
nothing while every pod, epoch and metric looks healthy. Watch the
degenerate-group count before the reward curve.

This example carries the scaling measurement. [examples/grpo](../grpo) carries
the learning result. They are separate because at 2B they cannot be the same
example.

## What is measured and what is not

Measured on hardware: the per-proof verification cost, the eight-way scaling
table above, and the 64-completion breakdown that overturned the `0/64`
reading. Provided but not run end to end: the full loop in `main.go`. The
generator and trainer sidecar is the one
[examples/grpo-gemma](../grpo-gemma) runs on a real accelerator, and that
example's twelve-step run is the evidence that the loop protocol closes.

## Running it

```sh
go test ./examples/grpo-lean/
```

Three images. The worker image from the repository `Dockerfile`, which
contains `/grpo-lean`. The sidecar image from [examples/sidecar](../sidecar),
shared with `grpo-gemma`. And a reward image carrying a Mathlib checkout with
prebuilt `.olean` files: `lake exe cache get` fetches them in 75 s, against
hours to compile, for a 3.9 GB image. Pin the toolchain to the one the
statements target; a mismatch fails proofs that are correct.

`problems.go` holds eight statements inline so the example runs without a
dataset download. The measured runs used Lean-Workbook statements in the same
shape, and swapping the source is a change to that file alone.
