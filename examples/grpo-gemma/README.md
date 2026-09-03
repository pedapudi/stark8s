# grpo-gemma: GRPO over a real model, as a Workload

The graph is [examples/grpo](../grpo) unchanged. What differs is inside two of
the pods: `rollout` and `learner` each pair the SDK worker with a sidecar that
holds Gemma 4 E2B on an accelerator, behind a localhost HTTP contract. The
worker owns the loop protocol and does all the emitting; the sidecar owns the
weights and answers three requests.

```
prompts --batch--> rollout --completions--> reward --scored--> advantage
                      ^      (GPU+model)    (CPU only)              |
                      \-------------weights------------\       advantages
                         (Broadcast, feedback: a URI)    \          |
                                                          \---- learner
                                                       metrics (Retained)
                                                                (GPU+model)
```

`reward` has no sidecar and no accelerator, because scoring a tour is
arithmetic. That is the whole reason it is a separate operation with its own
replica count.

## The task

Each step draws four Euclidean traveling-salesman instances of eight cities,
seeded from the slot name and the epoch, and asks for a tour. The reward is

```
0.3 + 0.7 * min(1, reference_length / tour_length)
```

for a parseable permutation of all eight cities, and zero otherwise. The
reference is nearest-neighbor followed by 2-opt, computed in the same Go
binary that scores the completion. A model that emits a valid tour therefore
starts around 0.3 and earns the rest by shortening it.

The parser takes the **last** eight integers in the completion, so reasoning
before the answer is not punished. An earlier version read the first eight and
scored perfect tours at zero whenever the model thought out loud first.

## What ran

One L4 for `rollout`, one L4 for `learner`, four CPU replicas for `reward`, two
for `advantage`; four prompts per step, eight completions per prompt, twelve
steps. Read back from the Retained `metrics` channel after the Workload reached
`Succeeded`:

| step | mean reward | degenerate groups |
|---|---|---|
| 0 | 0.715 | 0/4 |
| 1 | 0.733 | 0/4 |
| 2 | 0.656 | 0/4 |
| 3 | 0.711 | 0/4 |
| 4 | 0.719 | 0/4 |
| 5 | 0.773 | 0/4 |
| 6 | 0.731 | 0/4 |
| 7 | 0.774 | 0/4 |
| 8 | 0.704 | 0/4 |
| 9 | 0.750 | 0/4 |
| 10 | 0.768 | 0/4 |
| 11 | 0.751 | 0/4 |

**This does not demonstrate learning.** The fitted slope is +0.005 per step
against a step-to-step standard deviation of 0.034, and each step draws fresh
instances, so instance difficulty alone moves the mean by more than the trend
does. Twelve steps of rank-16 LoRA at 1e-5 over 32 completions is far too
little signal to separate from that noise. The measurement the run does
support is mechanical: every stage executed, the reward discriminated within
every group at every step, and the loop closed twelve times.

The degenerate-group count is the number to watch. A group whose completions
all score the same produces `(r - mean) / std = 0` for every member and
contributes no gradient; GRPO can look healthy — pods up, epochs advancing,
metrics arriving — while training on nothing. Across these twelve steps the
reward separated the eight completions of every prompt every time. It is not
guaranteed to: a later three-step run on the same task scored 1/4 degenerate
at two of its three steps.

## Weights on the edge

The `weights` channel is `Broadcast`, `Asynchronous`, `maxEpochs: 12`, and it
carries a checkpoint URI rather than the checkpoint. `Emit` marshals a value
and buffers it whole on both sides, and a model is gigabytes.

The split follows what each artifact is. Base weights are an immutable
dependency, so they are baked into the sidecar image: that pins the exact model
by image digest, survives a hub outage or rate limit, and needs no external
egress at run time. The cost is a 9.6 GB layer and a first pull of several
minutes. Checkpoints are mutable artifacts that must outlive the pod, so the
trainer writes the LoRA adapter, 97 MB, to object storage and puts the URI on
the channel. The generator reads the URI off the feedback edge
and reloads.

That reload was confirmed directly rather than inferred. A three-step run with
the generator sidecar's log streamed for its whole life printed:

```
[generator] model loaded
[generator] loaded gs://.../grpo-gemma/step-000
[generator] loaded gs://.../grpo-gemma/step-001
```

Two loads for three steps is the correct count: epoch 0 samples from the base
model, and the checkpoint emitted at the epoch bound is dropped.

## Wait for the sidecar before consuming

The worker dials the sidecar port and blocks until the dial succeeds. The
sidecar binds only after the weights are on the accelerator, so that is an
exact readiness signal, and the wait is not cosmetic.

Without it the worker starts first, takes a segment, fails its first request to
a port nothing is listening on, and exits. The container restarts inside a pod
that is still alive, and the coordinator returns in-flight segments to the
pending set only when a pod is gone. A restarted container leaves the pod
alive, so those segments are never redelivered. The graph then sits at full
replica count, every pod `Running`, every sidecar loaded, and no error
anywhere. The accelerator holds at 0% utilization, because the records that
would have driven the step are held by a pod that no longer remembers them.

Any operation whose container can outlive its own process needs the same
treatment until in-flight recovery keys on something narrower than pod
liveness.

## Running it

```sh
go test ./examples/grpo-gemma/
```

For the cluster run, build two images:

- the worker image from the repository `Dockerfile`, which contains
  `/grpo-gemma`;
- the sidecar image from [examples/sidecar](../sidecar), which installs
  PyTorch, transformers and peft and bakes `google/gemma-4-E2B-it`. The prompt
  arrives on the `/generate` request, so the same image serves
  [examples/grpo-lean](../grpo-lean).

Then set `CHECKPOINT_PREFIX` in `workload.yaml` to a bucket the pods can write,
and apply. The manifest requests `nvidia-l4` for `rollout` and `learner` only.

Two model-side details that cost a run each. LoRA must target the text decoder
alone. The vision and audio towers use a linear layer peft cannot wrap, and a
text-only forward never reaches them, so a regex over every linear module
yields parameters with no gradient path and a loss that does not require grad.
And `generate(num_return_sequences=n)` right-pads a group to its longest
member, so log-probabilities must be masked at the first end-of-sequence token;
summing over the padding makes the gradient push on pad predictions and held-out
reward falls while training reward rises.
