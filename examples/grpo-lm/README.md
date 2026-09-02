# grpo-lm: post-training a language model with GRPO

[examples/grpo](../grpo) with the table replaced by a real model. The graph is
the same shape; what changed is inside the pods.

```
prompts --batch--> rollout --completions--> reward --scored--> advantage
                      ^                                            |
                      \--------------weights--------------\   advantages
                         (Broadcast, feedback: a URI)      \        |
                                                            \-- learner
                                                        metrics (Retained)
```

## What is verified and what is not

**Verified, by `go test ./examples/grpo-lm/`:** the graph. The coordinator,
the segments over HTTP, the Hash partition that forms a group, the Broadcast
feedback edge carrying the checkpoint reference, the two counted barriers, the
epoch arithmetic that ends the run, and every reward checker. The test drives
the whole workload on CPU against a stub model that improves each time it is
told to load a checkpoint, so a rising reward proves the reference travelled
the edge and was acted on.

```
reward 0.174 -> 0.854 over 6 steps; 10 checkpoint loads; degenerate groups 2/4 -> 0/4
```

Ten loads is five delivered checkpoints across two rollout replicas; the sixth
is the one `maxEpochs` drops.

**Not verified:** everything that needs an accelerator. The manifest's GPU
requests, the two sidecars in `sidecar/`, and the model pull have never been
run. They are written from the documented interfaces. Read them as a starting
point.

## Choosing the model, and why the task follows from it

`google/gemma-4-E2B-it`: 2.3B effective parameters (5.1B with the per-layer
embedding tables), 128K context, BF16, **Apache 2.0 and ungated** — no token
and no terms click, which is why nothing here mounts a secret. E4B and 12B are
the steps up. Gemma 4 has no 0.5B-class member; E2B is the smallest.

The selection criterion is not capability. GRPO's gradient is the *spread
within a group*: if every completion scores the same, the standard deviation
is zero, every advantage is zero, and the update is exactly nothing. That
fails at **both** ends. A task the model never passes gives all-zero groups; a
task it always passes gives all-one groups. Either way the run looks perfectly
healthy — pods up, epochs advancing, metrics flowing — and learns nothing.

Which end you are near depends entirely on the model, and the generation gap
here is large. Gemma 3 at this size cannot do school maths at all: the 1B card
reports MGSM **2.04** and carries no GSM8K row, and the 270M sits at chance on
ARC-c (28.2) and WinoGrande (52.3). Gemma 4 E2B reports **AIME 2026 37.5%**,
GPQA Diamond 43.4% and LiveCodeBench v6 44.0% — ahead of Gemma 3 27B on all
three. So the risk inverts: with Gemma 3 a maths task starved the gradient
because it was too hard; with Gemma 4 a grade-school one plausibly starves it
because it is too easy.

This is why the reward here is a set of *tunable* checkable constraints rather
than a fixed benchmark. Bullet counts, word budgets, exact suffixes, JSON keys
— each is decided by reading the completion (`verify.go`), so no judge and no
reward model enters the graph, and the difficulty is a dial rather than a
property of a dataset. Scoring the fraction of constraints met, rather than
all-or-nothing, keeps a group's rewards spread out.

## Calibrate before you train

Do not guess the band; measure it. The learner already reports
`degenerateGroups` in every metrics record — the number of groups whose
completions all scored alike and therefore contributed no gradient.

Run one step and read it. If it sits at the group count, the task set is
mismatched, and `perTask` in the same record says which way: a task averaging
near 0 is too hard, near 1 too easy. Add or drop constraints and run again.
In the CPU test the counter falls 2/4 → 0/4 as the stub improves, which is the
shape you want to see.

## Memory, honestly

E2B is ~10 GB of weights at bf16. A full fine-tune also wants a frozen
reference copy and AdamW state — two fp32 moments plus fp32 master weights,
roughly another 60 GB — so it does not fit the single L4 the manifest asks
for. Two ways out, and the manifest assumes the first:

- **LoRA on the policy.** Optimizer state shrinks to the adapter, and the
  frozen reference comes free: it is the base model with the adapter disabled,
  so no second copy is resident. The checkpoint that travels the weights
  channel becomes an adapter rather than a full model.
- **A larger accelerator** (A100/H100 class) for a full fine-tune.

The `nvidia-l4` node selector in the manifest is a placeholder, not a
measurement.

## What the real model forces to change

**The weights do not travel on the channel.** E2B is ~10 GB at bf16 — and even
a LoRA adapter is tens of megabytes — while `Emit` JSON-marshals a value and
buffers it whole on both sides. The learner writes a checkpoint to shared
storage and broadcasts a reference of about a hundred bytes; rollout loads it. `sdk.EmitBlob` would let the bytes
travel pod to pod and remove the shared-storage requirement.

**The model runs beside the worker, not inside it.** The SDK is Go and there
is no Python client. Each pod runs the worker plus a sidecar behind a
localhost HTTP contract:

```
POST /load      {"checkpoint": uri}                        -> 204
POST /generate  {"prompt": str, "n": int, "seed": int}     -> {"completions": [...]}
POST /step      {"step": int, "samples": [...]}            -> {"checkpoint", "objective", "kl"}
```

The worker stays the SDK client and keeps the loop protocol. `engine.go` has
the interface and the HTTP clients; the test substitutes a stub. If this
pattern recurs, a thin Python SDK client would be the better answer — the
coordinator protocol is HTTP and JSON — but it would have to reimplement the
epoch and completion protocol, which is the subtle part.

**Reloading the engine dominates a step.** Every rollout replica loads a fresh
checkpoint after every update. A real system swaps weights into the running
inference engine in place; `sidecar/generate.py` does the simple, slow thing
and says so.

## Running it

```sh
go test ./examples/grpo-lm/      # the graph, on CPU, against a stub
```

On a cluster you would need, none of which is set up here: a GPU node pool
with accelerator quota, a bucket for checkpoints with Workload Identity, and a
sidecar image carrying vLLM and Transformers. Then watch it:

```sh
kubectl -n <namespace> port-forward svc/grpo-lm-coordinator 18080:8080
curl -s 'http://127.0.0.1:18080/channels/metrics/records?after=0'
```

`metrics` carries the mean reward, the per-task breakdown, the KL, the
checkpoint URI and the degenerate-group count for every step, and is
`Retained`, so it is readable while training is still in flight.
