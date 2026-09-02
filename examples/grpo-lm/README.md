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
requests, the two sidecars in `sidecar/`, and the gated model pull have never
been run. They are written from the documented interfaces. Read them as a
starting point.

## Choosing the model, and why the task follows from it

`google/gemma-3-270m-it`, with `gemma-3-1b-it` as the step-up. Both are
text-only, 32K context, BF16, and **gated** — the download needs accepted
Gemma terms and a token, which is why the manifest mounts one as a secret.

The task is instruction-following with programmatically checkable constraints,
and that choice is forced by the model rather than picked for convenience.

GRPO's gradient is the *spread within a group*. If every completion in a group
scores the same, the standard deviation is zero, every advantage is zero, and
the update is exactly nothing. A task the model always fails gives no signal
for the same reason as one it always passes.

Small Gemmas cannot do grade-school maths. The 1B card reports MGSM **2.04**
and carries no GSM8K row at all; the 270M reports no maths benchmark and sits
at chance on ARC-c (28.2) and WinoGrande (52.3). Point GRPO at GSM8K with one
of these and nearly every group returns all zeros. The run looks perfectly
healthy — pods up, epochs advancing, metrics flowing — and learns nothing.

What these models *are* good at is following instructions: IFEval **51.2** for
270M, **80.2** for 1B. That is the range GRPO wants, because a model that gets
some constraints right and others wrong produces groups with spread.

So the reward here is a program (`verify.go`): count the bullets, check the
word budget, parse the JSON and compare its keys, test the exact suffix. No
judge and no reward model enters the graph, and scoring the *fraction* of
constraints met keeps rewards spread out within a group.

The learner reports `degenerateGroups` every step for exactly this reason. If
it stays at the group count, the task is mismatched to the model and no amount
of waiting will help.

## What the real model forces to change

**The weights do not travel on the channel.** 270M parameters at bf16 is about
540 MB, and `Emit` JSON-marshals a value and buffers it whole on both sides.
The learner writes a checkpoint to shared storage and broadcasts a reference
of about a hundred bytes; rollout loads it. `sdk.EmitBlob` would let the bytes
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
with accelerator quota, a bucket for checkpoints with Workload Identity, an
`hf-token` secret, and a sidecar image carrying vLLM and Transformers. Then
watch it:

```sh
kubectl -n <namespace> port-forward svc/grpo-lm-coordinator 18080:8080
curl -s 'http://127.0.0.1:18080/channels/metrics/records?after=0'
```

`metrics` carries the mean reward, the per-task breakdown, the KL, the
checkpoint URI and the degenerate-group count for every step, and is
`Retained`, so it is readable while training is still in flight.
