# Model-backed group-relative training

This example runs one training pipeline through the shared runtime:

```text
prompts -> rollout -> reward -> advantage -> learner
              ^                              |
              +------ checkpoint reference --+
```

Rollout and learner workers call the loopback HTTP contract in
`../local-model-sidecar`. Reward and advantage workers use deterministic
constraint scoring. The learner stores a checkpoint through the backend and
broadcasts its reference; every rollout replica loads that reference before
acknowledging the feedback epoch.

The manifest expects a caller-built `model-sidecar:local` image and a
`model-training-data` persistent volume claim. The claim must contain the
model at `/data/model` and allow every model sidecar to read and write
`/data/checkpoints`. The repository contains no default model choice or
remote download.

The held-out task set is fixed and disjoint from generated training tasks.
Evaluation records include per-constraint rates, including constraints with
zero successes. Each rollout replica samples only the training slots assigned
to it by the batch channel, preventing duplicate groups when rollout scales
horizontally.

Run the graph protocol test with:

```sh
go test ./examples/model-training
```

The test starts the coordinator and segment protocol, uses two replicas for
rollout, reward, and advantage, and checks learner updates, checkpoint loads,
feedback epochs, scoring, and held-out evaluation with a deterministic model
fixture. The local model backend is an executable integration path, but it has
not been qualified for a particular model or training environment.
