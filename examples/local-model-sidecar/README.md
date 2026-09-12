# Local model sidecar protocol

The reinforcement-learning workload can keep orchestration in its main worker
and run generation and training in a second process in the same pod. The worker
calls three loopback HTTP endpoints:

- `POST /generate` accepts a prompt, sample count, and seed.
- `POST /load` replaces the checkpoint used for generation.
- `POST /step` applies one update and returns its checkpoint and metrics.

`server.py` implements the transport with the Python standard library. It
loads the module named by `BACKEND_MODULE`; that module must define `generate`,
`load`, and `step`. Requests are serialized because model backends commonly
mutate shared device state while loading or training.

`example_backend.py` is a deterministic CPU implementation for testing the
protocol. It does not train a model. Replace it with a backend that owns the
model, optimizer, and checkpoint storage required by the deployment.

`model_backend.py` is the model-backed implementation. It loads a local causal
language model, samples completions, applies one group-relative policy update,
and atomically publishes model, tokenizer, optimizer, and training-step state
after each update. Loading that directory restores learner state as well as
generation weights. Published step directories are immutable. The caller must
set `MODEL_PATH` to a local model directory and `CHECKPOINT_DIR` to durable shared
storage. The module performs no download and contains no default model choice.
Install its `torch` and `transformers` dependencies in a virtual environment,
then select it explicitly:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install torch transformers
MODEL_PATH=/models/local CHECKPOINT_DIR=/checkpoints \
  BACKEND_MODULE=model_backend python3 examples/local-model-sidecar/server.py
```

The repository tests the protocol and scoring on CPU with the deterministic
backend. The model-backed module has not been qualified against a particular
model, device, or training run. Its checkpoint path is the value that the
`examples/model-training` learner broadcasts on the feedback channel.

Run the protocol test with:

```sh
python3 -m unittest examples/local-model-sidecar/test_server.py
```

Bind the server only to loopback. A sidecar shares the pod network namespace,
so the main worker can reach it without exposing the service outside the pod.

## Fixed held-out evaluation

`evaluate.py` samples a fixed JSON Lines case set through `/generate` and
reports exact word-count, required-word, forbidden-word, and joint pass rates.
Each case has an explicit ID, prompt, and constraints. Keep this file disjoint
from training inputs so changes in case difficulty cannot be mistaken for a
policy change.

Run the checked-in cases against a sidecar with:

```sh
python3 examples/local-model-sidecar/evaluate.py \
  examples/local-model-sidecar/heldout.jsonl --samples 8 \
  --checkpoint /checkpoints/step-000010
```

The checked-in deterministic backend exercises the evaluation protocol. Its
scores do not establish model quality or learning. A training experiment must
measure the unchanged held-out set before and after loading a trained
checkpoint and report the sample counts with the rates.

Each training slot must enter the graph once. The batch channel assigns slots
to rollout replicas, and a replica samples only the slots it consumed. Sampling
every declared slot in every replica duplicates the training batch and changes
the effective group size. The `examples/model-training` protocol test runs two rollout
replicas against one Hash-partitioned batch and checks the resulting record
count.
