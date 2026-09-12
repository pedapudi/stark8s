"""Generation and group-relative training for a caller-provided local model."""

import json
import os
import shutil
import tempfile
from pathlib import Path

MODEL_PATH = os.environ.get("MODEL_PATH")
if not MODEL_PATH:
    raise RuntimeError("MODEL_PATH must name a local causal language model")

CHECKPOINT_DIR = Path(os.environ.get("CHECKPOINT_DIR", "./checkpoints"))
DEVICE = os.environ.get("DEVICE", "cpu")
LEARNING_RATE = float(os.environ.get("LEARNING_RATE", "1e-6"))
KL_WEIGHT = float(os.environ.get("KL_WEIGHT", "0.02"))
MAX_NEW_TOKENS = int(os.environ.get("MAX_NEW_TOKENS", "128"))
TEMPERATURE = float(os.environ.get("TEMPERATURE", "1.0"))

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, local_files_only=True)
policy = AutoModelForCausalLM.from_pretrained(MODEL_PATH, local_files_only=True).to(DEVICE)
reference = AutoModelForCausalLM.from_pretrained(MODEL_PATH, local_files_only=True).to(DEVICE)
reference.eval()
for parameter in reference.parameters():
    parameter.requires_grad_(False)
optimizer = torch.optim.AdamW(policy.parameters(), lr=LEARNING_RATE)
training_step = -1


def load(checkpoint):
    """Restore policy and learner state from a published checkpoint."""
    global policy, optimizer, training_step
    checkpoint_path = Path(checkpoint)
    optimizer_path = checkpoint_path / "optimizer.pt"
    state_path = checkpoint_path / "training_state.json"
    if not optimizer_path.is_file() or not state_path.is_file():
        raise ValueError(f"checkpoint lacks learner state: {checkpoint}")
    policy = AutoModelForCausalLM.from_pretrained(checkpoint, local_files_only=True).to(DEVICE)
    optimizer = torch.optim.AdamW(policy.parameters(), lr=LEARNING_RATE)
    optimizer.load_state_dict(torch.load(
        optimizer_path, map_location=DEVICE, weights_only=True
    ))
    training_step = int(json.loads(state_path.read_text())["step"])


def generate(prompt, count, seed):
    policy.eval()
    inputs = tokenizer(prompt, return_tensors="pt").to(DEVICE)
    torch.manual_seed(seed)
    with torch.no_grad():
        sequences = policy.generate(
            **inputs,
            do_sample=True,
            temperature=TEMPERATURE,
            max_new_tokens=MAX_NEW_TOKENS,
            num_return_sequences=count,
            pad_token_id=tokenizer.eos_token_id,
        )
    prompt_tokens = inputs["input_ids"].shape[1]
    return tokenizer.batch_decode(sequences[:, prompt_tokens:], skip_special_tokens=True)


def completion_log_probability(model, prompt, completion):
    prompt_ids = tokenizer(prompt, return_tensors="pt").input_ids.to(DEVICE)
    completion_ids = tokenizer(
        completion, return_tensors="pt", add_special_tokens=False
    ).input_ids.to(DEVICE)
    if completion_ids.numel() == 0:
        return None
    token_ids = torch.cat([prompt_ids, completion_ids], dim=-1)
    logits = model(token_ids).logits[:, :-1, :]
    targets = token_ids[:, 1:]
    token_log_probabilities = torch.log_softmax(logits.float(), dim=-1)
    selected = token_log_probabilities.gather(-1, targets.unsqueeze(-1)).squeeze(-1)
    return selected[:, prompt_ids.shape[-1] - 1 :].sum(-1)


def step(samples, step_index):
    """Apply one update and atomically publish resumable learner state."""
    global training_step
    policy.train()
    optimizer.zero_grad()
    objective_total = 0.0
    divergence_total = 0.0
    used = 0
    for sample in samples:
        current = completion_log_probability(policy, sample["prompt"], sample["completion"])
        if current is None:
            continue
        with torch.no_grad():
            baseline = completion_log_probability(
                reference, sample["prompt"], sample["completion"]
            )
        advantage = torch.tensor(float(sample["advantage"]), device=DEVICE)
        ratio = torch.exp(current - current.detach())
        divergence = torch.exp(baseline - current) - (baseline - current) - 1
        objective = ratio * advantage - KL_WEIGHT * divergence
        (-objective).backward()
        objective_total += float(objective.detach())
        divergence_total += float(divergence.detach())
        used += 1
    if used:
        for parameter in policy.parameters():
            if parameter.grad is not None:
                parameter.grad /= used
        torch.nn.utils.clip_grad_norm_(policy.parameters(), 1.0)
        optimizer.step()
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint = CHECKPOINT_DIR / f"step-{step_index:06d}"
    if checkpoint.exists():
        raise FileExistsError(f"checkpoint already exists: {checkpoint}")
    staging = Path(tempfile.mkdtemp(prefix=".checkpoint-", dir=CHECKPOINT_DIR))
    try:
        policy.save_pretrained(staging)
        tokenizer.save_pretrained(staging)
        torch.save(optimizer.state_dict(), staging / "optimizer.pt")
        (staging / "training_state.json").write_text(
            json.dumps({"step": step_index}) + "\n"
        )
        os.replace(staging, checkpoint)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    training_step = step_index
    return {
        "checkpoint": str(checkpoint.resolve()),
        "objective": objective_total / max(used, 1),
        "kl": divergence_total / max(used, 1),
    }
