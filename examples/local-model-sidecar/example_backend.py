"""Deterministic backend used to exercise the sidecar protocol on a CPU."""

checkpoint = "initial"


def generate(prompt: str, count: int, seed: int) -> list[str]:
    return [f"{prompt} [{checkpoint}:{seed + index}]" for index in range(count)]


def load(value: str) -> None:
    global checkpoint
    checkpoint = value


def step(samples: list[dict], step_index: int) -> dict:
    global checkpoint
    checkpoint = f"checkpoint-{step_index:03d}"
    if samples:
        objective = sum(float(sample["advantage"]) for sample in samples) / len(samples)
    else:
        objective = 0.0
    return {"checkpoint": checkpoint, "objective": objective, "kl": 0.0}
