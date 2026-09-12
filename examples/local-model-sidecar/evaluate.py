"""Evaluate a local sidecar on a fixed JSON Lines case set."""

import argparse
import json
import re
import urllib.request
from pathlib import Path

REQUEST_TIMEOUT_SECONDS = 600


def words(text):
    return re.findall(r"\b[\w']+\b", text.casefold())


def score(text, constraints):
    tokens = words(text)
    checks = {}
    if "exact_words" in constraints:
        checks["exact_words"] = len(tokens) == int(constraints["exact_words"])
    for required in constraints.get("includes", []):
        checks[f"includes:{required}"] = required.casefold() in tokens
    for forbidden in constraints.get("excludes", []):
        checks[f"excludes:{forbidden}"] = forbidden.casefold() not in tokens
    return checks


def generate(endpoint, prompt, count, seed):
    request = urllib.request.Request(
        endpoint.rstrip("/") + "/generate",
        data=json.dumps({"prompt": prompt, "n": count, "seed": seed}).encode(),
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        return json.load(response)["completions"]


def load_checkpoint(endpoint, checkpoint):
    request = urllib.request.Request(
        endpoint.rstrip("/") + "/load",
        data=json.dumps({"checkpoint": checkpoint}).encode(),
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS):
        pass


def evaluate(endpoint, cases, count, seed):
    totals = {}
    counts = {}
    passed = 0
    samples = 0
    for case_index, case in enumerate(cases):
        completions = generate(endpoint, case["prompt"], count, seed + case_index * count)
        for completion in completions:
            checks = score(completion, case["constraints"])
            for name, result in checks.items():
                totals[name] = totals.get(name, 0) + int(result)
                counts[name] = counts.get(name, 0) + 1
            passed += int(all(checks.values()))
            samples += 1
    return {"samples": samples, "all_constraints": passed / max(samples, 1),
            "constraint_rates": {name: totals[name] / counts[name]
                                 for name in sorted(counts)}}


def load_cases(path):
    with Path(path).open() as source:
        return [json.loads(line) for line in source if line.strip()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("cases")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8100")
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--checkpoint")
    arguments = parser.parse_args()
    if arguments.checkpoint:
        load_checkpoint(arguments.endpoint, arguments.checkpoint)
    print(json.dumps(evaluate(arguments.endpoint, load_cases(arguments.cases),
                              arguments.samples, arguments.seed), sort_keys=True))
