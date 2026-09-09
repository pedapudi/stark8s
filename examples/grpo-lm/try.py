#!/usr/bin/env python3
"""Run the post-trained model.

Loads the base model, applies the adapter this example produced, and prints
what it writes. Pass --base to see the untrained model on the same prompts.

    pip install torch transformers peft
    python try.py --adapter ./step-029 "Write one sentence about a bakery. Use exactly 18 words."

The adapter is 92 MB. The base weights are whatever `--model` names; the
adapter was trained against google/gemma-4-E2B-it and will not load onto a
different base.
"""
import argparse, os, re, statistics, sys

# Every from_pretrained contacts the Hub to check for updates, even when all
# the files are cached. That costs a round trip and prints an unauthenticated
# request warning. Once the weights are local there is nothing to check.
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")

import torch
from transformers import AutoProcessor, AutoModelForMultimodalLM

DEFAULT_PROMPT = (
    "Write one sentence describing a bicycle repair shop.\n\n"
    "Follow every requirement:\n"
    "- use exactly 21 words in total\n"
    '- never use the word "and"\n'
    '- use the word "copper" somewhere\n')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("prompt", nargs="*", default=None,
                    help="one or more prompts; loads the model once for all of them")
    ap.add_argument("--model", default="google/gemma-4-E2B-it")
    ap.add_argument("--adapter", default=None,
                    help="path to a checkpoint directory; omit for the base model")
    ap.add_argument("--base", action="store_true", help="ignore --adapter")
    ap.add_argument("-n", type=int, default=8)
    ap.add_argument("--max-new-tokens", type=int, default=96)
    ap.add_argument("--repl", action="store_true",
                    help="load once, then keep asking for prompts")
    ap.add_argument("--compare", action="store_true",
                    help="show the untrained and post-trained models side by side")
    ap.add_argument("--online", action="store_true", help="let the Hub check for updates")
    a = ap.parse_args()
    if a.online:
        os.environ.pop("HF_HUB_OFFLINE", None)
    prompts = a.prompt or [DEFAULT_PROMPT]

    dev = "cuda" if torch.cuda.is_available() else "cpu"
    proc = AutoProcessor.from_pretrained(a.model)
    model = AutoModelForMultimodalLM.from_pretrained(
        a.model, dtype=torch.bfloat16 if dev == "cuda" else torch.float32, device_map=dev)
    # The adapter is a small set of extra weights over the frozen base, so
    # both policies can live in one process: enabling and disabling it costs
    # nothing, and the model only loads once.
    tuned = None
    if a.adapter and not a.base:
        from peft import PeftModel
        tuned = PeftModel.from_pretrained(model, a.adapter)

    def summarise(counts, target):
        """Raw spread, plus the spread of the error, because a word count only
        means something next to the number that was asked for."""
        lo, mid, hi = min(counts), statistics.median(counts), max(counts)
        line = f"     words [min {lo}, median {mid:g}, max {hi}]"
        if target:
            errs = sorted(abs(c - target) for c in counts)
            exact = sum(1 for c in counts if c == target)
            line += (f" · target {target}"
                     f" · |error| [min {errs[0]}, median {statistics.median(errs):g},"
                     f" max {errs[-1]}]"
                     f" · exact {exact}/{len(counts)}")
        return line

    def run(policy, prompt, tag):
        msgs = [{"role": "user", "content": prompt}]
        inp = proc.apply_chat_template(msgs, tokenize=True, return_dict=True,
                                       return_tensors="pt", add_generation_prompt=True,
                                       enable_thinking=False).to(dev)
        k = inp["input_ids"].shape[-1]
        with torch.no_grad():
            out = policy.generate(**inp, max_new_tokens=a.max_new_tokens, do_sample=True,
                                  temperature=1.0, top_p=0.95, top_k=64,
                                  num_return_sequences=a.n)
        print(f"\n--- {tag} ---")
        counts = []
        for i, o in enumerate(out):
            text = proc.decode(o[k:], skip_special_tokens=True).strip()
            counts.append(len(text.split()))
            print(f"{i+1:2}  [{counts[-1]:2} words]  {text}")
        m = re.search(r"exactly (\d+) words", prompt)
        print(summarise(counts, int(m.group(1)) if m else None))

    def ask(prompt):
        if a.compare and tuned is not None:
            with tuned.disable_adapter():
                run(tuned, prompt, "untrained")
            run(tuned, prompt, "post-trained")
        elif tuned is not None:
            run(tuned, prompt, f"post-trained  ({a.adapter})")
        else:
            run(model, prompt, "base model, untrained")

    for p in prompts:
        ask(p)
    if a.repl:
        print("\n# model stays loaded — type a prompt, or ctrl-d to quit", file=sys.stderr)
        while True:
            try:
                line = input("\nprompt> ").strip()
            except EOFError:
                break
            if line:
                ask(line)

if __name__ == "__main__":
    main()
