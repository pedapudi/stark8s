"""Learner sidecar: one GRPO update, then write a checkpoint.

    POST /step {"step": int,
                "samples": [{"prompt": str, "completion": str, "advantage": float}]}
            -> {"checkpoint": str, "objective": float, "kl": float}

The advantages arrive already computed: the graph did that, because centring a
group is a partitioning problem and belongs where the group is. This applies
the clipped surrogate against a frozen reference and saves the result.

UNVERIFIED. This has not been run against a GPU.
"""
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

import torch
import torch.nn.functional as F
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL = os.environ.get("MODEL", "google/gemma-4-E2B-it")
PREFIX = os.environ["CHECKPOINT_PREFIX"].rstrip("/")
LR = float(os.environ.get("LR", "1e-6"))
CLIP = float(os.environ.get("CLIP", "0.2"))
BETA = float(os.environ.get("KL", "0.02"))

tok = AutoTokenizer.from_pretrained(MODEL)
policy = AutoModelForCausalLM.from_pretrained(MODEL, torch_dtype=torch.bfloat16).cuda()
reference = AutoModelForCausalLM.from_pretrained(MODEL, torch_dtype=torch.bfloat16).cuda()
reference.eval()
for p in reference.parameters():
    p.requires_grad_(False)
opt = torch.optim.AdamW(policy.parameters(), lr=LR)


def completion_logprobs(model, prompt_ids, completion_ids):
    """Sum of token log-probabilities of the completion, prompt tokens masked."""
    ids = torch.cat([prompt_ids, completion_ids], dim=-1)
    logits = model(ids).logits[:, :-1, :]
    targets = ids[:, 1:]
    logp = torch.log_softmax(logits.float(), dim=-1)
    tokenwise = logp.gather(-1, targets.unsqueeze(-1)).squeeze(-1)
    return tokenwise[:, prompt_ids.shape[-1] - 1:].sum(-1)


def step(samples, step_index):
    opt.zero_grad()
    total_obj, total_kl, n = 0.0, 0.0, 0
    for s in samples:
        prompt_ids = tok(s["prompt"], return_tensors="pt").input_ids.cuda()
        comp_ids = tok(s["completion"], return_tensors="pt",
                       add_special_tokens=False).input_ids.cuda()
        if comp_ids.numel() == 0:
            continue
        lp = completion_logprobs(policy, prompt_ids, comp_ids)
        with torch.no_grad():
            lp_ref = completion_logprobs(reference, prompt_ids, comp_ids)
            lp_old = lp.detach()
        adv = torch.tensor(float(s["advantage"]), device=lp.device)
        ratio = torch.exp(lp - lp_old)
        surrogate = torch.min(ratio * adv, ratio.clamp(1 - CLIP, 1 + CLIP) * adv)
        # k3 estimator: non-negative and lower variance than lp_ref - lp.
        kl = torch.exp(lp_ref - lp) - (lp_ref - lp) - 1
        (-(surrogate - BETA * kl)).backward()
        total_obj += float(surrogate.detach())
        total_kl += float(kl.detach())
        n += 1
    if n:
        for p in policy.parameters():
            if p.grad is not None:
                p.grad /= n
    torch.nn.utils.clip_grad_norm_(policy.parameters(), 1.0)
    opt.step()

    out = f"{PREFIX}/step-{step_index:03d}"
    policy.save_pretrained(out)
    tok.save_pretrained(out)
    return {"checkpoint": out,
            "objective": total_obj / max(n, 1),
            "kl": total_kl / max(n, 1)}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["content-length"] or 0)) or b"{}")
        if self.path != "/step":
            self.send_error(404)
            return
        try:
            payload = json.dumps(step(body["samples"], int(body["step"]))).encode()
        except Exception as exc:
            self.send_error(500, str(exc))
            return
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(os.environ.get("PORT", "8200"))), Handler).serve_forever()
