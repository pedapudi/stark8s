"""Rollout and learner sidecar for GRPO over a real model.

The Go worker beside this process links the SDK, owns the loop protocol and
does all the emitting. This holds the model and answers three requests.

  POST /generate {"prompt","n","max_new_tokens"} -> {"completions":[...]}
  POST /load     {"checkpoint": "gs://..."}   -> 204
  POST /step     {"samples":[...], "step":n}  -> {"checkpoint","objective","kl"}
"""
import json, os, threading, torch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from transformers import AutoProcessor, AutoModelForMultimodalLM

MODEL = os.environ.get("MODEL", "/models/gemma")     # baked into the image
ROLE  = os.environ.get("ROLE", "generator")
PREFIX = os.environ.get("CHECKPOINT_PREFIX", "")
LR    = float(os.environ.get("LR", "1e-5"))
CLIP  = float(os.environ.get("CLIP", "0.2"))
lock  = threading.Lock()

proc  = AutoProcessor.from_pretrained(MODEL)
model = AutoModelForMultimodalLM.from_pretrained(MODEL, dtype=torch.bfloat16, device_map="cuda")
EOS   = proc.tokenizer.eos_token_id
print(f"[{ROLE}] model loaded", flush=True)

SUF = ("q_proj","k_proj","v_proj","o_proj","gate_proj","up_proj","down_proj")
def text_targets(m):
    # The vision and audio towers use Gemma4ClippableLinear, which peft cannot
    # wrap and which a text-only forward never reaches.
    return [n for n, mod in m.named_modules()
            if isinstance(mod, torch.nn.Linear) and n.split(".")[-1] in SUF
            and "vision" not in n and "audio" not in n]

policy, opt = model, None
if ROLE == "trainer":
    from peft import LoraConfig, get_peft_model
    policy = get_peft_model(model, LoraConfig(r=16, lora_alpha=32, lora_dropout=0.0,
        bias="none", target_modules=text_targets(model), task_type="CAUSAL_LM"))
    policy.print_trainable_parameters()
    opt = torch.optim.AdamW([p for p in policy.parameters() if p.requires_grad], lr=LR)

def gcs_put(local_dir, uri):
    from google.cloud import storage
    b, _, pfx = uri[5:].partition("/")
    bucket = storage.Client().bucket(b)
    for f in os.listdir(local_dir):
        bucket.blob(f"{pfx}/{f}").upload_from_filename(os.path.join(local_dir, f))

def gcs_get(uri, local_dir):
    from google.cloud import storage
    b, _, pfx = uri[5:].partition("/")
    os.makedirs(local_dir, exist_ok=True)
    for blob in storage.Client().bucket(b).list_blobs(prefix=pfx):
        blob.download_to_filename(os.path.join(local_dir, os.path.basename(blob.name)))

def generate(text, n, max_new_tokens):
    # The prompt arrives on the request. Building it here as well would put a
    # second copy beside the worker's, and the trainer scores completions
    # against the worker's copy: the two drift and nothing reports it.
    msgs = [{"role": "user", "content": text}]
    inp = proc.apply_chat_template(msgs, tokenize=True, return_dict=True,
        return_tensors="pt", add_generation_prompt=True, enable_thinking=False).to("cuda")
    k = inp["input_ids"].shape[-1]
    with torch.no_grad():
        out = policy.generate(**inp, max_new_tokens=max_new_tokens, do_sample=True, temperature=1.0,
                              top_p=0.95, top_k=64, num_return_sequences=n)
    return [proc.decode(o[k:], skip_special_tokens=True) for o in out]

def load_ckpt(uri):
    from peft import PeftModel
    global policy
    d = "/tmp/ckpt"; os.system(f"rm -rf {d}"); gcs_get(uri, d)
    policy = PeftModel.from_pretrained(model, d)
    print(f"[{ROLE}] loaded {uri}", flush=True)

def comp_mask(ids):
    # generate() right-pads a group to its longest member; summing log-probs
    # over the padding makes the gradient push on pad predictions.
    m = torch.ones_like(ids, dtype=torch.float)
    hit = (ids == EOS).nonzero()
    if len(hit): m[hit[0, 0] + 1:] = 0.0
    return m

def train_step(samples, step):
    opt.zero_grad(); n = max(len(samples), 1)
    for s in samples:
        msgs = [{"role": "user", "content": s["prompt"]}]
        pin = proc.apply_chat_template(msgs, tokenize=True, return_dict=True,
            return_tensors="pt", add_generation_prompt=True, enable_thinking=False).to("cuda")
        cid = proc.tokenizer(s["completion"], return_tensors="pt",
                             add_special_tokens=False).input_ids.to("cuda")
        if cid.numel() == 0: continue
        ids = torch.cat([pin["input_ids"], cid], dim=-1)
        logits = policy(input_ids=ids).logits[:, :-1, :]
        lp = torch.log_softmax(logits.float(), -1).gather(-1, ids[:, 1:].unsqueeze(-1)).squeeze(-1)
        lp = (lp[:, pin["input_ids"].shape[-1]-1:] * comp_mask(cid[0]).unsqueeze(0)).sum()
        ratio = torch.exp(lp - lp.detach())
        a = float(s["advantage"])
        (-torch.min(ratio * a, ratio.clamp(1-CLIP, 1+CLIP) * a) / n).backward()
    torch.nn.utils.clip_grad_norm_([p for p in policy.parameters() if p.requires_grad], 1.0)
    opt.step()
    uri = f"{PREFIX}/step-{step:03d}"
    policy.save_pretrained("/tmp/out"); gcs_put("/tmp/out", uri)
    return {"checkpoint": uri, "objective": 0.0, "kl": 0.0}

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["content-length"] or 0)) or b"{}")
        try:
            with lock:
                if self.path == "/generate":
                    r = {"completions": generate(body["prompt"], int(body["n"]),
                                                 int(body.get("max_new_tokens", 64)))}
                elif self.path == "/load":
                    load_ckpt(body["checkpoint"]); r = None
                elif self.path == "/step":
                    r = train_step(body["samples"], int(body["step"]))
                else:
                    self.send_error(404); return
            if r is None: self.send_response(204); self.end_headers(); return
            p = json.dumps(r).encode()
            self.send_response(200); self.send_header("content-type","application/json")
            self.send_header("content-length", str(len(p))); self.end_headers(); self.wfile.write(p)
        except Exception as e:
            # The reason goes in the body. send_error's message argument lands
            # in the HTTP status line, so a multi-line exception there — a
            # storage 403, say, whose text is a JSON document — reaches the
            # caller as a malformed-header error instead of as the real fault.
            import traceback; traceback.print_exc()
            p = str(e).encode()[:1024]
            self.send_response(500); self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(p)))
            self.end_headers(); self.wfile.write(p)
    def log_message(self, *_): pass

ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("PORT","8100"))), H).serve_forever()
