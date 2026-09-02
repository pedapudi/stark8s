"""Rollout sidecar: draw completions under the checkpoint the worker last loaded.

The Go worker beside this process links the SDK, owns the loop protocol and
does the emitting. This answers two requests and holds the model.

    POST /load      {"checkpoint": "gs://.../step-007"}   -> 204
    POST /generate  {"prompt": str, "n": int, "seed": int}
                    -> {"completions": [str, ...]}

UNVERIFIED. This has not been run against a GPU. The vLLM calls follow the
documented API; treat them as a starting point.
"""
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from vllm import LLM, SamplingParams

MODEL = os.environ.get("MODEL", "google/gemma-4-E2B-it")
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "256"))
TEMPERATURE = float(os.environ.get("TEMPERATURE", "1.0"))

# Sampling has to be hot enough that a group differs from itself. GRPO's
# gradient is the spread within a group: at temperature 0 every completion is
# identical, the standard deviation is zero and the update is exactly nothing.
llm = LLM(model=MODEL, dtype="bfloat16", gpu_memory_utilization=0.85)


def load(checkpoint: str) -> None:
    """Point the engine at a new checkpoint.

    Reloading the whole engine is the simple thing and the slow thing: it is
    where a rollout pod spends most of a step. A real system swaps the weights
    in place instead, which is the single biggest speedup available here.
    """
    global llm
    llm = LLM(model=checkpoint, dtype="bfloat16", gpu_memory_utilization=0.85)


def generate(prompt: str, n: int, seed: int) -> list[str]:
    params = SamplingParams(n=n, temperature=TEMPERATURE, top_p=0.95,
                            max_tokens=MAX_TOKENS, seed=seed)
    out = llm.generate([prompt], params)[0]
    return [c.text for c in out.outputs]


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["content-length"] or 0)) or b"{}")
        try:
            if self.path == "/load":
                load(body["checkpoint"])
                self.send_response(204)
                self.end_headers()
                return
            if self.path == "/generate":
                texts = generate(body["prompt"], int(body["n"]), int(body.get("seed", 0)))
                payload = json.dumps({"completions": texts}).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            self.send_error(404)
        except Exception as exc:  # surface the reason to the worker's log
            self.send_error(500, str(exc))

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(os.environ.get("PORT", "8100"))), Handler).serve_forever()
