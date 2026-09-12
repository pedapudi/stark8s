"""Serve a local generation and training backend over loopback HTTP."""

import importlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


backend = importlib.import_module(os.environ.get("BACKEND_MODULE", "example_backend"))
backend_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        try:
            length = int(self.headers.get("content-length", "0"))
            body = json.loads(self.rfile.read(length) or b"{}")
            with backend_lock:
                if self.path == "/generate":
                    result = {"completions": backend.generate(
                        body["prompt"], int(body["n"]), int(body.get("seed", 0))
                    )}
                elif self.path == "/load":
                    backend.load(body["checkpoint"])
                    self.send_response(204)
                    self.end_headers()
                    return
                elif self.path == "/step":
                    result = backend.step(body["samples"], int(body["step"]))
                else:
                    self.send_error(404)
                    return
            payload = json.dumps(result).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception as exc:
            payload = str(exc).encode()[:1024]
            self.send_response(500)
            self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8100"))
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
