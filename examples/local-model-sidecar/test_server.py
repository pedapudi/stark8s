import json
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
import urllib.request


directory = Path(__file__).parent
evaluate_spec = importlib.util.spec_from_file_location("evaluate", directory / "evaluate.py")
evaluate = importlib.util.module_from_spec(evaluate_spec)
evaluate_spec.loader.exec_module(evaluate)


class ServerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        env = os.environ.copy()
        env["PORT"] = "18181"
        cls.process = subprocess.Popen(
            [sys.executable, "server.py"], cwd=os.path.dirname(__file__), env=env
        )
        for _ in range(50):
            try:
                cls.post("/generate", {"prompt": "ready", "n": 1})
                return
            except OSError:
                time.sleep(0.02)
        cls.process.terminate()
        raise RuntimeError("sidecar did not start")

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()
        cls.process.wait(timeout=2)

    @classmethod
    def post(cls, path, body):
        request = urllib.request.Request(
            f"http://127.0.0.1:18181{path}",
            data=json.dumps(body).encode(),
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            return response.status, json.load(response) if response.status != 204 else None

    def test_generate_load_and_step(self):
        status, generated = self.post("/generate", {"prompt": "item", "n": 2, "seed": 7})
        self.assertEqual(status, 200)
        self.assertEqual(generated["completions"], ["item [initial:7]", "item [initial:8]"])

        request = urllib.request.Request(
            "http://127.0.0.1:18181/load",
            data=json.dumps({"checkpoint": "saved"}).encode(),
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            self.assertEqual(response.status, 204)

        _, trained = self.post("/step", {"step": 3, "samples": [{"advantage": 0.5}]})
        self.assertEqual(trained, {"checkpoint": "checkpoint-003", "objective": 0.5, "kl": 0.0})

    def test_heldout_evaluation_calls_sidecar(self):
        cases = evaluate.load_cases(directory / "heldout.jsonl")
        result = evaluate.evaluate("http://127.0.0.1:18181", cases, 2, 11)
        self.assertEqual(result["samples"], 4)
        self.assertEqual(result["all_constraints"], 1.0)


if __name__ == "__main__":
    unittest.main()
