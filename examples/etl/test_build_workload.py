import importlib.util
import json
from pathlib import Path
import unittest


directory = Path(__file__).parent
spec = importlib.util.spec_from_file_location("build_workload", directory / "build_workload.py")
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class WorkloadBuilderTest(unittest.TestCase):
    def test_builder_matches_manifest(self):
        expected = json.loads((directory / "workload.json").read_text())
        self.assertEqual(builder.workload(), expected)


if __name__ == "__main__":
    unittest.main()
