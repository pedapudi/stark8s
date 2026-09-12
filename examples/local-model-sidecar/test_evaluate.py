import importlib.util
from pathlib import Path
import unittest
from unittest import mock


directory = Path(__file__).parent
spec = importlib.util.spec_from_file_location("evaluate", directory / "evaluate.py")
evaluate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evaluate)


class EvaluateTest(unittest.TestCase):
    def test_constraint_scoring_uses_words(self):
        constraints = {"exact_words": 3, "includes": ["the"], "excludes": ["cat"]}
        self.assertEqual(
            evaluate.score("the theatre opens", constraints),
            {"exact_words": True, "includes:the": True, "excludes:cat": True},
        )
        self.assertFalse(evaluate.score("theatre opens", constraints)["includes:the"])

    def test_checked_in_cases_are_disjoint_and_well_formed(self):
        cases = evaluate.load_cases(directory / "heldout.jsonl")
        self.assertEqual(len({case["id"] for case in cases}), len(cases))
        for case in cases:
            self.assertTrue(case["prompt"])
            self.assertTrue(case["constraints"])

    def test_zero_rate_constraints_remain_in_report(self):
        cases = [{"prompt": "prompt", "constraints": {"includes": ["missing"]}}]
        with mock.patch.object(evaluate, "generate", return_value=["answer"]):
            result = evaluate.evaluate("unused", cases, 1, 0)
        self.assertEqual(result["constraint_rates"], {"includes:missing": 0.0})


if __name__ == "__main__":
    unittest.main()
