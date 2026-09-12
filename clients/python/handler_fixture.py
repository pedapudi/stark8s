"""Cross-language test fixture for the local worker runtime."""

import json
import sys

from stark8s_runtime import Client, handlers


def double(invocation):
    record = invocation["record"]
    value = json.loads(record["value"]) if isinstance(record["value"], str) else record["value"]
    return [{"channel": "output", "key": record["key"], "value": value * 2}]


def source(_invocation):
    return [{"channel": "input", "key": "key", "value": 21}]


mode = sys.argv[2] if len(sys.argv) > 2 else "sink"
Client(sys.argv[1]).run(handlers(source=source) if mode == "source" else handlers(on_record=double))
