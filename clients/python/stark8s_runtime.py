"""Standard-library client for the local worker runtime."""

import json
import urllib.parse
import urllib.request


class Client:
    """Run application callbacks serially over local HTTP."""

    def __init__(self, base_url="http://127.0.0.1:8081"):
        self.base_url = base_url.rstrip("/")

    def _request(self, method, path, value=None):
        data = None if value is None else json.dumps(value).encode()
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request) as response:
            body = response.read()
        return json.loads(body) if body else None

    def run(self, handlers):
        """Connect, declare readiness, and dispatch callbacks until stopped."""
        session = self._request("POST", "/v1/connect", {})["sessionId"]
        self._request("POST", "/v1/ready", {"sessionId": session})
        while True:
            query = urllib.parse.urlencode({"sessionId": session})
            invocation = self._request("GET", "/v1/invocations?" + query)
            reply = {
                "sessionId": session,
                "invocationId": invocation["invocationId"],
            }
            handler = handlers.get(invocation["kind"])
            if handler is None and invocation["kind"] in ("epoch", "drain"):
                reply["outputs"] = []
            elif handler is None:
                reply["error"] = "no handler for " + invocation["kind"]
            else:
                try:
                    result = handler(invocation)
                    if invocation["kind"] == "snapshot":
                        reply["state"] = result
                    else:
                        reply["outputs"] = result or []
                except Exception as error:
                    reply["error"] = str(error)
            self._request("POST", "/v1/replies", reply)


def handlers(
    source=None,
    on_record=None,
    on_epoch=None,
    on_drain=None,
    on_tick=None,
    snapshot=None,
    restore=None,
):
    """Map named callbacks to protocol event names."""
    return {
        name: callback
        for name, callback in {
            "source": source,
            "record": on_record,
            "epoch": on_epoch,
            "drain": on_drain,
            "tick": on_tick,
            "snapshot": snapshot,
            "restore": restore,
        }.items()
        if callback is not None
    }
