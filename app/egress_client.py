"""Client for submitting egress messages to the handler's synchronous endpoint.

Egress no longer flows through BBMB: the worker (and the app.main_reply CLI)
POST each egress message to the handler and learn the delivery outcome instead
of publishing fire-and-forget. This is the shared transport both use.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

# The handler binds this loopback endpoint by default (see the handler's
# DefaultEgressHTTPHost/Port). A shared default means no extra config is needed
# when worker and handler run on the same host.
DEFAULT_HANDLER_EGRESS_URL = "http://127.0.0.1:9467/egress"
DEFAULT_SUBMIT_TIMEOUT_SECONDS = 30


def submit_egress(
    url: str,
    payload: dict[str, object],
    *,
    timeout_seconds: float = DEFAULT_SUBMIT_TIMEOUT_SECONDS,
) -> tuple[int, dict[str, object]]:
    """POST an egress payload to the handler.

    Returns (http_status, response_body). http_status is 0 when the handler was
    unreachable (connection refused, DNS, timeout) — i.e. it never processed the
    message and a retry may succeed.
    """
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return response.status, _decode_body(response.read())
    except urllib.error.HTTPError as error:
        return error.code, _decode_body(error.read())
    except urllib.error.URLError as error:
        return 0, {"reason": f"egress endpoint unreachable: {error.reason}"}


def _decode_body(raw: bytes) -> dict[str, object]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw.decode("utf-8", "replace"))
    except json.JSONDecodeError:
        return {"reason": raw.decode("utf-8", "replace")}
    return parsed if isinstance(parsed, dict) else {"reason": str(parsed)}
