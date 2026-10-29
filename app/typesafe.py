"""Bounded TypeSafe transport and response contracts shared by both callers."""

from __future__ import annotations

import json
import math
import time
import urllib.error
import urllib.request
from typing import Any

ENDPOINT = "https://api.typesafe.ai/v1/systemone"
MODEL = "jev-1.13.0"
ROUTING_MODEL = "jev-1.14.0"
MAX_RESPONSE_BYTES = 1_000_000


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("provider redirect refused")


def post(
    endpoint: str,
    key: str,
    payload: dict[str, Any],
    *,
    timeout: float = 30,
    retry_429: bool = True,
) -> dict[str, Any]:
    # Up to two retries, at most three seconds of waiting. A server asking us
    # to wait longer is respected by stopping, not by retrying prematurely.
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, allow_nan=False).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    opener = urllib.request.build_opener(NoRedirect())
    deadline = time.monotonic() + timeout
    waited = 0.0
    raw = b""
    for attempt in range(3 if retry_429 else 1):
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("provider request budget exhausted")
            with opener.open(request, timeout=remaining) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
            break
        except urllib.error.HTTPError as error:
            if error.code != 429 or not retry_429 or attempt == 2:
                raise
            delay = float(2**attempt)
            retry_after = error.headers.get("Retry-After") if error.headers else None
            if retry_after is not None:
                try:
                    requested = float(retry_after)
                except ValueError:
                    # Includes HTTP-date headers: don't guess and retry early.
                    raise error
                if not math.isfinite(requested) or requested < 0:
                    raise
                delay = max(delay, requested)
            if waited + delay > 3 or delay >= deadline - time.monotonic():
                raise
            error.close()
            time.sleep(delay)
            waited += delay
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError("provider result too large")
    try:
        result = json.loads(raw)
    except RecursionError as error:
        raise ValueError("provider result too deeply nested") from error
    if not isinstance(result, dict):
        raise ValueError("provider result must be an object")
    return result


def probability(value: Any) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not 0 <= value <= 1
        or not math.isfinite(value)
    ):
        raise ValueError("invalid provider probability")
    return float(value)


def choice(answer: dict[str, Any], options: Any) -> tuple[str, float]:
    selected = answer["choice"]
    distribution = answer["probabilities"]
    if (
        answer["type"] != "choice"
        or not isinstance(selected, str)
        or selected not in options
        or not isinstance(distribution, dict)
        or set(distribution) != set(options)
    ):
        raise ValueError("invalid provider choice")
    values = {key: probability(value) for key, value in distribution.items()}
    confidence = probability(answer["confidence"])
    if abs(sum(values.values()) - 1) > 0.01 or values[selected] < max(values.values()):
        raise ValueError("invalid provider distribution")
    baseline = 1 / len(values)
    expected_confidence = (values[selected] - baseline) / (1 - baseline)
    # Provider fields are rounded; accept rounding, not a different meaning.
    if abs(confidence - expected_confidence) > 0.02:
        raise ValueError("inconsistent provider confidence")
    return selected, confidence
