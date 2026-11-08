"""Capture CLI counters without retaining prompts or model output."""

from collections.abc import Callable
from contextvars import ContextVar
import json

usage_sink: ContextVar[Callable[[dict], None] | None] = ContextVar(
    "codex_usage_sink", default=None
)


def cli_usage(output: str | bytes | None) -> list[dict]:
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    result = []
    for line in (output or "").splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or event.get("type") != "turn.completed":
            continue
        usage = event.get("usage")
        if not isinstance(usage, dict):
            continue
        counts = {
            key: value
            for key, value in usage.items()
            if key
            in {
                "input_tokens",
                "cached_input_tokens",
                "output_tokens",
                "reasoning_output_tokens",
            }
            and type(value) is int
            and value >= 0
        }
        if {"input_tokens", "output_tokens"} <= counts.keys():
            result.append(counts)
    return result
