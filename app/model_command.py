"""Explicit model selection for one persistent work item."""

from __future__ import annotations

import re

from app.models import TaskEnvelope
from app.usage_command import _THREAD_PREFIX_RE

MODELS = {"high": "gpt-6.1-sol", "low": "gpt-6-luna"}


def parse_model_command(envelope: TaskEnvelope) -> tuple[str, str | None] | None:
    content = _THREAD_PREFIX_RE.sub("", envelope.content.strip(), count=1).strip()
    if re.fullmatch(r"/model(?:@\w+)?", content, flags=re.IGNORECASE):
        return ("show", None)
    match = re.fullmatch(r"/set(?:@\w+)?\s+(high|low)", content, flags=re.IGNORECASE)
    if match:
        return ("set", match.group(1).lower())
    return None
