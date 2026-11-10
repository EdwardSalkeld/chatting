"""Explicit model selection for one persistent work item."""

from __future__ import annotations

import re
import os
from collections.abc import Mapping

from app.models import TaskEnvelope
from app.usage_command import _THREAD_PREFIX_RE


def models_from_environment(environment: Mapping[str, str]) -> dict[str, str]:
    models = {}
    for tier, default in {"high": "gpt-6.1-sol", "low": "gpt-6-luna"}.items():
        setting = f"CHATTING_MODEL_{tier.upper()}"
        value = environment.get(setting, default).strip()
        if not value or any(character.isspace() for character in value):
            raise ValueError(
                f"{setting} must be a nonempty model ID without whitespace"
            )
        models[tier] = value
    return models


# Resolve once at process startup. Executors and their session owners inherit
# the same environment; running tasks keep their original model configuration.
MODELS = models_from_environment(os.environ)


def parse_model_command(envelope: TaskEnvelope) -> tuple[str, str | None] | None:
    content = _THREAD_PREFIX_RE.sub("", envelope.content.strip(), count=1).strip()
    executor = re.fullmatch(
        r"/executor(?:@\w+)?(?:\s+(exec|app-server))?", content, flags=re.IGNORECASE
    )
    if executor:
        return ("executor", executor.group(1).lower() if executor.group(1) else None)
    finish = re.fullmatch(
        r"/finish(?:@\w+)?(?:\s+(on|off))?", content, flags=re.IGNORECASE
    )
    if finish:
        return ("finish", finish.group(1).lower() if finish.group(1) else None)
    if re.fullmatch(r"/model(?:@\w+)?", content, flags=re.IGNORECASE):
        return ("show", None)
    match = re.fullmatch(
        r"/set(?:@\w+)?\s+(high|low|auto)", content, flags=re.IGNORECASE
    )
    if match:
        return ("set", match.group(1).lower())
    return None
