"""Go handler command selection for E2E tests."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


HANDLER_IMPLEMENTATION_ENV = "CHATTING_E2E_HANDLER_IMPLEMENTATION"
SUPPORTED_HANDLER_IMPLEMENTATIONS = ("go",)


def selected_handler_implementation(
    env: Mapping[str, str] | None = None,
) -> str:
    values = os.environ if env is None else env
    selected = values.get(HANDLER_IMPLEMENTATION_ENV, "go").strip().lower()
    if not selected:
        selected = "go"
    if selected not in SUPPORTED_HANDLER_IMPLEMENTATIONS:
        raise ValueError(
            f"{HANDLER_IMPLEMENTATION_ENV} must be 'go'; got {selected!r}"
        )
    return selected


def message_handler_command(
    config_path: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> list[str]:
    selected_handler_implementation(env)
    return [
        "sh",
        "-c",
        'cd go/handler && exec go run ./cmd/chatting-handler --config "$1"',
        "chatting-handler",
        str(config_path),
    ]
