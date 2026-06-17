"""Go handler command selection for E2E tests."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


HANDLER_IMPLEMENTATION_ENV = "CHATTING_E2E_HANDLER_IMPLEMENTATION"
HANDLER_BINARY_ENV = "CHATTING_E2E_HANDLER_BINARY"
SUPPORTED_HANDLER_IMPLEMENTATIONS = ("go",)
DEFAULT_HANDLER_IMPLEMENTATION = "go"


def selected_handler_implementation(
    env: Mapping[str, str] | None = None,
) -> str:
    values = os.environ if env is None else env
    selected = values.get(
        HANDLER_IMPLEMENTATION_ENV, DEFAULT_HANDLER_IMPLEMENTATION
    ).strip().lower()
    if not selected:
        selected = DEFAULT_HANDLER_IMPLEMENTATION
    if selected not in SUPPORTED_HANDLER_IMPLEMENTATIONS:
        supported = ", ".join(SUPPORTED_HANDLER_IMPLEMENTATIONS)
        raise ValueError(
            f"{HANDLER_IMPLEMENTATION_ENV} must be one of {supported}; got {selected!r}"
        )
    return selected


def message_handler_command(
    config_path: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> list[str]:
    selected_handler_implementation(env)
    values = os.environ if env is None else env
    handler_binary = values.get(HANDLER_BINARY_ENV, "").strip()
    if handler_binary:
        return [handler_binary, "--config", str(config_path)]
    return [
        "sh",
        "-c",
        'cd go/handler && exec go run ./cmd/chatting-handler --config "$1"',
        "chatting-handler",
        str(config_path),
    ]
