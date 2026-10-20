"""Go handler command for E2E tests."""

from __future__ import annotations

import os
from pathlib import Path


HANDLER_BINARY_ENV = "CHATTING_E2E_HANDLER_BINARY"


def message_handler_command(config_path: Path) -> list[str]:
    handler_binary = os.environ.get(HANDLER_BINARY_ENV, "").strip()
    if handler_binary:
        return [handler_binary, "--config", str(config_path)]
    return [
        "sh",
        "-c",
        'cd go/handler && exec go run ./cmd/chatting-handler --config "$1"',
        "chatting-handler",
        str(config_path),
    ]
