"""Handler implementation selection for E2E tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Mapping


HANDLER_IMPLEMENTATION_ENV = "CHATTING_E2E_HANDLER_IMPLEMENTATION"
SUPPORTED_HANDLER_IMPLEMENTATIONS = ("python", "go")


def selected_handler_implementation(
    env: Mapping[str, str] | None = None,
) -> str:
    values = os.environ if env is None else env
    selected = values.get(HANDLER_IMPLEMENTATION_ENV, "python").strip().lower()
    if not selected:
        selected = "python"
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
    python_executable: str = sys.executable,
) -> list[str]:
    implementation = selected_handler_implementation(env)
    if implementation == "python":
        return [
            python_executable,
            "-m",
            "app.main_message_handler",
            "--config",
            str(config_path),
        ]
    raise NotImplementedError(
        f"{HANDLER_IMPLEMENTATION_ENV}=go was selected, but the Go handler "
        "runtime is not implemented yet. This is intentional and must not "
        "fall back to the Python handler."
    )
