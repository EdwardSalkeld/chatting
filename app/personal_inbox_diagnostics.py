"""Safe inbox diagnostics: never format exception text or task/model contents."""

from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import json
import subprocess
import urllib.error
from typing import Any, Callable


@dataclass
class ProcessingContext:
    stage: str = "pickup"
    kind: str = "unknown"
    task_ref: str = "unknown"
    cached: bool = False


current: ContextVar[ProcessingContext | None] = ContextVar(
    "inbox_processing", default=None
)


def mark_stage(name: str) -> None:
    if context := current.get():
        context.stage = name


def at_stage(name: str, work: Callable, *args: Any, **kwargs: Any) -> Any:
    mark_stage(name)
    return work(*args, **kwargs)


def identify(task: object) -> None:
    context = current.get()
    if context is None or not isinstance(task, dict):
        return
    kind = task.get("kind")
    if not isinstance(kind, str) or kind not in {
        "personal_inbox.classify.v1",
        "personal_inbox.digest.v1",
    }:
        return
    context.kind = kind
    identity = task.get(
        "message_id" if kind == "personal_inbox.classify.v1" else "digest_id"
    )
    if isinstance(identity, str):
        context.task_ref = hashlib.sha256((kind + ":" + identity).encode()).hexdigest()[
            :16
        ]


SAFE_REASONS = {
    "invalid task fields",
    "invalid task kind",
    "invalid task identity",
    "invalid mail fields",
    "invalid mail text",
    "invalid completeness flag",
    "invalid Jev Noul answer",
    "codex exited non-zero",
    "codex result too large",
    "codex result must be an object",
    "invalid digest fields",
    "invalid digest kind",
    "invalid digest size",
    "invalid digest item",
    "invalid digest text",
    "digest did not account for every email",
    "invalid digest line",
    "invalid digest line text",
    "invalid summary fields",
    "invalid summary text",
    "empty summary",
    "personal inbox rules too large",
    "invalid personal inbox rules document",
    "invalid personal inbox rule",
}


def failure_details(error: Exception) -> tuple[str, str]:
    reason = "unexpected_error"
    if isinstance(error, json.JSONDecodeError):
        reason = "invalid_json"
    elif isinstance(error, subprocess.TimeoutExpired):
        reason = "subprocess_timeout"
    elif isinstance(error, subprocess.CalledProcessError):
        reason = f"subprocess_exit_{int(error.returncode)}"
    elif isinstance(error, urllib.error.HTTPError):
        reason = f"http_status_{int(error.code)}"
    elif isinstance(error, urllib.error.URLError):
        reason = "upstream_connection_error"
    elif (
        type(error) is ValueError
        and len(error.args) == 1
        and isinstance(error.args[0], str)
    ):
        if error.args[0] in SAFE_REASONS:
            reason = error.args[0].replace(" ", "_")
    location = "unknown"
    trace = error.__traceback__
    while trace is not None:
        module = trace.tb_frame.f_globals.get("__name__")
        if module in {
            "app.personal_inbox",
            "app.worker.personal_inbox",
            "app.worker.personal_inbox_rules",
        }:
            location = f"{module}:{trace.tb_frame.f_code.co_name}:{trace.tb_lineno}"
        trace = trace.tb_next
    return reason, location
