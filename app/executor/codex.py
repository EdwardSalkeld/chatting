"""Codex executor with timeout control and strict output parsing."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from app.models import (
    ActionProposal,
    ConfigUpdate,
    ExecutionResult,
    RoutedTask,
    SCHEMA_VERSION,
)

_ALLOWED_TOP_LEVEL_KEYS = {
    "schema_version",
    "actions",
    "config_updates",
    "requires_human_review",
    "errors",
}
_REQUIRED_TOP_LEVEL_KEYS = {
    "schema_version",
    "actions",
    "config_updates",
    "requires_human_review",
    "errors",
}
_ALLOWED_ACTION_KEYS = {"type", "path", "content"}
_ALLOWED_CONFIG_UPDATE_KEYS = {"path", "value"}

EXECUTION_RESULT_JSON_SCHEMA = json.dumps(
    {
        "type": "object",
        "properties": {
            "schema_version": {"type": "string"},
            "actions": {"type": "array"},
            "config_updates": {"type": "array"},
            "requires_human_review": {"type": "boolean"},
            "errors": {"type": "array"},
        },
        "required": sorted(_REQUIRED_TOP_LEVEL_KEYS),
    },
    separators=(",", ":"),
)


@dataclass(frozen=True)
class CodexExecutor:
    """Run Codex as a subprocess and parse strict JSON output."""

    command: tuple[str, ...] = ("codex", "exec", "--json")
    cwd: str | None = None
    now_provider: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))

    def execute(self, task: RoutedTask) -> ExecutionResult:
        payload = json.dumps(_task_payload(task, current_time=self.now_provider()))
        try:
            completed = subprocess.run(
                self.command,
                input=payload,
                capture_output=True,
                text=True,
                timeout=task.execution_constraints.timeout_seconds,
                check=False,
                cwd=self.cwd,
            )
        except subprocess.TimeoutExpired:
            return _error_result("executor_timeout")

        if completed.returncode != 0:
            error = f"executor_exit_nonzero:{completed.returncode}"
            stderr = completed.stderr.strip()
            if stderr:
                error = f"{error}:{stderr}"
            return _error_result(error)

        try:
            return parse_execution_result(completed.stdout)
        except ValueError as error:
            return _error_result(f"executor_output_invalid:{error}")


def parse_execution_result(raw_output: str) -> ExecutionResult:
    """Parse strict JSON output into ExecutionResult."""
    payload = _load_execution_payload(raw_output)

    if not isinstance(payload, dict):
        raise ValueError("top_level_object_required")

    unknown_keys = set(payload) - _ALLOWED_TOP_LEVEL_KEYS
    if unknown_keys:
        raise ValueError(
            "unknown_top_level_keys:" + ",".join(sorted(unknown_keys))
        )

    missing = _REQUIRED_TOP_LEVEL_KEYS - set(payload)
    if missing:
        raise ValueError("missing_top_level_keys:" + ",".join(sorted(missing)))

    actions = _parse_actions(payload["actions"])
    config_updates = _parse_config_updates(payload["config_updates"])

    requires_human_review = payload["requires_human_review"]
    if not isinstance(requires_human_review, bool):
        raise ValueError("requires_human_review_must_be_bool")

    errors = payload["errors"]
    if not isinstance(errors, list) or not all(isinstance(item, str) for item in errors):
        raise ValueError("errors_must_be_list_of_strings")
    if any(_is_blank(item) for item in errors):
        raise ValueError("errors_items_must_be_non_empty_strings")

    schema_version = payload["schema_version"]
    if not isinstance(schema_version, str):
        raise ValueError("schema_version_must_be_string")
    if not schema_version:
        raise ValueError("schema_version_required")
    if schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported_schema_version:{schema_version}")

    return ExecutionResult(
        actions=actions,
        config_updates=config_updates,
        requires_human_review=requires_human_review,
        errors=errors,
        schema_version=schema_version,
    )


def _load_execution_payload(raw_output: str) -> Any:
    try:
        parsed = json.loads(raw_output)
    except json.JSONDecodeError as error:
        recovered = _recover_last_json_object(raw_output)
        if recovered is None:
            raise ValueError(f"invalid_json:{error.msg}") from error
        return recovered
    # Claude CLI wraps structured output under a "structured_output" key
    if isinstance(parsed, dict) and "structured_output" in parsed:
        return parsed["structured_output"]
    return parsed


def _recover_last_json_object(raw_output: str) -> dict[str, Any] | None:
    decoder = json.JSONDecoder()
    index = 0
    last_object: dict[str, Any] | None = None
    last_execution_like: dict[str, Any] | None = None

    while index < len(raw_output):
        try:
            parsed, end_index = decoder.raw_decode(raw_output, index)
        except json.JSONDecodeError:
            index += 1
            continue

        if isinstance(parsed, dict):
            last_object = parsed
            if _REQUIRED_TOP_LEVEL_KEYS.issubset(set(parsed)):
                last_execution_like = parsed

        index = end_index

    return last_execution_like or last_object


def _task_payload(task: RoutedTask, *, current_time: datetime) -> dict[str, Any]:
    if current_time.tzinfo is None:
        raise ValueError("current_time must be timezone-aware")
    return {
        "schema_version": SCHEMA_VERSION,
        "task": task.to_dict(),
        "current_time": current_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "reply_contract": {
            "visible_replies_must_use": "python3 -m app.main_reply",
            "visible_replies_must_not_be_returned_in_executor_output": True,
            "executor_output_is_completion_only": True,
        },
    }

def _parse_actions(value: Any) -> list[ActionProposal]:
    if not isinstance(value, list):
        raise ValueError("actions_must_be_list")

    actions: list[ActionProposal] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("actions_items_must_be_objects")
        unknown_keys = set(item) - _ALLOWED_ACTION_KEYS
        if unknown_keys:
            raise ValueError("unknown_action_keys:" + ",".join(sorted(unknown_keys)))

        path = item.get("path")
        if path is not None and not isinstance(path, str):
            raise ValueError("action_path_must_be_string")

        content = item.get("content")
        if content is not None and not isinstance(content, str):
            raise ValueError("action_content_must_be_string")

        action_type = _required_str(item, "type", "action")
        _validate_action_payload(
            action_type=action_type,
            path=path,
            content=content,
        )
        actions.append(
            ActionProposal(
                type=action_type,
                path=path,
                content=content,
            )
        )
    return actions


def _parse_config_updates(value: Any) -> list[ConfigUpdate]:
    if not isinstance(value, list):
        raise ValueError("config_updates_must_be_list")

    updates: list[ConfigUpdate] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("config_updates_items_must_be_objects")
        unknown_keys = set(item) - _ALLOWED_CONFIG_UPDATE_KEYS
        if unknown_keys:
            raise ValueError(
                "unknown_config_update_keys:" + ",".join(sorted(unknown_keys))
            )
        if "value" not in item:
            raise ValueError("config_update_value_required")
        updates.append(
            ConfigUpdate(
                path=_required_str(item, "path", "config_update"),
                value=item["value"],
            )
        )
    return updates


def _required_str(payload: dict[str, Any], key: str, context: str) -> str:
    if key not in payload:
        raise ValueError(f"{context}_{key}_required")
    value = payload[key]
    if not isinstance(value, str):
        raise ValueError(f"{context}_{key}_must_be_string")
    if _is_blank(value):
        raise ValueError(f"{context}_{key}_required")
    return value


def _optional_required_str(value: Any, key: str, context: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{context}_{key}_must_be_string")
    if _is_blank(value):
        raise ValueError(f"{context}_{key}_required")
    return value


def _validate_action_payload(
    *,
    action_type: str,
    path: str | None,
    content: str | None,
) -> None:
    if action_type == "write_file":
        if path is None or _is_blank(path):
            raise ValueError("write_file_path_required")
        if content is None or _is_blank(content):
            raise ValueError("write_file_content_required")
        return

    if path is not None:
        raise ValueError("non_write_file_path_forbidden")
    if content is not None:
        raise ValueError("non_write_file_content_forbidden")


def _error_result(error: str) -> ExecutionResult:
    return ExecutionResult(
        actions=[],
        config_updates=[],
        requires_human_review=False,
        errors=[error],
    )


def _is_blank(value: str) -> bool:
    return not value.strip()


__all__ = ["CodexExecutor", "parse_execution_result"]
