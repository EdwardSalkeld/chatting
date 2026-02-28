"""Codex executor with timeout control and strict output parsing."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any

from app.models import (
    ActionProposal,
    ConfigUpdate,
    ExecutionResult,
    OutboundMessage,
    RoutedTask,
    SCHEMA_VERSION,
)

_ALLOWED_TOP_LEVEL_KEYS = {
    "schema_version",
    "messages",
    "actions",
    "config_updates",
    "requires_human_review",
    "errors",
}
_ALLOWED_MESSAGE_KEYS = {"channel", "target", "body"}
_ALLOWED_ACTION_KEYS = {"type", "path", "content"}
_ALLOWED_CONFIG_UPDATE_KEYS = {"path", "value"}


@dataclass(frozen=True)
class CodexExecutor:
    """Run Codex as a subprocess and parse strict JSON output."""

    command: tuple[str, ...] = ("codex", "exec", "--json")

    def execute(self, task: RoutedTask) -> ExecutionResult:
        payload = json.dumps(_task_payload(task))
        try:
            completed = subprocess.run(
                self.command,
                input=payload,
                capture_output=True,
                text=True,
                timeout=task.execution_constraints.timeout_seconds,
                check=False,
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
    try:
        payload = json.loads(raw_output)
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid_json:{error.msg}") from error

    if not isinstance(payload, dict):
        raise ValueError("top_level_object_required")

    unknown_keys = set(payload) - _ALLOWED_TOP_LEVEL_KEYS
    if unknown_keys:
        raise ValueError(
            "unknown_top_level_keys:" + ",".join(sorted(unknown_keys))
        )

    required_keys = {
        "schema_version",
        "messages",
        "actions",
        "config_updates",
        "requires_human_review",
        "errors",
    }
    missing = required_keys - set(payload)
    if missing:
        raise ValueError("missing_top_level_keys:" + ",".join(sorted(missing)))

    messages = _parse_messages(payload["messages"])
    actions = _parse_actions(payload["actions"])
    config_updates = _parse_config_updates(payload["config_updates"])

    requires_human_review = payload["requires_human_review"]
    if not isinstance(requires_human_review, bool):
        raise ValueError("requires_human_review_must_be_bool")

    errors = payload["errors"]
    if not isinstance(errors, list) or not all(isinstance(item, str) for item in errors):
        raise ValueError("errors_must_be_list_of_strings")

    schema_version = payload["schema_version"]
    if not isinstance(schema_version, str):
        raise ValueError("schema_version_must_be_string")
    if not schema_version:
        raise ValueError("schema_version_required")
    if schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported_schema_version:{schema_version}")

    return ExecutionResult(
        messages=messages,
        actions=actions,
        config_updates=config_updates,
        requires_human_review=requires_human_review,
        errors=errors,
        schema_version=schema_version,
    )


def _task_payload(task: RoutedTask) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "task": task.to_dict(),
    }


def _parse_messages(value: Any) -> list[OutboundMessage]:
    if not isinstance(value, list):
        raise ValueError("messages_must_be_list")

    messages: list[OutboundMessage] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("messages_items_must_be_objects")
        unknown_keys = set(item) - _ALLOWED_MESSAGE_KEYS
        if unknown_keys:
            raise ValueError("unknown_message_keys:" + ",".join(sorted(unknown_keys)))
        messages.append(
            OutboundMessage(
                channel=_required_str(item, "channel", "message"),
                target=_required_str(item, "target", "message"),
                body=_required_str(item, "body", "message"),
            )
        )
    return messages


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

        actions.append(
            ActionProposal(
                type=_required_str(item, "type", "action"),
                path=path,
                content=content,
            )
        )
        _validate_action_payload(actions[-1])
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
    if not value:
        raise ValueError(f"{context}_{key}_required")
    return value


def _validate_action_payload(action: ActionProposal) -> None:
    if action.type != "write_file":
        return
    if action.path is None or not action.path:
        raise ValueError("write_file_path_required")
    if action.content is None or not action.content:
        raise ValueError("write_file_content_required")


def _error_result(error: str) -> ExecutionResult:
    return ExecutionResult(
        messages=[],
        actions=[],
        config_updates=[],
        requires_human_review=False,
        errors=[error],
    )


__all__ = ["CodexExecutor", "parse_execution_result"]
