"""Canonical data models for the prototype contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

SOURCE_TYPES = ("cron", "email", "im", "webhook")
PRIORITY_TYPES = ("low", "normal", "high")


@dataclass(frozen=True)
class AttachmentRef:
    """Reference to an external attachment."""

    uri: str
    name: str | None = None


@dataclass(frozen=True)
class ReplyChannel:
    """Target used by response dispatch adapters."""

    type: str
    target: str


@dataclass(frozen=True)
class ExecutionConstraints:
    """Execution budget and timeout for a routed task."""

    timeout_seconds: int
    max_tokens: int

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be positive")


@dataclass(frozen=True)
class TaskEnvelope:
    """Normalized input event schema."""

    id: str
    source: Literal["cron", "email", "im", "webhook"]
    received_at: datetime
    actor: str | None
    content: str
    attachments: list[AttachmentRef]
    context_refs: list[str]
    policy_profile: str
    reply_channel: ReplyChannel
    dedupe_key: str
    schema_version: str = "1.0"

    def __post_init__(self) -> None:
        if self.source not in SOURCE_TYPES:
            raise ValueError(f"source must be one of {SOURCE_TYPES}")
        if not self.id:
            raise ValueError("id is required")
        if not self.dedupe_key:
            raise ValueError("dedupe_key is required")
        if not self.policy_profile:
            raise ValueError("policy_profile is required")
        if self.received_at.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "id": self.id,
            "source": self.source,
            "received_at": self.received_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "actor": self.actor,
            "content": self.content,
            "attachments": [
                {"uri": item.uri, "name": item.name}
                for item in self.attachments
            ],
            "context_refs": self.context_refs,
            "policy_profile": self.policy_profile,
            "reply_channel": {
                "type": self.reply_channel.type,
                "target": self.reply_channel.target,
            },
            "dedupe_key": self.dedupe_key,
        }


@dataclass(frozen=True)
class RoutedTask:
    """Task created by router from the canonical envelope."""

    task_id: str
    envelope_id: str
    workflow: str
    priority: Literal["low", "normal", "high"]
    execution_constraints: ExecutionConstraints
    policy_profile: str
    schema_version: str = "1.0"

    def __post_init__(self) -> None:
        if self.priority not in PRIORITY_TYPES:
            raise ValueError(f"priority must be one of {PRIORITY_TYPES}")
        if not self.task_id:
            raise ValueError("task_id is required")
        if not self.envelope_id:
            raise ValueError("envelope_id is required")
        if not self.workflow:
            raise ValueError("workflow is required")
        if not self.policy_profile:
            raise ValueError("policy_profile is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "task_id": self.task_id,
            "envelope_id": self.envelope_id,
            "workflow": self.workflow,
            "priority": self.priority,
            "execution_constraints": {
                "timeout_seconds": self.execution_constraints.timeout_seconds,
                "max_tokens": self.execution_constraints.max_tokens,
            },
            "policy_profile": self.policy_profile,
        }
