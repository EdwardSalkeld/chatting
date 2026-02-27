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


@dataclass(frozen=True)
class OutboundMessage:
    """Message emitted by executor and gated by policy."""

    channel: str
    target: str
    body: str

    def __post_init__(self) -> None:
        if not self.channel:
            raise ValueError("channel is required")
        if not self.target:
            raise ValueError("target is required")
        if not self.body:
            raise ValueError("body is required")

    def to_dict(self) -> dict[str, str]:
        return {
            "channel": self.channel,
            "target": self.target,
            "body": self.body,
        }


@dataclass(frozen=True)
class ActionProposal:
    """Action proposal emitted by executor or policy."""

    type: str
    path: str | None = None
    content: str | None = None

    def __post_init__(self) -> None:
        if not self.type:
            raise ValueError("type is required")

    def to_dict(self) -> dict[str, str]:
        payload: dict[str, str] = {"type": self.type}
        if self.path is not None:
            payload["path"] = self.path
        if self.content is not None:
            payload["content"] = self.content
        return payload


@dataclass(frozen=True)
class ConfigUpdate:
    """Proposed or policy-reviewed config update."""

    path: str
    value: Any

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("path is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "value": self.value,
        }


@dataclass(frozen=True)
class ExecutionResult:
    """Structured output contract from executor."""

    messages: list[OutboundMessage]
    actions: list[ActionProposal]
    config_updates: list[ConfigUpdate]
    requires_human_review: bool
    errors: list[str]
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "messages": [message.to_dict() for message in self.messages],
            "actions": [action.to_dict() for action in self.actions],
            "config_updates": [update.to_dict() for update in self.config_updates],
            "requires_human_review": self.requires_human_review,
            "errors": self.errors,
        }


@dataclass(frozen=True)
class ConfigUpdateDecision:
    """Policy categorization buckets for config update proposals."""

    approved: list[ConfigUpdate] = field(default_factory=list)
    pending_review: list[ConfigUpdate] = field(default_factory=list)
    rejected: list[ConfigUpdate] = field(default_factory=list)

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "approved": [update.to_dict() for update in self.approved],
            "pending_review": [update.to_dict() for update in self.pending_review],
            "rejected": [update.to_dict() for update in self.rejected],
        }


@dataclass(frozen=True)
class PolicyDecision:
    """Policy output contract for approved and blocked operations."""

    approved_actions: list[ActionProposal]
    blocked_actions: list[ActionProposal]
    approved_messages: list[OutboundMessage]
    config_updates: ConfigUpdateDecision
    reason_codes: list[str]
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "approved_actions": [action.to_dict() for action in self.approved_actions],
            "blocked_actions": [action.to_dict() for action in self.blocked_actions],
            "approved_messages": [message.to_dict() for message in self.approved_messages],
            "config_updates": self.config_updates.to_dict(),
            "reason_codes": self.reason_codes,
        }


@dataclass(frozen=True)
class RunRecord:
    """Persisted execution record for observability and audit history."""

    run_id: str
    envelope_id: str
    source: Literal["cron", "email", "im", "webhook"]
    workflow: str
    policy_profile: str
    latency_ms: int
    result_status: str
    created_at: datetime
    schema_version: str = "1.0"

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id is required")
        if not self.envelope_id:
            raise ValueError("envelope_id is required")
        if self.source not in SOURCE_TYPES:
            raise ValueError(f"source must be one of {SOURCE_TYPES}")
        if not self.workflow:
            raise ValueError("workflow is required")
        if not self.policy_profile:
            raise ValueError("policy_profile is required")
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")
        if not self.result_status:
            raise ValueError("result_status is required")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "envelope_id": self.envelope_id,
            "source": self.source,
            "workflow": self.workflow,
            "policy_profile": self.policy_profile,
            "latency_ms": self.latency_ms,
            "result_status": self.result_status,
            "created_at": self.created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
