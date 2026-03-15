"""Canonical data models for the prototype contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

SOURCE_TYPES = ("cron", "email", "im", "webhook", "internal")
PRIORITY_TYPES = ("low", "normal", "high")
SCHEMA_VERSION = "1.0"


def _validate_schema_version(schema_version: str) -> None:
    if not isinstance(schema_version, str):
        raise ValueError("schema_version must be a string")
    if not schema_version:
        raise ValueError("schema_version is required")
    if schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported_schema_version:{schema_version}")


def _validate_string_list(values: list[str], *, field_name: str) -> None:
    if not isinstance(values, list):
        raise ValueError(f"{field_name} must be a list")
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{field_name} items must be non-empty strings")


def _validate_required_string(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")


def _validate_typed_list(values: list[Any], *, field_name: str, item_type: type[Any]) -> None:
    if not isinstance(values, list):
        raise ValueError(f"{field_name} must be a list")
    for item in values:
        if not isinstance(item, item_type):
            raise ValueError(f"{field_name} items must be {item_type.__name__}")


def _validate_context_refs(values: list[str]) -> None:
    if not isinstance(values, list):
        raise ValueError("context_refs must be a list")
    for item in values:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("context_refs items must be non-empty strings")


def _validate_metadata_dict(values: dict[str, Any], *, field_name: str) -> None:
    if not isinstance(values, dict):
        raise ValueError(f"{field_name} must be a dict")
    for key in values:
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"{field_name} keys must be non-empty strings")


def _validate_attachments(values: list["AttachmentRef"]) -> None:
    if not isinstance(values, list):
        raise ValueError("attachments must be a list")
    for item in values:
        if not isinstance(item, AttachmentRef):
            raise ValueError("attachments items must be AttachmentRef")


@dataclass(frozen=True)
class AttachmentRef:
    """Reference to an external attachment."""

    uri: str
    name: str | None = None

    def __post_init__(self) -> None:
        _validate_required_string(self.uri, field_name="uri")
        if self.name is not None:
            _validate_required_string(self.name, field_name="name")


@dataclass(frozen=True)
class ReplyChannel:
    """Target used by response dispatch adapters."""

    type: str
    target: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_required_string(self.type, field_name="type")
        _validate_required_string(self.target, field_name="target")
        _validate_metadata_dict(self.metadata, field_name="metadata")


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
    source: Literal["cron", "email", "im", "webhook", "internal"]
    received_at: datetime
    actor: str | None
    content: str
    attachments: list[AttachmentRef]
    context_refs: list[str]
    policy_profile: str
    reply_channel: ReplyChannel
    dedupe_key: str
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if self.source not in SOURCE_TYPES:
            raise ValueError(f"source must be one of {SOURCE_TYPES}")
        _validate_required_string(self.id, field_name="id")
        _validate_required_string(self.content, field_name="content")
        _validate_required_string(self.dedupe_key, field_name="dedupe_key")
        _validate_required_string(self.policy_profile, field_name="policy_profile")
        _validate_attachments(self.attachments)
        _validate_context_refs(self.context_refs)
        if self.received_at.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        payload = {
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
        if self.reply_channel.metadata:
            payload["reply_channel"]["metadata"] = self.reply_channel.metadata
        return payload


@dataclass(frozen=True)
class RoutedTask:
    """Task created by router from the canonical envelope."""

    task_id: str
    envelope_id: str
    workflow: str
    priority: Literal["low", "normal", "high"]
    execution_constraints: ExecutionConstraints
    policy_profile: str
    event_time: datetime | None = None
    source: Literal["cron", "email", "im", "webhook", "internal"] | None = None
    actor: str | None = None
    content: str | None = None
    attachments: list[AttachmentRef] = field(default_factory=list)
    reply_channel: ReplyChannel | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if self.priority not in PRIORITY_TYPES:
            raise ValueError(f"priority must be one of {PRIORITY_TYPES}")
        _validate_required_string(self.task_id, field_name="task_id")
        _validate_required_string(self.envelope_id, field_name="envelope_id")
        _validate_required_string(self.workflow, field_name="workflow")
        _validate_required_string(self.policy_profile, field_name="policy_profile")
        if self.event_time is not None and self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")
        if self.source is not None and self.source not in SOURCE_TYPES:
            raise ValueError(f"source must be one of {SOURCE_TYPES}")
        if self.actor is not None:
            _validate_required_string(self.actor, field_name="actor")
        if self.content is not None:
            _validate_required_string(self.content, field_name="content")
        _validate_attachments(self.attachments)
        if self.reply_channel is not None:
            _validate_required_string(self.reply_channel.type, field_name="reply_channel.type")
            _validate_required_string(self.reply_channel.target, field_name="reply_channel.target")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
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
        if self.event_time is not None:
            payload["event_time"] = (
                self.event_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            )
        if self.source is not None:
            payload["source"] = self.source
        if self.actor is not None:
            payload["actor"] = self.actor
        if self.content is not None:
            payload["content"] = self.content
        if self.attachments:
            payload["attachments"] = [
                {"uri": item.uri, "name": item.name}
                for item in self.attachments
            ]
        if self.reply_channel is not None:
            payload["reply_channel"] = {
                "type": self.reply_channel.type,
                "target": self.reply_channel.target,
            }
            if self.reply_channel.metadata:
                payload["reply_channel"]["metadata"] = self.reply_channel.metadata
        return payload


@dataclass(frozen=True)
class OutboundMessage:
    """Message emitted by executor and gated by policy."""

    channel: str
    target: str
    body: str | None = None
    attachment: AttachmentRef | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_required_string(self.channel, field_name="channel")
        _validate_required_string(self.target, field_name="target")
        if self.body is not None:
            _validate_required_string(self.body, field_name="body")
        if self.attachment is not None and not isinstance(self.attachment, AttachmentRef):
            raise ValueError("attachment must be AttachmentRef")
        if self.body is None and self.attachment is None:
            raise ValueError("body or attachment is required")
        _validate_metadata_dict(self.metadata, field_name="metadata")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "channel": self.channel,
            "target": self.target,
        }
        if self.body is not None:
            payload["body"] = self.body
        if self.attachment is not None:
            payload["attachment"] = {
                "uri": self.attachment.uri,
                "name": self.attachment.name,
            }
        if self.metadata:
            payload["metadata"] = self.metadata
        return payload


@dataclass(frozen=True)
class ActionProposal:
    """Action proposal emitted by executor or policy."""

    type: str
    path: str | None = None
    content: str | None = None

    def __post_init__(self) -> None:
        _validate_required_string(self.type, field_name="type")
        if self.path is not None:
            _validate_required_string(self.path, field_name="path")
        if self.content is not None:
            _validate_required_string(self.content, field_name="content")

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
        if not isinstance(self.path, str) or not self.path.strip():
            raise ValueError("path is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "value": self.value,
        }


@dataclass(frozen=True)
class ExecutionResult:
    """Structured output contract from executor."""

    actions: list[ActionProposal]
    config_updates: list[ConfigUpdate]
    requires_human_review: bool
    errors: list[str]
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        _validate_typed_list(
            self.actions,
            field_name="actions",
            item_type=ActionProposal,
        )
        _validate_typed_list(
            self.config_updates,
            field_name="config_updates",
            item_type=ConfigUpdate,
        )
        _validate_string_list(self.errors, field_name="errors")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
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

    def __post_init__(self) -> None:
        _validate_typed_list(
            self.approved,
            field_name="approved",
            item_type=ConfigUpdate,
        )
        _validate_typed_list(
            self.pending_review,
            field_name="pending_review",
            item_type=ConfigUpdate,
        )
        _validate_typed_list(
            self.rejected,
            field_name="rejected",
            item_type=ConfigUpdate,
        )

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
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        _validate_typed_list(
            self.approved_actions,
            field_name="approved_actions",
            item_type=ActionProposal,
        )
        _validate_typed_list(
            self.blocked_actions,
            field_name="blocked_actions",
            item_type=ActionProposal,
        )
        _validate_typed_list(
            self.approved_messages,
            field_name="approved_messages",
            item_type=OutboundMessage,
        )
        if not isinstance(self.config_updates, ConfigUpdateDecision):
            raise ValueError("config_updates must be ConfigUpdateDecision")
        _validate_string_list(self.reason_codes, field_name="reason_codes")

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
class ApplyResult:
    """Result contract produced by applier implementations."""

    applied_actions: list[ActionProposal]
    skipped_actions: list[ActionProposal]
    dispatched_messages: list[OutboundMessage]
    reason_codes: list[str]
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        _validate_typed_list(
            self.applied_actions,
            field_name="applied_actions",
            item_type=ActionProposal,
        )
        _validate_typed_list(
            self.skipped_actions,
            field_name="skipped_actions",
            item_type=ActionProposal,
        )
        _validate_typed_list(
            self.dispatched_messages,
            field_name="dispatched_messages",
            item_type=OutboundMessage,
        )
        _validate_string_list(self.reason_codes, field_name="reason_codes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "applied_actions": [action.to_dict() for action in self.applied_actions],
            "skipped_actions": [action.to_dict() for action in self.skipped_actions],
            "dispatched_messages": [
                message.to_dict() for message in self.dispatched_messages
            ],
            "reason_codes": self.reason_codes,
        }


@dataclass(frozen=True)
class RunRecord:
    """Persisted execution record for observability and audit history."""

    run_id: str
    envelope_id: str
    source: Literal["cron", "email", "im", "webhook", "internal"]
    workflow: str
    policy_profile: str
    latency_ms: int
    result_status: str
    created_at: datetime
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        _validate_required_string(self.run_id, field_name="run_id")
        _validate_required_string(self.envelope_id, field_name="envelope_id")
        if self.source not in SOURCE_TYPES:
            raise ValueError(f"source must be one of {SOURCE_TYPES}")
        _validate_required_string(self.workflow, field_name="workflow")
        _validate_required_string(self.policy_profile, field_name="policy_profile")
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")
        _validate_required_string(self.result_status, field_name="result_status")
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


@dataclass(frozen=True)
class AuditEvent:
    """Audit log event persisted for every processed run."""

    run_id: str
    envelope_id: str
    source: Literal["cron", "email", "im", "webhook", "internal"]
    workflow: str
    policy_profile: str
    result_status: str
    detail: dict[str, Any]
    created_at: datetime
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        _validate_required_string(self.run_id, field_name="run_id")
        _validate_required_string(self.envelope_id, field_name="envelope_id")
        if self.source not in SOURCE_TYPES:
            raise ValueError(f"source must be one of {SOURCE_TYPES}")
        _validate_required_string(self.workflow, field_name="workflow")
        _validate_required_string(self.policy_profile, field_name="policy_profile")
        _validate_required_string(self.result_status, field_name="result_status")
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
            "result_status": self.result_status,
            "detail": self.detail,
            "created_at": self.created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        }


@dataclass(frozen=True)
class DeadLetterRecord:
    """Persisted dead-letter queue record for replay operations."""

    dead_letter_id: int
    run_id: str
    envelope: TaskEnvelope
    reason_codes: list[str]
    last_error: str | None
    attempt_count: int
    status: str
    created_at: datetime
    replayed_run_id: str | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if self.dead_letter_id <= 0:
            raise ValueError("dead_letter_id must be positive")
        _validate_required_string(self.run_id, field_name="run_id")
        if not isinstance(self.envelope, TaskEnvelope):
            raise ValueError("envelope must be TaskEnvelope")
        _validate_string_list(self.reason_codes, field_name="reason_codes")
        if self.last_error is not None:
            _validate_required_string(self.last_error, field_name="last_error")
        if self.attempt_count <= 0:
            raise ValueError("attempt_count must be positive")
        if self.status not in {"pending", "replayed"}:
            raise ValueError("status must be pending or replayed")
        if self.replayed_run_id is not None:
            _validate_required_string(self.replayed_run_id, field_name="replayed_run_id")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dead_letter_id": self.dead_letter_id,
            "run_id": self.run_id,
            "envelope": self.envelope.to_dict(),
            "reason_codes": self.reason_codes,
            "last_error": self.last_error,
            "attempt_count": self.attempt_count,
            "status": self.status,
            "created_at": self.created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "replayed_run_id": self.replayed_run_id,
        }


@dataclass(frozen=True)
class PendingApprovalRecord:
    """Persisted human-approval item for sensitive config updates."""

    approval_id: int
    run_id: str
    envelope_id: str
    config_path: str
    config_value: Any
    status: str
    created_at: datetime
    resolved_at: datetime | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if self.approval_id <= 0:
            raise ValueError("approval_id must be positive")
        _validate_required_string(self.run_id, field_name="run_id")
        _validate_required_string(self.envelope_id, field_name="envelope_id")
        _validate_required_string(self.config_path, field_name="config_path")
        if self.status not in {"pending", "approved", "rejected"}:
            raise ValueError("status must be pending, approved, or rejected")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        if self.resolved_at is not None and self.resolved_at.tzinfo is None:
            raise ValueError("resolved_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "approval_id": self.approval_id,
            "run_id": self.run_id,
            "envelope_id": self.envelope_id,
            "config_path": self.config_path,
            "config_value": self.config_value,
            "status": self.status,
            "created_at": self.created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "resolved_at": (
                self.resolved_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                if self.resolved_at is not None
                else None
            ),
        }


@dataclass(frozen=True)
class ConfigVersionRecord:
    """Persisted config version entry for approved updates and rollbacks."""

    version_id: int
    config_path: str
    old_value: Any
    new_value: Any
    source: str
    source_ref: str | None
    created_at: datetime
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if self.version_id <= 0:
            raise ValueError("version_id must be positive")
        _validate_required_string(self.config_path, field_name="config_path")
        _validate_required_string(self.source, field_name="source")
        if self.source_ref is not None:
            _validate_required_string(self.source_ref, field_name="source_ref")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "version_id": self.version_id,
            "config_path": self.config_path,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "source": self.source,
            "source_ref": self.source_ref,
            "created_at": self.created_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
