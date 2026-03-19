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


SUPPORTED_CONTEXT_SCHEMES = ("repo",)


@dataclass(frozen=True)
class ContextRef:
    """Parsed context reference with explicit scheme and path."""

    type: str
    path: str

    def __post_init__(self) -> None:
        if self.type not in SUPPORTED_CONTEXT_SCHEMES:
            raise ValueError(f"unsupported context scheme: {self.type}")
        if not self.path.startswith("/"):
            raise ValueError(f"context path must be absolute: {self.path}")

    def to_dict(self) -> dict[str, str]:
        return {"type": self.type, "path": self.path}


def parse_context_ref(raw: str) -> ContextRef:
    """Parse 'scheme:/path' into a ContextRef."""
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("context_refs items must be non-empty strings")
    if ":" not in raw:
        raise ValueError(f"context ref must use scheme:path format: {raw}")
    scheme, path = raw.split(":", 1)
    scheme = scheme.strip()
    path = path.strip()
    if not scheme:
        raise ValueError(f"context ref scheme must not be empty: {raw}")
    if not path:
        raise ValueError(f"context ref path must not be empty: {raw}")
    return ContextRef(type=scheme, path=path)


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


def _validate_prompt_context(value: "PromptContext") -> None:
    if not isinstance(value, PromptContext):
        raise ValueError("prompt_context must be PromptContext")


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
class PromptContext:
    """Structured instruction context that is separate from repo/file context."""

    global_instructions: list[str] = field(default_factory=list)
    source_instructions: list[str] = field(default_factory=list)
    reply_channel_instructions: list[str] = field(default_factory=list)
    task_instructions: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        _validate_string_list(
            self.global_instructions,
            field_name="prompt_context.global_instructions",
        )
        _validate_string_list(
            self.source_instructions,
            field_name="prompt_context.source_instructions",
        )
        _validate_string_list(
            self.reply_channel_instructions,
            field_name="prompt_context.reply_channel_instructions",
        )
        _validate_string_list(
            self.task_instructions,
            field_name="prompt_context.task_instructions",
        )

    def assembled_instructions(self) -> list[str]:
        return (
            list(self.global_instructions)
            + list(self.source_instructions)
            + list(self.reply_channel_instructions)
            + list(self.task_instructions)
        )

    def has_content(self) -> bool:
        return bool(
            self.global_instructions
            or self.source_instructions
            or self.reply_channel_instructions
            or self.task_instructions
        )

    def to_dict(self) -> dict[str, list[str]]:
        return {
            "global_instructions": list(self.global_instructions),
            "source_instructions": list(self.source_instructions),
            "reply_channel_instructions": list(self.reply_channel_instructions),
            "task_instructions": list(self.task_instructions),
            "assembled_instructions": self.assembled_instructions(),
        }


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
    reply_channel: ReplyChannel
    dedupe_key: str
    prompt_context: PromptContext = field(default_factory=PromptContext)
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if self.source not in SOURCE_TYPES:
            raise ValueError(f"source must be one of {SOURCE_TYPES}")
        _validate_required_string(self.id, field_name="id")
        _validate_required_string(self.content, field_name="content")
        _validate_required_string(self.dedupe_key, field_name="dedupe_key")
        _validate_attachments(self.attachments)
        _validate_context_refs(self.context_refs)
        _validate_prompt_context(self.prompt_context)
        if self.received_at.tzinfo is None:
            raise ValueError("received_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        reply_channel: dict[str, Any] = {
            "type": self.reply_channel.type,
            "target": self.reply_channel.target,
        }
        if self.reply_channel.metadata:
            reply_channel["metadata"] = self.reply_channel.metadata
        payload: dict[str, Any] = {
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
            "reply_channel": reply_channel,
            "dedupe_key": self.dedupe_key,
        }
        if self.prompt_context.has_content():
            payload["prompt_context"] = self.prompt_context.to_dict()
        return payload


@dataclass(frozen=True)
class RoutedTask:
    """Task created by router from the canonical envelope."""

    task_id: str
    envelope_id: str
    workflow: str
    priority: Literal["low", "normal", "high"]
    execution_constraints: ExecutionConstraints
    event_time: datetime | None = None
    source: Literal["cron", "email", "im", "webhook", "internal"] | None = None
    actor: str | None = None
    content: str | None = None
    attachments: list[AttachmentRef] = field(default_factory=list)
    context: list[ContextRef] = field(default_factory=list)
    prompt_context: PromptContext = field(default_factory=PromptContext)
    reply_channel: ReplyChannel | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        if self.priority not in PRIORITY_TYPES:
            raise ValueError(f"priority must be one of {PRIORITY_TYPES}")
        _validate_required_string(self.task_id, field_name="task_id")
        _validate_required_string(self.envelope_id, field_name="envelope_id")
        _validate_required_string(self.workflow, field_name="workflow")
        if self.event_time is not None and self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")
        if self.source is not None and self.source not in SOURCE_TYPES:
            raise ValueError(f"source must be one of {SOURCE_TYPES}")
        if self.actor is not None:
            _validate_required_string(self.actor, field_name="actor")
        if self.content is not None:
            _validate_required_string(self.content, field_name="content")
        _validate_attachments(self.attachments)
        _validate_typed_list(self.context, field_name="context", item_type=ContextRef)
        _validate_prompt_context(self.prompt_context)
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
        if self.context:
            payload["context"] = [ref.to_dict() for ref in self.context]
        if self.prompt_context.has_content():
            payload["prompt_context"] = self.prompt_context.to_dict()
        if self.reply_channel is not None:
            reply_channel_dict: dict[str, Any] = {
                "type": self.reply_channel.type,
                "target": self.reply_channel.target,
            }
            if self.reply_channel.metadata:
                reply_channel_dict["metadata"] = self.reply_channel.metadata
            payload["reply_channel"] = reply_channel_dict
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
class ExecutionResult:
    """Structured output contract from executor."""

    actions: list[ActionProposal]
    errors: list[str]
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_schema_version(self.schema_version)
        _validate_typed_list(
            self.actions,
            field_name="actions",
            item_type=ActionProposal,
        )
        _validate_string_list(self.errors, field_name="errors")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "actions": [action.to_dict() for action in self.actions],
            "errors": self.errors,
        }


@dataclass(frozen=True)
class PolicyDecision:
    """Policy output contract for approved and blocked operations."""

    approved_actions: list[ActionProposal]
    blocked_actions: list[ActionProposal]
    approved_messages: list[OutboundMessage]
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
        _validate_string_list(self.reason_codes, field_name="reason_codes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "approved_actions": [action.to_dict() for action in self.approved_actions],
            "blocked_actions": [action.to_dict() for action in self.blocked_actions],
            "approved_messages": [message.to_dict() for message in self.approved_messages],
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
