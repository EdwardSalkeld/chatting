"""Queue payload contracts for BBMB transport."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, cast

from app.models import (
    AttachmentRef,
    OutboundMessage,
    PromptContext,
    ReplyChannel,
    SCHEMA_VERSION,
    TaskEnvelope,
)
from app.timefmt import ensure_utc, format_utc, parse_utc

SourceType = Literal["cron", "email", "im", "webhook", "internal", "reminder"]

_TASK_MESSAGE_TYPE = "chatting.task.v1"
_EGRESS_MESSAGE_TYPE_V2 = "chatting.egress.v2"


def _require_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _optional_non_empty_string(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_non_empty_string(value, field_name=field_name)


def _parse_utc_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be an RFC3339 string")
    return parse_utc(value, field_name=field_name)


def _task_id_for_envelope(envelope: TaskEnvelope) -> str:
    return f"task:{envelope.id}"


def _parse_sequence(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("sequence must be a non-negative integer")
    return value


def _parse_optional_dict(value: object, *, field_name: str) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return dict(value)


def _parse_prompt_context_payload(value: object) -> PromptContext:
    if value is None:
        return PromptContext()
    if not isinstance(value, dict):
        raise ValueError("envelope.prompt_context must be an object")
    return PromptContext(
        global_instructions=_parse_prompt_context_list(
            value.get("global_instructions", []),
            field_name="envelope.prompt_context.global_instructions",
        ),
        source_instructions=_parse_prompt_context_list(
            value.get("source_instructions", []),
            field_name="envelope.prompt_context.source_instructions",
        ),
        reply_channel_instructions=_parse_prompt_context_list(
            value.get("reply_channel_instructions", []),
            field_name="envelope.prompt_context.reply_channel_instructions",
        ),
        task_instructions=_parse_prompt_context_list(
            value.get("task_instructions", []),
            field_name="envelope.prompt_context.task_instructions",
        ),
    )


def _parse_prompt_context_list(value: object, *, field_name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return [_require_non_empty_string(item, field_name=field_name) for item in value]


@dataclass(frozen=True)
class TaskQueueMessage:
    """Message published by ingress and consumed by worker."""

    envelope: TaskEnvelope
    trace_id: str
    task_id: str
    emitted_at: datetime
    schema_version: str = SCHEMA_VERSION
    work_item_id: str | None = None

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"unsupported_schema_version:{self.schema_version}")
        _require_non_empty_string(self.trace_id, field_name="trace_id")
        _require_non_empty_string(self.task_id, field_name="task_id")
        if self.emitted_at.tzinfo is None:
            raise ValueError("emitted_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "message_type": _TASK_MESSAGE_TYPE,
            "trace_id": self.trace_id,
            "task_id": self.task_id,
            "emitted_at": format_utc(self.emitted_at),
            "envelope": self.envelope.to_dict(),
        }
        if self.work_item_id is not None:
            payload["work_item_id"] = self.work_item_id
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TaskQueueMessage":
        if not isinstance(payload, dict):
            raise ValueError("task_message_payload_must_be_object")
        message_type = _require_non_empty_string(
            payload.get("message_type"), field_name="message_type"
        )
        if message_type != _TASK_MESSAGE_TYPE:
            raise ValueError("task_message_type_invalid")

        envelope_payload = payload.get("envelope")
        if not isinstance(envelope_payload, dict):
            raise ValueError("envelope must be an object")

        attachments_payload = envelope_payload.get("attachments", [])
        if not isinstance(attachments_payload, list):
            raise ValueError("envelope.attachments must be a list")

        context_refs_payload = envelope_payload.get("context_refs", [])
        if not isinstance(context_refs_payload, list):
            raise ValueError("envelope.context_refs must be a list")

        reply_channel_payload = envelope_payload.get("reply_channel")
        if not isinstance(reply_channel_payload, dict):
            raise ValueError("envelope.reply_channel must be an object")

        conversations_payload = envelope_payload.get("conversation_channels", [])
        if not isinstance(conversations_payload, list):
            raise ValueError("envelope.conversation_channels must be a list")
        conversations: list[ReplyChannel] = []
        for channel in conversations_payload:
            if not isinstance(channel, dict):
                raise ValueError("envelope.conversation_channels[] must be an object")
            conversations.append(
                ReplyChannel(
                    type=_require_non_empty_string(
                        channel.get("type"), field_name="conversation.type"
                    ),
                    target=_require_non_empty_string(
                        channel.get("target"), field_name="conversation.target"
                    ),
                    metadata=_parse_optional_dict(
                        channel.get("metadata"), field_name="conversation.metadata"
                    ),
                )
            )

        attachments: list[AttachmentRef] = []
        for item in attachments_payload:
            if not isinstance(item, dict):
                raise ValueError("envelope.attachments[] must be an object")
            attachments.append(
                AttachmentRef(
                    uri=_require_non_empty_string(
                        item.get("uri"), field_name="envelope.attachments[].uri"
                    ),
                    name=item.get("name"),
                )
            )

        envelope = TaskEnvelope(
            id=_require_non_empty_string(
                envelope_payload.get("id"), field_name="envelope.id"
            ),
            source=cast(
                SourceType,
                _require_non_empty_string(
                    envelope_payload.get("source"), field_name="envelope.source"
                ),
            ),
            received_at=_parse_utc_datetime(
                envelope_payload.get("received_at"), field_name="envelope.received_at"
            ),
            actor=envelope_payload.get("actor"),
            content=_require_non_empty_string(
                envelope_payload.get("content"), field_name="envelope.content"
            ),
            attachments=attachments,
            context_refs=[
                _require_non_empty_string(item, field_name="envelope.context_refs[]")
                for item in context_refs_payload
            ],
            reply_channel=ReplyChannel(
                type=_require_non_empty_string(
                    reply_channel_payload.get("type"),
                    field_name="envelope.reply_channel.type",
                ),
                target=_require_non_empty_string(
                    reply_channel_payload.get("target"),
                    field_name="envelope.reply_channel.target",
                ),
                metadata=_parse_optional_dict(
                    reply_channel_payload.get("metadata"),
                    field_name="envelope.reply_channel.metadata",
                ),
            ),
            dedupe_key=_require_non_empty_string(
                envelope_payload.get("dedupe_key"),
                field_name="envelope.dedupe_key",
            ),
            conversation_channels=conversations,
            prompt_context=_parse_prompt_context_payload(
                envelope_payload.get("prompt_context")
            ),
            schema_version=_require_non_empty_string(
                envelope_payload.get("schema_version", SCHEMA_VERSION),
                field_name="envelope.schema_version",
            ),
        )

        return cls(
            envelope=envelope,
            trace_id=_require_non_empty_string(
                payload.get("trace_id"), field_name="trace_id"
            ),
            task_id=_require_non_empty_string(
                payload.get("task_id"), field_name="task_id"
            ),
            emitted_at=_parse_utc_datetime(
                payload.get("emitted_at"), field_name="emitted_at"
            ),
            schema_version=_require_non_empty_string(
                payload.get("schema_version", SCHEMA_VERSION),
                field_name="schema_version",
            ),
            work_item_id=_optional_non_empty_string(
                payload.get("work_item_id"), field_name="work_item_id"
            ),
        )

    @classmethod
    def from_envelope(
        cls, envelope: TaskEnvelope, *, trace_id: str
    ) -> "TaskQueueMessage":
        return cls(
            envelope=envelope,
            trace_id=trace_id,
            task_id=_task_id_for_envelope(envelope),
            emitted_at=datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class EgressQueueMessage:
    """Message published by worker and consumed by message-handler egress."""

    task_id: str
    envelope_id: str
    trace_id: str
    message: OutboundMessage
    emitted_at: datetime
    event_id: str
    sequence: int | None = None
    event_kind: str = "message"
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"unsupported_schema_version:{self.schema_version}")
        _require_non_empty_string(self.task_id, field_name="task_id")
        _require_non_empty_string(self.envelope_id, field_name="envelope_id")
        _require_non_empty_string(self.trace_id, field_name="trace_id")
        if self.emitted_at.tzinfo is None:
            raise ValueError("emitted_at must be timezone-aware")
        _require_non_empty_string(self.event_id, field_name="event_id")
        if self.event_kind not in {"message", "completion", "incremental"}:
            raise ValueError("event_kind must be message, completion, or incremental")
        if self.sequence is None:
            if self.event_kind != "incremental":
                raise ValueError(
                    "sequence is required for message and completion events"
                )
            return
        _parse_sequence(self.sequence)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "message_type": _EGRESS_MESSAGE_TYPE_V2,
            "task_id": self.task_id,
            "envelope_id": self.envelope_id,
            "trace_id": self.trace_id,
            "event_id": self.event_id,
            "event_kind": self.event_kind,
            "emitted_at": format_utc(self.emitted_at),
            "message": self.message.to_dict(),
        }
        if self.sequence is not None:
            payload["sequence"] = self.sequence
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EgressQueueMessage":
        if not isinstance(payload, dict):
            raise ValueError("egress_message_payload_must_be_object")
        message_type = _require_non_empty_string(
            payload.get("message_type"), field_name="message_type"
        )
        if message_type != _EGRESS_MESSAGE_TYPE_V2:
            raise ValueError("egress_message_type_invalid")

        message_payload = payload.get("message")
        if not isinstance(message_payload, dict):
            raise ValueError("message must be an object")

        event_id = _require_non_empty_string(
            payload.get("event_id"), field_name="event_id"
        )
        raw_sequence = payload.get("sequence")
        sequence: int | None = None
        if raw_sequence is not None:
            sequence = _parse_sequence(raw_sequence)
        event_kind = _require_non_empty_string(
            payload.get("event_kind"), field_name="event_kind"
        )

        return cls(
            task_id=_require_non_empty_string(
                payload.get("task_id"), field_name="task_id"
            ),
            envelope_id=_require_non_empty_string(
                payload.get("envelope_id"), field_name="envelope_id"
            ),
            trace_id=_require_non_empty_string(
                payload.get("trace_id"), field_name="trace_id"
            ),
            message=OutboundMessage(
                channel=_require_non_empty_string(
                    message_payload.get("channel"), field_name="message.channel"
                ),
                target=_require_non_empty_string(
                    message_payload.get("target"), field_name="message.target"
                ),
                body=_optional_non_empty_string(
                    message_payload.get("body"), field_name="message.body"
                ),
                attachment=_parse_message_attachment(message_payload.get("attachment")),
                metadata=_parse_optional_dict(
                    message_payload.get("metadata"),
                    field_name="message.metadata",
                ),
            ),
            emitted_at=_parse_utc_datetime(
                payload.get("emitted_at"), field_name="emitted_at"
            ),
            event_id=event_id,
            sequence=sequence,
            event_kind=event_kind,
            schema_version=_require_non_empty_string(
                payload.get("schema_version", SCHEMA_VERSION),
                field_name="schema_version",
            ),
        )


def build_task_egress(
    task_message: TaskQueueMessage,
    *,
    sequence: int,
    event_kind: Literal["message", "completion"],
    event_suffix: str,
    body: str,
    emitted_at: datetime,
    channel: str | None = None,
    target: str | None = None,
) -> EgressQueueMessage:
    """Build a worker-originated event, addressed to the task's reply channel by default."""
    reply_channel = task_message.envelope.reply_channel
    return EgressQueueMessage(
        task_id=task_message.task_id,
        envelope_id=task_message.envelope.id,
        trace_id=task_message.trace_id,
        message=OutboundMessage(
            channel=channel or reply_channel.type,
            target=target or reply_channel.target,
            body=body,
        ),
        emitted_at=ensure_utc(emitted_at),
        event_id=f"evt:{task_message.task_id}:{sequence}:{event_kind}:{event_suffix}",
        sequence=sequence,
        event_kind=event_kind,
    )


def _parse_message_attachment(value: object) -> AttachmentRef | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("message.attachment must be an object")
    return AttachmentRef(
        uri=_require_non_empty_string(
            value.get("uri"), field_name="message.attachment.uri"
        ),
        name=_optional_non_empty_string(
            value.get("name"), field_name="message.attachment.name"
        ),
    )


__all__ = [
    "TaskQueueMessage",
    "EgressQueueMessage",
    "build_task_egress",
]
