"""Shared helpers for internal message-handler/worker heartbeat traffic."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import OutboundMessage, ReplyChannel, TaskEnvelope

INTERNAL_SOURCE = "internal"
INTERNAL_REPLY_CHANNEL_TYPE = "internal"
INTERNAL_HEARTBEAT_TARGET = "heartbeat"
INTERNAL_HEARTBEAT_POLICY_PROFILE = "default"


def build_internal_heartbeat_envelope(*, sequence: int, now: datetime) -> TaskEnvelope:
    current_time = _ensure_utc(now)
    heartbeat_id = f"internal-heartbeat:{current_time.isoformat()}:{sequence}"
    return TaskEnvelope(
        id=heartbeat_id,
        source=INTERNAL_SOURCE,
        received_at=current_time,
        actor="message-handler",
        content="internal heartbeat ping",
        attachments=[],
        context_refs=[],
        policy_profile=INTERNAL_HEARTBEAT_POLICY_PROFILE,
        reply_channel=ReplyChannel(
            type=INTERNAL_REPLY_CHANNEL_TYPE,
            target=INTERNAL_HEARTBEAT_TARGET,
        ),
        dedupe_key=heartbeat_id,
    )


def is_internal_heartbeat_envelope(envelope: TaskEnvelope) -> bool:
    return (
        envelope.source == INTERNAL_SOURCE
        and envelope.reply_channel.type == INTERNAL_REPLY_CHANNEL_TYPE
        and envelope.reply_channel.target == INTERNAL_HEARTBEAT_TARGET
    )


def build_internal_heartbeat_egress(
    *,
    task_message: TaskQueueMessage,
    worker_received_at: datetime,
) -> EgressQueueMessage:
    current_time = _ensure_utc(worker_received_at)
    ping_emitted_at = task_message.emitted_at.astimezone(timezone.utc)
    body = json.dumps(
        {
            "kind": "heartbeat_pong",
            "ping_emitted_at": ping_emitted_at.isoformat().replace("+00:00", "Z"),
            "worker_received_at": current_time.isoformat().replace("+00:00", "Z"),
        },
        sort_keys=True,
    )
    return EgressQueueMessage(
        task_id=task_message.task_id,
        envelope_id=task_message.envelope.id,
        trace_id=task_message.trace_id,
        event_index=0,
        event_count=1,
        message=OutboundMessage(channel="log", target=INTERNAL_HEARTBEAT_TARGET, body=body),
        emitted_at=current_time,
        event_id=f"evt:{task_message.task_id}:0:final:internal-heartbeat",
        sequence=0,
        event_kind="final",
        message_type="chatting.egress.v2",
    )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = [
    "INTERNAL_HEARTBEAT_TARGET",
    "INTERNAL_REPLY_CHANNEL_TYPE",
    "INTERNAL_SOURCE",
    "build_internal_heartbeat_egress",
    "build_internal_heartbeat_envelope",
    "is_internal_heartbeat_envelope",
]
