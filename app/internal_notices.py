"""Shared helpers for deterministic internal notice tasks."""

from __future__ import annotations

from datetime import datetime, timezone

from app.broker import EgressQueueMessage, TaskQueueMessage
from app.models import OutboundMessage, TaskEnvelope

INTERNAL_NOTICE_METADATA_KEY = "internal_notice"
TELEGRAM_CHANNEL_NOT_ENABLED_NOTICE = "telegram_channel_not_enabled"


def is_internal_telegram_channel_not_enabled_envelope(
    envelope: TaskEnvelope,
) -> bool:
    return (
        envelope.source == "internal"
        and envelope.reply_channel.type == "telegram"
        and envelope.reply_channel.metadata.get(INTERNAL_NOTICE_METADATA_KEY)
        == TELEGRAM_CHANNEL_NOT_ENABLED_NOTICE
    )


def build_internal_telegram_channel_not_enabled_egress(
    *,
    task_message: TaskQueueMessage,
    emitted_at: datetime,
) -> EgressQueueMessage:
    current_time = _ensure_utc(emitted_at)
    return EgressQueueMessage(
        task_id=task_message.task_id,
        envelope_id=task_message.envelope.id,
        trace_id=task_message.trace_id,
        event_index=0,
        event_count=2,
        message=OutboundMessage(
            channel=task_message.envelope.reply_channel.type,
            target=task_message.envelope.reply_channel.target,
            body=task_message.envelope.content,
        ),
        emitted_at=current_time,
        event_id=f"evt:{task_message.task_id}:0:message:telegram-channel-not-enabled",
        sequence=0,
        event_kind="message",
        message_type="chatting.egress.v2",
    )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)
