"""Shared helpers for internal message-handler/worker heartbeat traffic."""

from __future__ import annotations

from app.models import TaskEnvelope

INTERNAL_SOURCE = "internal"
INTERNAL_REPLY_CHANNEL_TYPE = "internal"
INTERNAL_HEARTBEAT_TARGET = "heartbeat"


def is_internal_heartbeat_envelope(envelope: TaskEnvelope) -> bool:
    return (
        envelope.source == INTERNAL_SOURCE
        and envelope.reply_channel.type == INTERNAL_REPLY_CHANNEL_TYPE
        and envelope.reply_channel.target == INTERNAL_HEARTBEAT_TARGET
    )


__all__ = [
    "INTERNAL_HEARTBEAT_TARGET",
    "INTERNAL_REPLY_CHANNEL_TYPE",
    "INTERNAL_SOURCE",
    "is_internal_heartbeat_envelope",
]
