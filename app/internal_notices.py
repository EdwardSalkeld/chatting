"""Shared helpers for deterministic internal notice tasks."""

from __future__ import annotations

from app.models import TaskEnvelope

INTERNAL_NOTICE_METADATA_KEY = "internal_notice"
TELEGRAM_CHANNEL_NOT_ENABLED_NOTICE = "telegram_channel_not_enabled"
TELEGRAM_GROUP_NOT_ENABLED_NOTICE = "telegram_group_not_enabled"


def is_internal_telegram_channel_not_enabled_envelope(
    envelope: TaskEnvelope,
) -> bool:
    return (
        envelope.source == "internal"
        and envelope.reply_channel.type == "telegram"
        and envelope.reply_channel.metadata.get(INTERNAL_NOTICE_METADATA_KEY)
        in {
            TELEGRAM_CHANNEL_NOT_ENABLED_NOTICE,
            TELEGRAM_GROUP_NOT_ENABLED_NOTICE,
        }
    )
