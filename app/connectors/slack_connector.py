"""Slack polling connector."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from app.models import ReplyChannel, TaskEnvelope


class SlackConnector:
    """Normalize Slack messages into canonical IM task envelopes."""

    source = "im"

    def __init__(
        self,
        *,
        fetch_messages: Callable[[], list[dict[str, object]]],
        context_refs: list[str] | None = None,
        allowed_channel_ids: list[str] | None = None,
    ) -> None:
        self._fetch_messages = fetch_messages
        self._context_refs = list(context_refs or [])
        self._allowed_channel_ids = set(allowed_channel_ids or [])

    def poll(self) -> list[TaskEnvelope]:
        envelopes: list[TaskEnvelope] = []
        for payload in self._fetch_messages():
            envelope = self._to_envelope(payload)
            if envelope is not None:
                envelopes.append(envelope)
        return envelopes

    def _to_envelope(self, payload: dict[str, object]) -> TaskEnvelope | None:
        message_id = payload.get("id")
        user_id = payload.get("user")
        channel_id = payload.get("channel")
        text = payload.get("text")
        if not isinstance(message_id, str) or not message_id.strip():
            return None
        if not isinstance(user_id, str) or not user_id.strip():
            return None
        if not isinstance(channel_id, str) or not channel_id.strip():
            return None
        if not isinstance(text, str) or not text.strip():
            return None
        if self._allowed_channel_ids and channel_id not in self._allowed_channel_ids:
            return None

        event_id = f"slack:{message_id}"
        return TaskEnvelope(
            id=event_id,
            source="im",
            received_at=_parse_slack_timestamp(payload.get("ts")),
            actor=user_id,
            content=text.strip(),
            attachments=[],
            context_refs=self._context_refs,
            reply_channel=ReplyChannel(type="slack", target=channel_id),
            dedupe_key=event_id,
        )


def _parse_slack_timestamp(raw_value: object) -> datetime:
    if isinstance(raw_value, str):
        try:
            return datetime.fromtimestamp(float(raw_value), tz=timezone.utc)
        except ValueError:
            pass
    if isinstance(raw_value, (int, float)):
        return datetime.fromtimestamp(float(raw_value), tz=timezone.utc)
    return datetime.now(timezone.utc)


__all__ = ["SlackConnector"]
