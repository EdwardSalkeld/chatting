"""Fake email connector used by bootstrap flow and tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.models import ReplyChannel, TaskEnvelope


@dataclass(frozen=True)
class EmailMessage:
    """In-memory representation of a polled email message."""

    provider_message_id: str
    from_address: str
    subject: str
    body: str
    received_at: datetime
    context_refs: list[str]


class FakeEmailConnector:
    """Convert fake email payloads into canonical task envelopes."""

    source = "email"

    def __init__(self, messages: list[EmailMessage]) -> None:
        self._messages = messages

    def poll(self) -> list[TaskEnvelope]:
        envelopes: list[TaskEnvelope] = []
        for message in self._messages:
            received_at = _ensure_utc(message.received_at)
            event_id = f"email:{message.provider_message_id}"
            content = f"Subject: {message.subject}\n\n{message.body}"
            envelopes.append(
                TaskEnvelope(
                    id=event_id,
                    source="email",
                    received_at=received_at,
                    actor=message.from_address,
                    content=content,
                    attachments=[],
                    context_refs=message.context_refs,
                    reply_channel=ReplyChannel(type="email", target=message.from_address),
                    dedupe_key=event_id,
                )
            )
        return envelopes



def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("received_at must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = ["EmailMessage", "FakeEmailConnector"]
