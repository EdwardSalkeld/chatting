"""Webhook event connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.models import ReplyChannel, TaskEnvelope


@dataclass(frozen=True)
class WebhookEvent:
    """One inbound webhook event payload."""

    event_id: str
    actor: str | None
    content: str
    received_at: datetime
    reply_target: str
    context_refs: list[str]


class WebhookConnector:
    """Drain queued webhook events into canonical webhook envelopes."""

    source = "webhook"

    def __init__(self, *, events: list[WebhookEvent] | None = None) -> None:
        self._events = list(events or [])

    def enqueue(self, event: WebhookEvent) -> None:
        self._events.append(event)

    def poll(self) -> list[TaskEnvelope]:
        envelopes: list[TaskEnvelope] = []
        while self._events:
            event = self._events.pop(0)
            envelopes.append(
                TaskEnvelope(
                    id=f"webhook:{event.event_id}",
                    source="webhook",
                    received_at=_ensure_utc(event.received_at),
                    actor=event.actor,
                    content=event.content,
                    attachments=[],
                    context_refs=event.context_refs,
                    reply_channel=ReplyChannel(type="webhook", target=event.reply_target),
                    dedupe_key=f"webhook:{event.event_id}",
                )
            )
        return envelopes


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("received_at must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = ["WebhookConnector", "WebhookEvent"]
