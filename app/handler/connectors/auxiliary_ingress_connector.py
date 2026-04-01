"""Connector for auxiliary ingress events delivered over BBMB."""

from __future__ import annotations

import json
from typing import Any

from app.broker import (
    AuxiliaryIngressQueueMessage,
    BBMBQueueAdapter,
)
from app.models import PromptContext, ReplyChannel, TaskEnvelope


class AuxiliaryIngressConnector:
    """Drain raw auxiliary ingress JSON bodies into webhook-style task envelopes."""

    source = "webhook"

    def __init__(
        self,
        *,
        broker: BBMBQueueAdapter,
        queue_name: str,
        reply_target: str = "generic_post",
        context_refs: list[str] | None = None,
        prompt_context: PromptContext | None = None,
    ) -> None:
        self._broker = broker
        self._queue_name = queue_name
        self._reply_target = reply_target
        self._context_refs = list(context_refs or [])
        self._prompt_context = prompt_context or PromptContext()
        self._pending_by_envelope_id: dict[str, str] = {}
        self._pending_envelopes: dict[str, TaskEnvelope] = {}

    def poll(self) -> list[TaskEnvelope]:
        envelopes = list(self._pending_envelopes.values())
        while True:
            picked = self._broker.pickup_json(
                self._queue_name,
                timeout_seconds=0,
                wait_seconds=0,
            )
            if picked is None:
                return envelopes
            message = AuxiliaryIngressQueueMessage.from_dict(picked.payload)
            envelope = TaskEnvelope(
                id=f"webhook:{message.event_id}",
                source="webhook",
                received_at=message.received_at,
                actor=None,
                content=_render_body_content(message.body),
                attachments=[],
                context_refs=list(self._context_refs),
                reply_channel=ReplyChannel(type="webhook", target=self._reply_target),
                dedupe_key=f"webhook:{message.event_id}",
                prompt_context=self._prompt_context,
            )
            if envelope.id in self._pending_envelopes:
                continue
            self._pending_by_envelope_id[envelope.id] = picked.guid
            self._pending_envelopes[envelope.id] = envelope
            envelopes.append(envelope)

    def ack_envelope(self, envelope_id: str) -> None:
        guid = self._pending_by_envelope_id.pop(envelope_id, None)
        self._pending_envelopes.pop(envelope_id, None)
        if guid is None:
            return
        self._broker.ack(self._queue_name, guid)


def _render_body_content(body: Any) -> str:
    if isinstance(body, (dict, list)):
        return json.dumps(body, indent=2, sort_keys=True)
    return json.dumps(body)


__all__ = ["AuxiliaryIngressConnector"]
