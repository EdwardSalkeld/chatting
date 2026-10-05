"""Bounded Jev decision for work items explicitly set to auto."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass

from app.models import TaskEnvelope
from app.state import SQLiteStateStore

_ENDPOINT = "https://api.typesafe.ai/v1/systemone"
_MODEL = "jev-latest"
_LOW_THRESHOLD = 0.80


@dataclass(frozen=True)
class ModelDecision:
    tier: str
    reason: str
    confidence: float | None = None
    probabilities: dict[str, float] | None = None
    provider_model: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def choose_model(*, store: SQLiteStateStore, envelope: TaskEnvelope) -> ModelDecision:
    """Select Luna only for a confident low-risk choice; otherwise use Sol."""
    key = os.environ.get("TYPESAFE_API_KEY", "").strip()
    if not key:
        return ModelDecision("high", "jev_key_missing")

    history = []
    metadata = envelope.reply_channel.metadata
    message_id = metadata.get("message_id")
    topic_id = metadata.get("message_thread_id")
    if (
        envelope.reply_channel.type == "telegram"
        and isinstance(message_id, int)
        and not isinstance(message_id, bool)
        and message_id > 0
    ):
        history = store.list_recent_telegram_history(
            target=envelope.reply_channel.target,
            topic_id=topic_id if isinstance(topic_id, int) and topic_id > 0 else None,
            before_message_id=message_id,
            limit=30,
        )
    state = {
        "recent_conversation": [
            {"sender": turn.sender, "content": (turn.content or "")[:1000]}
            for turn in history
        ],
        "current_request": envelope.content[:8000],
        "source": envelope.source,
        "attachment_count": len(envelope.attachments),
    }
    request = {
        "model": _MODEL,
        "state": state,
        "questions": {
            "executor_tier": {
                "type": "choice",
                "instructions": (
                    "Choose the least costly model likely to complete the current request "
                    "well, considering the recent conversation. Choose high if the "
                    "request is ambiguous, consequential, broad, multi-step, requires "
                    "complex coding or debugging, or may need significant tool use. "
                    "Choose low only for clear, bounded, routine work."
                ),
                "criteria": {
                    "low": "Luna can reliably finish this clear, bounded, routine task.",
                    "high": "Sol is warranted by complexity, risk, ambiguity, or breadth.",
                },
            }
        },
    }
    try:
        data = json.dumps(request).encode("utf-8")
        http_request = urllib.request.Request(
            _ENDPOINT,
            data=data,
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(http_request, timeout=5) as response:
            result = json.load(response)
        answer = result["answers"]["executor_tier"]
        probabilities = answer["probabilities"]
        if (
            answer["type"] != "choice"
            or answer["choice"] not in ("high", "low")
            or not isinstance(probabilities, dict)
            or set(probabilities) != {"high", "low"}
            or any(
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not 0 <= value <= 1
                for value in probabilities.values()
            )
        ):
            raise ValueError("invalid Jev choice")
        confidence = float(answer["confidence"])
        if not 0 <= confidence <= 1:
            raise ValueError("invalid Jev confidence")
        tier = (
            "low"
            if answer["choice"] == "low"
            and probabilities["low"] >= _LOW_THRESHOLD
            and confidence >= _LOW_THRESHOLD
            else "high"
        )
        return ModelDecision(
            tier=tier,
            reason="jev_confident_low" if tier == "low" else "jev_high_or_uncertain",
            confidence=confidence,
            probabilities={k: float(v) for k, v in probabilities.items()},
            provider_model=str(result.get("model", _MODEL)),
        )
    except (OSError, ValueError, KeyError, TypeError, urllib.error.HTTPError):
        return ModelDecision("high", "jev_unavailable_or_invalid")
