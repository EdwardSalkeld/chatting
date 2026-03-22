"""Always-on internal heartbeat connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from app.internal_heartbeat import build_internal_heartbeat_envelope
from app.models import TaskEnvelope


@dataclass
class InternalHeartbeatConnector:
    """Emit one internal heartbeat envelope every poll cycle."""

    now_provider: Callable[[], datetime] | None = None

    def __post_init__(self) -> None:
        self._now_provider = self.now_provider or (lambda: datetime.now(timezone.utc))
        self._sequence = 0

    def poll(self) -> list[TaskEnvelope]:
        self._sequence += 1
        return [
            build_internal_heartbeat_envelope(
                sequence=self._sequence,
                now=self._now_provider(),
            )
        ]


__all__ = ["InternalHeartbeatConnector"]
