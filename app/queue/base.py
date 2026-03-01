"""Queue backend interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import TaskEnvelope


@runtime_checkable
class QueueBackend(Protocol):
    """Queue backend used between connectors and workers."""

    def enqueue(self, envelope: TaskEnvelope) -> None:
        """Add one normalized envelope to the queue."""

    def dequeue(self) -> TaskEnvelope | None:
        """Pop one envelope from the queue, or None when empty."""

    def size(self) -> int:
        """Return current queue size."""


__all__ = ["QueueBackend"]
