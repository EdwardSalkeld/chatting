"""In-memory queue backend."""

from __future__ import annotations

from collections import deque

from app.models import TaskEnvelope


class InMemoryQueueBackend:
    """Simple FIFO queue implementation for single-process runs."""

    def __init__(self) -> None:
        self._queue: deque[TaskEnvelope] = deque()

    def enqueue(self, envelope: TaskEnvelope) -> None:
        self._queue.append(envelope)

    def dequeue(self) -> TaskEnvelope | None:
        if not self._queue:
            return None
        return self._queue.popleft()

    def size(self) -> int:
        return len(self._queue)


__all__ = ["InMemoryQueueBackend"]
