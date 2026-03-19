"""Router interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import RoutedTask, TaskEnvelope


@runtime_checkable
class Router(Protocol):
    """Route normalized envelopes to execution-ready tasks."""

    def route(self, envelope: TaskEnvelope) -> RoutedTask:
        """Return a routed task derived from a canonical envelope."""
        ...


__all__ = ["Router"]
