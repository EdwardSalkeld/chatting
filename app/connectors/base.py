"""Connector interface contracts."""

from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable

from app.models import TaskEnvelope


@runtime_checkable
class Connector(Protocol):
    """Polling connector contract that emits normalized task envelopes."""

    def poll(self) -> Iterable[TaskEnvelope]:
        """Fetch and normalize available source events."""


__all__ = ["Connector"]
