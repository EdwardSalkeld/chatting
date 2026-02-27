"""Connector interface contracts."""

from __future__ import annotations

from typing import Protocol

from app.models import TaskEnvelope


class Connector(Protocol):
    """Polling connector contract that emits normalized task envelopes."""

    def poll(self) -> list[TaskEnvelope]:
        """Fetch and normalize available source events."""


__all__ = ["Connector"]
