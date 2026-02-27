"""State store interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import AuditEvent, RunRecord


@runtime_checkable
class StateStore(Protocol):
    """Persistence boundary for idempotency, run records, and audit events."""

    def seen(self, source: str, dedupe_key: str) -> bool:
        """Return whether the source-scoped dedupe key has been observed before."""

    def mark_seen(self, source: str, dedupe_key: str) -> None:
        """Persist a source-scoped dedupe key as seen."""

    def append_run(self, record: RunRecord) -> None:
        """Persist one run record."""

    def list_runs(self) -> list[RunRecord]:
        """Return run records in storage order."""

    def append_audit_event(self, event: AuditEvent) -> None:
        """Persist one audit event."""

    def list_audit_events(self) -> list[AuditEvent]:
        """Return persisted audit events in storage order."""


__all__ = ["StateStore"]
