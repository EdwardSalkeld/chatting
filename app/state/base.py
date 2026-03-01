"""State store interface contracts."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.models import (
    AuditEvent,
    DeadLetterRecord,
    PendingApprovalRecord,
    RunRecord,
    TaskEnvelope,
)


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

    def append_dead_letter(
        self,
        *,
        run_id: str,
        envelope: TaskEnvelope,
        reason_codes: list[str],
        last_error: str | None,
        attempt_count: int,
    ) -> int:
        """Persist one dead-letter entry and return its record ID."""

    def list_dead_letters(self, *, status: str | None = None) -> list[DeadLetterRecord]:
        """Return dead-letter entries in storage order."""

    def mark_dead_letter_replayed(self, dead_letter_id: int, replayed_run_id: str) -> None:
        """Mark a dead-letter entry as replayed."""

    def append_pending_approval(
        self,
        *,
        run_id: str,
        envelope_id: str,
        config_path: str,
        config_value: object,
    ) -> int:
        """Persist one pending human-approval item and return its ID."""

    def list_pending_approvals(self, *, status: str | None = None) -> list[PendingApprovalRecord]:
        """Return pending-approval entries in storage order."""

    def resolve_pending_approval(self, approval_id: int, status: str) -> None:
        """Mark one pending-approval item as approved or rejected."""


__all__ = ["StateStore"]
