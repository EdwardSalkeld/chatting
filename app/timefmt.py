"""UTC timestamp helpers shared by the wire contracts and the worker database."""

from __future__ import annotations

from datetime import datetime, timezone


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


def format_utc(value: datetime) -> str:
    """Format as RFC 3339 in UTC with a ``Z`` suffix."""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str, *, field_name: str = "timestamp") -> datetime:
    """Parse an RFC 3339 timestamp, which must carry an offset, into UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)
