"""Cheap Prometheus file metrics; no database queries or write locks."""

from pathlib import Path
import json
import sqlite3
from contextlib import closing


def render_sqlite_metrics(db_path: str) -> bytes:
    lines = [
        "# HELP chatting_sqlite_file_size_bytes Apparent SQLite file size in bytes.",
        "# TYPE chatting_sqlite_file_size_bytes gauge",
        "# HELP chatting_sqlite_file_stat_success Whether the SQLite file size could be read (absent optional sidecars count as success).",
        "# TYPE chatting_sqlite_file_stat_success gauge",
    ]
    for name, suffix in (("database", ""), ("wal", "-wal"), ("shm", "-shm")):
        labels = f'component="worker",file="{name}"'
        success = True
        size = 0
        try:
            size = Path(db_path + suffix).stat().st_size
        except FileNotFoundError:
            success = bool(suffix)
        except OSError:
            success = False
        if success:
            lines.append(f"chatting_sqlite_file_size_bytes{{{labels}}} {size}")
        lines.append(f"chatting_sqlite_file_stat_success{{{labels}}} {int(success)}")
    lines.extend(_quota_metrics(db_path))
    return ("\n".join(lines) + "\n").encode()


def _quota_metrics(db_path: str) -> list[str]:
    lines = [
        "# HELP chatting_codex_quota_snapshot_timestamp_seconds Time of the last successful Codex usage read.",
        "# TYPE chatting_codex_quota_snapshot_timestamp_seconds gauge",
        "# HELP chatting_codex_quota_blocked Whether Codex reports ordinary usage is blocked.",
        "# TYPE chatting_codex_quota_blocked gauge",
        "# HELP chatting_codex_quota_remaining_percent Remaining percentage of a Codex usage window.",
        "# TYPE chatting_codex_quota_remaining_percent gauge",
        "# HELP chatting_codex_quota_reset_timestamp_seconds Reset time of a Codex usage window.",
        "# TYPE chatting_codex_quota_reset_timestamp_seconds gauge",
        "# HELP chatting_codex_quota_interrupted_tasks Tasks requiring review after quota interruption.",
        "# TYPE chatting_codex_quota_interrupted_tasks gauge",
    ]
    try:
        with closing(
            sqlite3.connect(
                Path(db_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=1
            )
        ) as db:
            row = db.execute(
                "SELECT snapshot_json, observed_at FROM codex_quota_snapshot WHERE id = 1"
            ).fetchone()
            blocked = (
                db.execute("SELECT 1 FROM codex_quota_gate WHERE id=1").fetchone()
                is not None
            )
            interrupted = db.execute(
                "SELECT COUNT(*) FROM worker_inbox WHERE state='quota_interrupted'"
            ).fetchone()[0]
        lines.append(f"chatting_codex_quota_interrupted_tasks {interrupted}")
        if row is None:
            if blocked:
                lines.append("chatting_codex_quota_blocked 1")
            return lines
        snapshot, observed = json.loads(row[0]), float(row[1])
        lines.append(f"chatting_codex_quota_snapshot_timestamp_seconds {observed}")
        allowed = snapshot.get("ordinary_usage_allowed")
        if blocked or type(allowed) is bool:
            lines.append(f"chatting_codex_quota_blocked {int(blocked or not allowed)}")
        for window in snapshot.get("windows", []):
            minutes, used, reset = (
                window.get("duration_minutes"),
                window.get("used_percent"),
                window.get("resets_at"),
            )
            if (
                type(minutes) is not int
                or minutes <= 0
                or type(used) not in (int, float)
                or not 0 <= used <= 100
            ):
                continue
            label = f'window_minutes="{minutes}"'
            lines.append(
                f"chatting_codex_quota_remaining_percent{{{label}}} {100 - used}"
            )
            if type(reset) is int:
                lines.append(
                    f"chatting_codex_quota_reset_timestamp_seconds{{{label}}} {reset}"
                )
    except OSError, ValueError, TypeError, AttributeError, sqlite3.Error:
        pass
    return lines
