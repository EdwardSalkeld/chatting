"""Cheap Prometheus file metrics; no database queries or write locks."""

from pathlib import Path


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
    return ("\n".join(lines) + "\n").encode()
