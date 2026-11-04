import urllib.request
from pathlib import Path
from unittest.mock import patch

from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor, start_worker_activity_server
from app.worker.metrics import render_sqlite_metrics


def test_worker_metrics_endpoint(tmp_path: Path) -> None:
    path = tmp_path / "worker.db"
    store = SQLiteStateStore(str(path))
    server = start_worker_activity_server(
        host="127.0.0.1", port=0, monitor=WorkerActivityMonitor(store=store)
    )
    try:
        host, port = server.server.server_address
        with urllib.request.urlopen(f"http://{host}:{port}/metrics") as response:
            assert response.headers["Content-Type"].startswith(
                "text/plain; version=0.0.4"
            )
            body = response.read().decode()
        assert (
            f'chatting_sqlite_file_size_bytes{{component="worker",file="database"}} {path.stat().st_size}'
            in body
        )
        assert (
            'chatting_sqlite_file_size_bytes{component="worker",file="wal"} 0' in body
        )
        wal = Path(str(path) + "-wal")
        wal.write_bytes(b"journal")
        assert 'file="wal"} 7' in render_sqlite_metrics(str(path)).decode()
        wal.unlink()
        assert (
            'chatting_sqlite_file_size_bytes{component="worker",file="wal"} 0'
            in render_sqlite_metrics(str(path)).decode()
        )
    finally:
        server.shutdown()


def test_stat_failure_is_not_zero_size(tmp_path: Path) -> None:
    for error in (PermissionError(), FileNotFoundError()):
        with patch("app.worker.metrics.Path.stat", side_effect=error):
            body = render_sqlite_metrics(str(tmp_path / "worker.db")).decode()
        assert (
            'chatting_sqlite_file_stat_success{component="worker",file="database"} 0'
            in body
        )
        assert (
            'chatting_sqlite_file_size_bytes{component="worker",file="database"}'
            not in body
        )
