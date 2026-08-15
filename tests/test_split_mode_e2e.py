import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.state import SQLiteStateStore
from tests.e2e.handler_selector import message_handler_command


def _seed_schedule(
    handler_db_path: Path,
    *,
    job_name: str,
    content: str,
    cron: str,
    context_refs: list[str],
    reply_channel_type: str,
    reply_channel_target: str,
) -> None:
    # The handler's schedule connector reads active schedules from its DB, not
    # from a file, so seed the row the same way the API/UI would. Column layout
    # mirrors the handler's schedules table; created_at uses RFC3339 so the Go
    # store's parseTimestamp accepts it.
    now = datetime.now(timezone.utc).isoformat()
    connection = sqlite3.connect(str(handler_db_path))
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schedules (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                status TEXT NOT NULL,
                job_name TEXT NOT NULL,
                content TEXT NOT NULL,
                cron TEXT NOT NULL,
                timezone TEXT NOT NULL,
                context_refs TEXT NOT NULL,
                prompt_context TEXT NOT NULL,
                reply_channel_type TEXT NOT NULL,
                reply_channel_target TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                superseded_at TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO schedules (
                schedule_id, version, status, job_name, content, cron, timezone,
                context_refs, prompt_context, reply_channel_type,
                reply_channel_target, created_at, created_by, superseded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "sched_ci_split_smoke",
                1,
                "active",
                job_name,
                content,
                cron,
                "UTC",
                json.dumps(context_refs),
                "[]",
                reply_channel_type,
                reply_channel_target,
                now,
                "test",
                None,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _seed_due_reminder(handler_db_path: Path, *, context_refs: list[str]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    connection = sqlite3.connect(str(handler_db_path))
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                reminder_id TEXT NOT NULL,
                revision INTEGER NOT NULL,
                status TEXT NOT NULL,
                run_at TEXT NOT NULL,
                prompt TEXT NOT NULL,
                context_refs TEXT NOT NULL,
                prompt_context TEXT NOT NULL,
                reply_channel_type TEXT NOT NULL,
                reply_channel_target TEXT NOT NULL,
                reply_channel_metadata TEXT NOT NULL,
                created_from_task_id TEXT NOT NULL,
                created_by TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                request_fingerprint TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                fired_at TEXT,
                cancelled_at TEXT,
                UNIQUE (reminder_id, revision)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO reminders (
                reminder_id, revision, status, run_at, prompt, context_refs,
                prompt_context, reply_channel_type, reply_channel_target,
                reply_channel_metadata, created_from_task_id, created_by,
                idempotency_key, request_fingerprint, created_at, updated_at,
                fired_at, cancelled_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "rem_ci_split_smoke",
                1,
                "scheduled",
                now,
                "CI reminder smoke task",
                json.dumps(context_refs),
                "[]",
                "log",
                "ci-reminder-smoke",
                "{}",
                "task:test:seed",
                "test",
                "test:reminder:seed",
                "seed",
                now,
                now,
                None,
                None,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        return probe.connect_ex((host, port)) == 0


def _wait_for_port(host: str, port: int, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _is_port_open(host, port):
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {host}:{port}")


def _reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        return int(listener.getsockname()[1])


class SplitModeE2ETests(unittest.TestCase):
    def test_split_mode_roundtrip_with_real_bbmb_server(self) -> None:
        server_bin_raw = os.environ.get("CHATTING_BBMB_SERVER_BIN", "").strip()
        if not server_bin_raw:
            self.skipTest("CHATTING_BBMB_SERVER_BIN is not set")

        server_bin = Path(server_bin_raw)
        if not server_bin.exists():
            self.skipTest(f"bbmb server binary not found: {server_bin}")

        repo_root = Path(__file__).resolve().parent.parent
        fake_codex = str(repo_root / "tests" / "e2e" / "fake_codex.py")
        bbmb_port = _reserve_port()
        bbmb_metrics_port = _reserve_port()
        handler_metrics_port = _reserve_port()
        egress_http_port = _reserve_port()
        bbmb_address = f"127.0.0.1:{bbmb_port}"
        server_proc: subprocess.Popen[str] | None = None
        worker_proc: subprocess.Popen[str] | None = None
        handler_proc: subprocess.Popen[str] | None = None

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            handler_db_path = temp_root / "handler.db"
            worker_db_path = temp_root / "worker.db"
            handler_config_path = temp_root / "message-handler.json"
            worker_config_path = temp_root / "worker.json"

            handler_config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(handler_db_path),
                        "bbmb_address": bbmb_address,
                        "poll_interval_seconds": 0.1,
                        "poll_timeout_seconds": 1,
                        "metrics_port": handler_metrics_port,
                        "allowed_egress_channels": ["log"],
                        "egress_http_port": egress_http_port,
                    }
                ),
                encoding="utf-8",
            )

            # The connector reads active schedules from the handler DB (not a
            # file), so seed the smoke schedule before the handler starts.
            _seed_schedule(
                handler_db_path,
                job_name="ci-split-smoke",
                content="CI smoke task",
                cron="* * * * *",
                context_refs=[f"repo:{repo_root}"],
                reply_channel_type="log",
                reply_channel_target="ci-split-smoke",
            )
            _seed_due_reminder(
                handler_db_path,
                context_refs=[f"repo:{repo_root}"],
            )
            worker_config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(worker_db_path),
                        "bbmb_address": bbmb_address,
                        "max_attempts": 2,
                        "poll_timeout_seconds": 1,
                        "sleep_seconds": 0.05,
                        "max_loops": 20,
                        "activity_port": 0,
                        "codex_command": f"{sys.executable} {fake_codex}",
                        "handler_egress_url": f"http://127.0.0.1:{egress_http_port}/egress",
                        "handler_api_url": f"http://127.0.0.1:{handler_metrics_port}",
                    }
                ),
                encoding="utf-8",
            )

            try:
                server_proc = subprocess.Popen(
                    [
                        str(server_bin),
                        f"--port={bbmb_port}",
                        f"--metrics-port={bbmb_metrics_port}",
                    ],
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                _wait_for_port("127.0.0.1", bbmb_port, timeout_seconds=5.0)

                handler_proc = subprocess.Popen(
                    message_handler_command(handler_config_path),
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                _wait_for_port("127.0.0.1", egress_http_port, timeout_seconds=30.0)

                worker_proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "app.main_worker",
                        "--config",
                        str(worker_config_path),
                    ],
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

                # Egress is delivered synchronously worker -> handler, so the
                # handler must stay up until the worker is done. The handler runs
                # until terminated (no max_loops); wait for the bounded worker
                # first, then stop the handler and collect its output.
                worker_out, worker_err = worker_proc.communicate(timeout=45)
                handler_proc.terminate()
                handler_out, handler_err = handler_proc.communicate(timeout=15)
            finally:
                for proc in (handler_proc, worker_proc, server_proc):
                    if proc is None:
                        continue
                    if proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
                    if proc.stdout is not None:
                        proc.stdout.close()
                    if proc.stderr is not None:
                        proc.stderr.close()

            # The handler runs until we terminate it, so a clean self-exit (0)
            # or the SIGTERM we send are both fine; anything else is a crash.
            self.assertIn(
                handler_proc.returncode,
                (0, -signal.SIGTERM),
                msg=f"message-handler exited unexpectedly\nstdout:\n{handler_out}\nstderr:\n{handler_err}",
            )
            self.assertEqual(
                worker_proc.returncode,
                0,
                msg=f"worker exited non-zero\nstdout:\n{worker_out}\nstderr:\n{worker_err}",
            )

            worker_store = SQLiteStateStore(str(worker_db_path))
            worker_runs = worker_store.list_runs()
            matching_worker_runs = [
                run
                for run in worker_runs
                if run.envelope_id.startswith("cron:ci-split-smoke:")
            ]
            self.assertTrue(
                matching_worker_runs,
                msg="missing worker run for cron:ci-split-smoke",
            )
            self.assertTrue(
                any(
                    run.envelope_id == "reminder:rem_ci_split_smoke:1"
                    and run.result_status == "success"
                    for run in worker_runs
                ),
                msg="missing successful worker run for one-off reminder",
            )
            expected_envelope_id = matching_worker_runs[0].envelope_id
            self.assertTrue(
                any(
                    run.envelope_id == expected_envelope_id
                    and run.result_status == "success"
                    for run in worker_runs
                ),
                msg=f"missing successful worker run for expected envelope_id={expected_envelope_id!r}",
            )

            handler_store = SQLiteStateStore(str(handler_db_path))
            expected_task_id = f"task:{expected_envelope_id}"
            expected_event_id = f"evt:{expected_task_id}:0:completion:internal"
            self.assertEqual(
                handler_store.list_dispatched_event_indices(run_id=expected_task_id), []
            )
            self.assertTrue(
                handler_store.has_dispatched_event_id(
                    task_id=expected_task_id,
                    event_id=expected_event_id,
                )
            )
            with sqlite3.connect(str(handler_db_path)) as connection:
                reminder_status, fired_at = connection.execute(
                    "SELECT status, fired_at FROM reminders WHERE reminder_id = ? AND revision = ?",
                    ("rem_ci_split_smoke", 1),
                ).fetchone()
            self.assertEqual(reminder_status, "fired")
            self.assertIsNotNone(fired_at)


if __name__ == "__main__":
    unittest.main()
