import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from app.state import SQLiteStateStore


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


class SplitModeE2ETests(unittest.TestCase):
    def test_split_mode_roundtrip_with_real_bbmb_server_and_stub_executor(self) -> None:
        server_bin_raw = os.environ.get("CHATTING_BBMB_SERVER_BIN", "").strip()
        if not server_bin_raw:
            self.skipTest("CHATTING_BBMB_SERVER_BIN is not set")

        server_bin = Path(server_bin_raw)
        if not server_bin.exists():
            self.skipTest(f"bbmb server binary not found: {server_bin}")

        if _is_port_open("127.0.0.1", 9876):
            self.skipTest("127.0.0.1:9876 already in use")

        repo_root = Path(__file__).resolve().parent.parent
        server_proc: subprocess.Popen[str] | None = None
        worker_proc: subprocess.Popen[str] | None = None
        handler_proc: subprocess.Popen[str] | None = None

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            handler_db_path = temp_root / "handler.db"
            worker_db_path = temp_root / "worker.db"
            schedule_path = temp_root / "schedule.json"
            handler_config_path = temp_root / "message-handler.json"
            worker_config_path = temp_root / "worker.json"

            schedule_payload = [
                {
                    "job_name": "ci-split-smoke",
                    "content": "CI smoke task",
                    "interval_seconds": 3600,
                    "context_refs": ["repo:/workspace/chatting"],
                    "start_at": "2026-01-01T00:00:00Z",
                    "reply_channel_type": "log",
                    "reply_channel_target": "ci-split-smoke",
                }
            ]
            schedule_path.write_text(json.dumps(schedule_payload), encoding="utf-8")

            handler_config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(handler_db_path),
                        "bbmb_address": "127.0.0.1:9876",
                        "poll_interval_seconds": 0.1,
                        "poll_timeout_seconds": 1,
                        "max_loops": 20,
                        "allowed_egress_channels": ["log"],
                        "schedule_file": str(schedule_path),
                    }
                ),
                encoding="utf-8",
            )
            worker_config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(worker_db_path),
                        "bbmb_address": "127.0.0.1:9876",
                        "max_attempts": 2,
                        "poll_timeout_seconds": 1,
                        "sleep_seconds": 0.05,
                        "max_loops": 20,
                        "use_stub_executor": True,
                    }
                ),
                encoding="utf-8",
            )

            expected_envelope_id = "cron:ci-split-smoke:2026-01-01T00:00:00+00:00"
            expected_task_id = f"task:{expected_envelope_id}"
            expected_event_id = f"evt:{expected_task_id}:0:final"

            try:
                server_proc = subprocess.Popen(
                    [str(server_bin)],
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                _wait_for_port("127.0.0.1", 9876, timeout_seconds=5.0)

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
                handler_proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "app.main_message_handler",
                        "--config",
                        str(handler_config_path),
                    ],
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

                handler_out, handler_err = handler_proc.communicate(timeout=45)
                worker_out, worker_err = worker_proc.communicate(timeout=45)
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

            self.assertEqual(
                handler_proc.returncode,
                0,
                msg=f"message-handler exited non-zero\nstdout:\n{handler_out}\nstderr:\n{handler_err}",
            )
            self.assertEqual(
                worker_proc.returncode,
                0,
                msg=f"worker exited non-zero\nstdout:\n{worker_out}\nstderr:\n{worker_err}",
            )

            worker_store = SQLiteStateStore(str(worker_db_path))
            worker_runs = worker_store.list_runs()
            self.assertTrue(
                any(run.envelope_id == expected_envelope_id for run in worker_runs),
                msg=f"missing worker run for expected envelope_id={expected_envelope_id!r}",
            )
            self.assertTrue(
                any(
                    run.envelope_id == expected_envelope_id and run.result_status == "success"
                    for run in worker_runs
                ),
                msg=f"missing successful worker run for expected envelope_id={expected_envelope_id!r}",
            )

            handler_store = SQLiteStateStore(str(handler_db_path))
            self.assertEqual(
                handler_store.list_dispatched_event_indices(run_id=expected_task_id),
                [0],
            )
            self.assertTrue(
                handler_store.has_dispatched_event_id(
                    task_id=expected_task_id,
                    event_id=expected_event_id,
                )
            )


if __name__ == "__main__":
    unittest.main()
