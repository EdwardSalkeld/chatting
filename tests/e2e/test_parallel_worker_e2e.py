"""Run the real worker against a private BBMB server and timed executors."""

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.broker import BBMBQueueAdapter, TASK_QUEUE_NAME, TaskQueueMessage
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port(port: int) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"BBMB did not open port {port}")


def _task(number: int, item: str, duration: float) -> TaskQueueMessage:
    envelope = TaskEnvelope(
        id=f"parallel-smoke:{number}",
        source="im",
        received_at=datetime.now(timezone.utc),
        actor="parallel-smoke",
        content=f"sleep:{duration}",
        attachments=[],
        context_refs=[],
        reply_channel=ReplyChannel(type="log", target="parallel-smoke"),
        dedupe_key=f"parallel-smoke:{number}",
    )
    original = TaskQueueMessage.from_envelope(
        envelope, trace_id=f"parallel-smoke:{number}"
    )
    return TaskQueueMessage(
        envelope=original.envelope,
        trace_id=original.trace_id,
        task_id=original.task_id,
        emitted_at=original.emitted_at,
        work_item_id=item,
    )


class ParallelWorkerE2ETests(unittest.TestCase):
    def test_two_items_overlap_and_same_item_serializes(self) -> None:
        server_bin = os.environ.get("CHATTING_BBMB_SERVER_BIN")
        if not server_bin:
            self.skipTest("CHATTING_BBMB_SERVER_BIN is not set")
        repo_root = Path(__file__).resolve().parents[2]
        fake = repo_root / "tests/e2e/parallel_fake_codex.py"
        port, metrics_port = _free_port(), _free_port()
        while metrics_port == port:
            metrics_port = _free_port()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "worker.db"
            events = root / "events.jsonl"
            config = root / "worker.json"
            config.write_text(
                json.dumps(
                    {
                        "db_path": str(db),
                        "bbmb_address": f"127.0.0.1:{port}",
                        "handler_egress_url": "http://127.0.0.1:1/egress",
                        "codex_command": f"{sys.executable} {fake} {events}",
                        "workspace_root": str(root / "workspaces"),
                        "executor_pool_size": 2,
                        "activity_port": 0,
                        "poll_timeout_seconds": 1,
                        "sleep_seconds": 0.05,
                    }
                ),
                encoding="utf-8",
            )
            server = subprocess.Popen(
                [server_bin, f"--port={port}", f"--metrics-port={metrics_port}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            worker = None
            try:
                _wait_for_port(port)
                broker = BBMBQueueAdapter(address=f"127.0.0.1:{port}")
                broker.ensure_queue(TASK_QUEUE_NAME)
                for task in (
                    _task(1, "item_parallel_a", 0.8),
                    _task(2, "item_parallel_a", 0.1),
                    _task(3, "item_parallel_b", 0.8),
                ):
                    broker.publish_json(TASK_QUEUE_NAME, task.to_dict())
                worker = subprocess.Popen(
                    [sys.executable, "-m", "app.main_worker", "--config", str(config)],
                    cwd=repo_root,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    start_new_session=True,
                )
                deadline = time.monotonic() + 15
                store = SQLiteStateStore(str(db))
                while time.monotonic() < deadline:
                    if all(
                        (
                            inbox := store.get_inbox_task(
                                task_id=f"task:parallel-smoke:{n}"
                            )
                        )
                        is not None
                        and inbox.state == "completed"
                        for n in (1, 2, 3)
                    ):
                        break
                    time.sleep(0.05)
                else:
                    self.fail("timed out waiting for three worker runs")
                log = [json.loads(line) for line in events.read_text().splitlines()]
                intervals = {
                    task_id: {
                        event["phase"]: event["at"]
                        for event in log
                        if event["task_id"] == task_id
                    }
                    for task_id in (
                        "task:parallel-smoke:1",
                        "task:parallel-smoke:2",
                        "task:parallel-smoke:3",
                    )
                }
                first, second, other = (
                    intervals[f"task:parallel-smoke:{n}"] for n in (1, 2, 3)
                )
                self.assertLess(other["start"], first["end"])
                self.assertLess(first["start"], other["end"])
                self.assertGreaterEqual(second["start"], first["end"])
                self.assertEqual(
                    store.inbox_queue_summary(), {"queued": 0, "running": 0}
                )

                # Interrupt a live executor and restart the coordinator. The
                # orphaned subprocess is killed with its process group, as
                # systemd does for the Sparrow service.
                interrupted = _task(4, "item_parallel_restart", 2.0)
                broker.publish_json(TASK_QUEUE_NAME, interrupted.to_dict())
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    if events.exists() and any(
                        entry["task_id"] == interrupted.task_id
                        and entry["phase"] == "start"
                        for entry in (
                            json.loads(line) for line in events.read_text().splitlines()
                        )
                    ):
                        break
                    time.sleep(0.05)
                else:
                    self.fail("interrupted executor did not start")
                os.killpg(worker.pid, signal.SIGKILL)
                worker.wait(timeout=5)
                worker = subprocess.Popen(
                    [sys.executable, "-m", "app.main_worker", "--config", str(config)],
                    cwd=repo_root,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    start_new_session=True,
                )
                deadline = time.monotonic() + 12
                while time.monotonic() < deadline:
                    inbox = store.get_inbox_task(task_id=interrupted.task_id)
                    if inbox is not None and inbox.state == "completed":
                        break
                    time.sleep(0.05)
                else:
                    self.fail("interrupted task was not recovered")
                completed = [
                    run
                    for run in store.list_runs()
                    if run.envelope_id == interrupted.envelope.id
                ]
                self.assertEqual(len(completed), 1)
                self.assertEqual(
                    store.get_inbox_task(task_id=interrupted.task_id).state,
                    "completed",
                )
            finally:
                for process in (worker, server):
                    if process is not None and process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                if server.stderr is not None:
                    server.stderr.close()


if __name__ == "__main__":
    unittest.main()
