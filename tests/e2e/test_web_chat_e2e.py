"""Real handler/broker/worker roundtrip with no Telegram or model dependency."""

import base64
import http.client
import json
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
from tests.e2e.exec_backend import use_exec_backend
from tests.e2e.handler_command import message_handler_command
from tests.e2e.servers import require_bbmb_server, reserve_port, wait_for_port
from tests.e2e.worker_cleanup import stop_test_supervisors


def unix_json(path, method, route, payload=None):
    connection = http.client.HTTPConnection("localhost", timeout=5)
    connection.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.sock.connect(str(path))
    try:
        connection.request(
            method,
            route,
            body=json.dumps(payload) if payload is not None else None,
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        return response.status, json.load(response)
    finally:
        connection.close()


class WebChatE2E(unittest.TestCase):
    def test_web_conversation_roundtrip_uses_existing_lane_without_telegram(self):
        broker = require_bbmb_server(self)
        repo = Path(__file__).resolve().parents[2]
        ports = [reserve_port() for _ in range(4)]
        bbmb, bbmb_metrics, metrics, egress = ports
        processes = []
        with tempfile.TemporaryDirectory(prefix="web-e2e-", dir="/tmp") as directory:
            root = Path(directory)
            handler_db, worker_db = root / "handler.db", root / "worker.db"
            private_socket = root / "web.sock"
            handler_config, worker_config = root / "handler.json", root / "worker.json"
            use_exec_backend(worker_db)
            handler_config.write_text(
                json.dumps(
                    {
                        "db_path": str(handler_db),
                        "bbmb_address": f"127.0.0.1:{bbmb}",
                        "poll_interval_seconds": 0.05,
                        "metrics_port": metrics,
                        "egress_http_port": egress,
                        "webhook_http_port": 0,
                        "platform_api_socket": str(private_socket),
                        "telegram_enabled": False,
                        "allowed_egress_channels": [],
                    }
                )
            )
            worker_config.write_text(
                json.dumps(
                    {
                        "db_path": str(worker_db),
                        "workspace_root": str(root / "workspaces"),
                        "bbmb_address": f"127.0.0.1:{bbmb}",
                        "max_attempts": 2,
                        "poll_timeout_seconds": 1,
                        "sleep_seconds": 0.05,
                        "activity_port": 0,
                        "codex_command": f"{sys.executable} {repo / 'tests/e2e/fake_codex.py'}",
                        "handler_egress_url": f"http://127.0.0.1:{egress}/egress",
                    }
                )
            )
            log_path = root / "processes.log"
            with log_path.open("w") as log:

                def start(command):
                    process = subprocess.Popen(
                        command, cwd=repo, stdout=log, stderr=log
                    )
                    processes.append(process)
                    return process

                try:
                    start(
                        [
                            str(broker),
                            f"--port={bbmb}",
                            f"--metrics-port={bbmb_metrics}",
                        ]
                    )
                    wait_for_port("127.0.0.1", bbmb)
                    start(message_handler_command(handler_config))
                    wait_for_port("127.0.0.1", egress, timeout_seconds=30)
                    now = datetime.now(timezone.utc).isoformat()
                    origin = {
                        "type": "telegram",
                        "target": "-123",
                        "metadata": {
                            "message_thread_id": 7,
                            "chat_title": "Existing conversation",
                        },
                    }
                    route = json.dumps(
                        {"chat": "-123", "topic": 7},
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    with sqlite3.connect(handler_db) as db:
                        db.execute(
                            "INSERT INTO work_items VALUES ('item_existing',?,?)",
                            (json.dumps(origin), now),
                        )
                        db.execute(
                            "INSERT INTO work_item_routes VALUES ('telegram',?,'item_existing')",
                            (route,),
                        )
                    cid = base64.urlsafe_b64encode(route.encode()).decode().rstrip("=")
                    status, listing = unix_json(
                        private_socket, "GET", "/api/conversations"
                    )
                    self.assertEqual(status, 200)
                    self.assertEqual(
                        listing["conversations"][0]["work_item_id"], "item_existing"
                    )
                    request = {
                        "message": "Continue our conversation through the web.",
                        "client_message_id": "abcdefghijklmnop1234567890",
                    }
                    status, accepted = unix_json(
                        private_socket,
                        "POST",
                        f"/api/conversations/{cid}/messages",
                        request,
                    )
                    self.assertEqual(status, 202)
                    _, retried = unix_json(
                        private_socket,
                        "POST",
                        f"/api/conversations/{cid}/messages",
                        request,
                    )
                    self.assertEqual(accepted["task_id"], retried["task_id"])
                    start(
                        [
                            sys.executable,
                            "-m",
                            "app.worker.main",
                            "--config",
                            str(worker_config),
                        ]
                    )
                    store = SQLiteStateStore(str(worker_db))
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        snapshot = store.web_conversation_snapshot(conversation_id=cid)
                        inbox = store.get_inbox_task(task_id=accepted["task_id"])
                        if (
                            inbox
                            and inbox.state == "completed"
                            and len(snapshot["messages"]) == 2
                        ):
                            break
                        time.sleep(0.1)
                    logs = log_path.read_text()
                    self.assertEqual(len(snapshot["messages"]), 2, logs)
                    self.assertEqual(
                        snapshot["messages"][0]["status"], "answered", logs
                    )
                    self.assertEqual(
                        snapshot["messages"][1]["content"], "E2E fake codex reply", logs
                    )
                    self.assertEqual(inbox.work_item_id, "item_existing", logs)
                    self.assertEqual(
                        inbox.task_message.envelope.conversation_channels[0].type,
                        "telegram",
                    )
                    self.assertEqual(inbox.state, "completed", logs)
                    with sqlite3.connect(handler_db) as db:
                        count = db.execute(
                            "SELECT COUNT(*) FROM web_ingress"
                        ).fetchone()[0]
                    self.assertEqual(count, 1)
                finally:
                    for process in reversed(processes):
                        if process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait(timeout=5)
                        self.assertIn(
                            process.returncode,
                            (0, -signal.SIGTERM),
                            log_path.read_text(),
                        )
                    stop_test_supervisors(worker_db)
