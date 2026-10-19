"""Exercise the real socket and process boundary without a broker."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from app.state import SQLiteStateStore
from app.worker.supervisor import Supervisor, SupervisorError, SupervisorPool, request
from tests.e2e.test_parallel_worker_e2e import _task
from tests.test_worker_inbox import _telegram_task


class SupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = SQLiteStateStore(str(self.root / "worker.db"))
        self.events = self.root / "events.jsonl"
        fake = Path(__file__).parent / "e2e/parallel_fake_codex.py"
        args = argparse.Namespace(
            codex_working_dir=None,
            workspace_root=str(self.root / "workspaces"),
            codex_command=f"{sys.executable} {fake.resolve()} {self.events}",
            claude_command=None,
            config=None,
        )
        self.settings = {
            "db_path": str(self.root / "worker.db"),
            "args": vars(args),
            "config": {},
            "max_attempts": 1,
            "handler_egress_url": "http://127.0.0.1:1/egress",
        }
        self.pools = []
        self.addCleanup(self.stop_processes)
        self.pool = self.connect()
        self.pids = [s["pid"] for s in self.pool.statuses()]

    def connect(self, size: int = 2) -> SupervisorPool:
        pool = SupervisorPool(
            directory=self.root / "slots", size=size, settings=self.settings
        )
        self.pools.append(pool)
        return pool

    def stop_processes(self) -> None:
        for pool in self.pools:
            for child in pool.children:
                if child.poll() is None:
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        # macOS can report EPERM when a clean shutdown races
                        # killpg. wait() below still fails if it remains alive.
                        pass
                child.wait(timeout=5)

    def wait_for(self, condition) -> None:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.025)
        self.fail("timed out waiting for supervisor")

    def log(self) -> list[dict]:
        return (
            [json.loads(line) for line in self.events.read_text().splitlines()]
            if self.events.exists()
            else []
        )

    def test_app_server_sandbox_config_is_validated_and_forwarded(self):
        self.settings["config"]["app_server_sandbox_mode"] = "danger-full-access"
        supervisor = Supervisor(slot=99, settings=self.settings)
        self.assertEqual(
            supervisor.executor.persistent.sandbox_mode, "danger-full-access"
        )
        self.settings["config"]["app_server_sandbox_mode"] = "invalid"
        with self.assertRaisesRegex(ValueError, "app_server_sandbox_mode"):
            Supervisor(slot=99, settings=self.settings)

    def test_reconnect_preserves_running_executor_and_finished_notice(self) -> None:
        task = _task(1, "item_restart", 0.8)
        self.store.stage_inbox_task(task)
        self.pool.tick()
        self.wait_for(lambda: len(self.log()) == 1)
        # A replacement manager discovers the same processes and protects their
        # leases before doing its normal inbox recovery.
        replacement = self.connect()
        statuses = replacement.statuses()
        self.assertEqual([s["pid"] for s in statuses], self.pids)
        self.assertNotIn(os.getpid(), self.pids)
        self.store.recover_inbox_tasks(
            supervised_task_ids=tuple(s["task_id"] for s in statuses if s["task_id"])
        )
        self.assertIsNone(self.store.claim_next_inbox_task())
        # No manager tick is needed to finish work or preserve the notice.
        self.wait_for(lambda: replacement.statuses()[0]["state"] == "finished")
        again = self.connect()
        self.assertEqual(again.statuses()[0]["task_id"], task.task_id)
        again.tick()
        self.assertEqual(again.statuses()[0]["state"], "idle")
        self.assertEqual([entry["phase"] for entry in self.log()], ["start", "end"])
        self.assertEqual(
            self.store.get_inbox_task(task_id=task.task_id).state, "completed"
        )

    def test_lost_start_response_does_not_repeat_handoff(self) -> None:
        task = _task(1, "item_lost_response", 0.2)
        self.store.stage_inbox_task(task)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(self.pool.path(1)))
            client.sendall(b'{"version":1,"command":"start"}\n')
        replacement = self.connect()
        self.wait_for(lambda: replacement.status(1)["state"] == "finished")
        replacement.tick()
        self.assertEqual(len(self.log()), 2)

    def test_explicit_finish_survives_manager_reconnection(self) -> None:
        task = _task(1, "item_explicit", 0.8)
        self.store.stage_inbox_task(task)
        self.store.set_work_explicit_finish(work_item_id="item_explicit", enabled=True)
        self.store.set_work_model_tier(work_item_id="item_explicit", tier="high")
        self.pool.tick()
        self.wait_for(lambda: len(self.log()) == 1)
        replacement = self.connect()
        self.assertEqual([s["pid"] for s in replacement.statuses()], self.pids)
        self.wait_for(lambda: replacement.status(1)["state"] == "finished")
        records = list(
            (self.root / "workspaces/item_explicit/.chatting-finish").glob("*.json")
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(json.loads(records[0].read_text())["task_id"], task.task_id)
        self.assertEqual([entry["phase"] for entry in self.log()], ["start", "end"])
        self.assertEqual(
            self.store.list_recent_runs(limit=1)[0].result_status, "success"
        )
        replacement.tick()
        self.assertEqual(replacement.status(1)["state"], "idle")

    def test_slots_run_in_parallel_and_serialize_same_work_item(self) -> None:
        for task in (
            _task(1, "item_a", 0.3),
            _task(2, "item_a", 0.01),
            _task(3, "item_b", 0.3),
        ):
            self.store.stage_inbox_task(task)
        self.pool.tick()
        self.wait_for(
            lambda: len([e for e in self.log() if e["phase"] == "start"]) == 2
        )
        self.assertEqual(
            {e["task_id"] for e in self.log() if e["phase"] == "start"},
            {"task:parallel-smoke:1", "task:parallel-smoke:3"},
        )
        self.wait_for(
            lambda: all(s["state"] == "finished" for s in self.pool.statuses())
        )
        self.pool.tick()
        self.wait_for(lambda: len(self.log()) == 6)
        self.assertGreaterEqual(self.log()[4]["at"], self.log()[2]["at"])

    def test_dead_supervisor_is_a_failure_and_is_not_replaced(self) -> None:
        os.kill(self.pids[0], signal.SIGKILL)
        self.pool.children[0].wait(timeout=5)
        with self.assertRaises(SupervisorError):
            self.connect()
        self.assertEqual(self.pool.status(2)["pid"], self.pids[1])

    def test_busy_claim_keeps_supervisor_alive_and_retries(self) -> None:
        task = _task(1, "item_busy", 0.1)
        self.store.stage_inbox_task(task)
        with sqlite3.connect(self.settings["db_path"]) as writer:
            writer.execute("BEGIN IMMEDIATE")
            status = request(self.pool.path(1), "start")
            self.assertEqual(status["state"], "idle")
            self.assertEqual(status["pid"], self.pids[0])
            self.assertEqual(
                self.store.get_inbox_task(task_id=task.task_id).state, "pending"
            )
        request(self.pool.path(1), "start")
        self.wait_for(lambda: self.pool.status(1)["state"] == "finished")
        self.assertEqual(self.pool.status(1)["pid"], self.pids[0])
        self.assertEqual([event["phase"] for event in self.log()], ["start", "end"])

    def test_shutdown_rejects_running_slot_and_ack_checks_task(self) -> None:
        self.store.stage_inbox_task(_task(1, "item_a", 0.3))
        self.pool.tick()
        with self.assertRaises(SupervisorError):
            request(self.pool.path(1), "shutdown")
        with self.assertRaises(SupervisorError):
            request(self.pool.path(1), "ack", task_id="wrong-task")
        self.wait_for(lambda: self.pool.status(1)["state"] == "finished")
        with self.assertRaises(SupervisorError):
            request(self.pool.path(1), "ack", task_id="wrong-task")

    def test_recovery_preserves_attached_followups_for_live_supervisor(self) -> None:
        parent, followup = _telegram_task(1), _telegram_task(2)
        self.store.stage_inbox_task(parent)
        self.store.claim_next_inbox_task()
        self.store.stage_inbox_task(followup)
        self.store.claim_conversation_followups(parent_task_id=parent.task_id)
        self.store.recover_inbox_tasks(supervised_task_ids=(parent.task_id,))
        self.assertEqual(
            self.store.get_inbox_task(task_id=parent.task_id).state, "active"
        )
        self.assertEqual(
            self.store.get_inbox_task(task_id=followup.task_id).state, "attached"
        )
        self.assertIsNone(self.store.claim_next_inbox_task())

    def test_smoke_run_can_drain_and_stop_all_slots(self) -> None:
        self.store.stage_inbox_task(_task(1, "item_a", 0.1))
        self.pool.tick()
        self.pool.drain()
        self.assertFalse(list((self.root / "slots").glob("*.lock")))

    def test_settings_change_waits_for_busy_supervisor_before_replacing_it(
        self,
    ) -> None:
        self.store.stage_inbox_task(_task(1, "item_upgrade", 0.3))
        self.pool.tick()
        self.settings = {**self.settings, "max_attempts": 2}
        replacement = self.connect()
        replacement.tick()
        self.assertEqual(replacement.status(1)["pid"], self.pids[0])
        self.wait_for(lambda: replacement.status(1)["state"] == "finished")
        replacement.tick()
        self.assertNotEqual(replacement.status(1)["pid"], self.pids[0])
        self.assertEqual(len(self.log()), 2)

    def test_pool_shrink_drains_busy_surplus_slot(self) -> None:
        for n in (1, 2):
            self.store.stage_inbox_task(_task(n, f"item_{n}", 0.3))
        self.pool.tick()
        replacement = self.connect(size=1)
        replacement.tick()
        self.assertEqual(replacement.status(2)["pid"], self.pids[1])
        self.wait_for(lambda: replacement.status(2)["state"] == "finished")
        replacement.tick()
        self.assertEqual(replacement.slots, [1])
        self.wait_for(lambda: not replacement.path(2).exists())

    def test_slot_lock_prevents_duplicate_supervisor(self) -> None:
        duplicate = subprocess.run(
            [
                sys.executable,
                "-m",
                "app.worker.supervisor",
                "--slot",
                "1",
                "--directory",
                str(self.root / "slots"),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
        )
        self.assertNotEqual(duplicate.returncode, 0)
        self.assertEqual(self.pool.status(1)["pid"], self.pids[0])

    def test_shutdown_command_waits_for_work_and_requires_manager_to_be_stopped(
        self,
    ) -> None:
        command = [
            sys.executable,
            "-m",
            "app.worker.supervisor",
            "--directory",
            str(self.root / "slots"),
            "--shutdown",
        ]
        with Path(f"{self.settings['db_path']}.worker.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            blocked = subprocess.run(command, capture_output=True, timeout=5)
            self.assertNotEqual(blocked.returncode, 0)
            self.assertIn(b"stop the manager", blocked.stderr)
        self.store.stage_inbox_task(_task(1, "item_shutdown", 0.1))
        self.pool.tick()
        subprocess.run(command, check=True, capture_output=True, timeout=5)
        self.wait_for(lambda: not list((self.root / "slots").glob("*.lock")))
        self.assertEqual([event["phase"] for event in self.log()], ["start", "end"])


if __name__ == "__main__":
    unittest.main()
