import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
from dataclasses import replace
from contextlib import closing

from app.broker import TaskQueueMessage
from app.models import ReplyChannel
from app.state import SQLiteStateStore
from app.worker.executor import CodexExecutor
from app.worker.executor.finish import FinishSession
from app.worker.activity import WorkerActivityMonitor
from app.worker.runtime import process_task_message
from tests.test_executor import _envelope


class ExplicitFinishTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = SQLiteStateStore(str(Path(self.temporary.name) / "worker.db"))
        self.task = TaskQueueMessage.from_envelope(_envelope(), trace_id="finish")
        self.store.stage_inbox_task(self.task)
        assignment = self.store.get_work_assignment(task_id=self.task.task_id)
        assert assignment is not None
        self.item = assignment.work_item_id

    def executor(self, script, timeout=8):
        return CodexExecutor(
            command=(sys.executable, "-u", "-c", script),
            cwd=self.temporary.name,
            work_item_id=self.item,
            history_store=self.store,
            timeout_seconds=timeout,
        )

    def enable(self):
        self.store.set_work_explicit_finish(work_item_id=self.item, enabled=True)

    def test_default_off_preserves_exit_completion_and_payload(self):
        self.assertFalse(self.store.get_work_explicit_finish(work_item_id=self.item))
        result = self.executor("import sys; print(sys.stdin.read())").execute(
            _envelope()
        )
        self.assertFalse(result.errors)
        assert result.stdout is not None
        payload = json.loads(result.stdout)
        self.assertNotIn("termination_contract", payload)
        self.assertTrue(
            payload["reply_contract"]["executor_exit_status_drives_completion"]
        )

    def test_migration_defaults_existing_item_off(self):
        path = str(Path(self.temporary.name) / "old-worker.db")
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute(
                "CREATE TABLE work_items (work_item_id TEXT PRIMARY KEY, origin_conversation_id TEXT NOT NULL, preferred_reply_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'open', model_tier TEXT NOT NULL DEFAULT 'auto', created_at TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO work_items VALUES ('existing', 'chat', '{}', 'open', 'high', '2026-10-14T00:00:00Z')"
            )
        migrated = SQLiteStateStore(path)
        self.assertFalse(migrated.get_work_explicit_finish(work_item_id="existing"))
        self.assertEqual(migrated.get_work_model_tier(work_item_id="existing"), "high")

    def test_finish_records_then_kills_executor_and_child(self):
        self.enable()
        script = """
import json, os, subprocess, sys
from pathlib import Path
payload = json.load(sys.stdin)
assert not payload['reply_contract']['executor_exit_status_drives_completion']
assert payload['termination_contract']['execution_id'] == os.environ['CHATTING_FINISH_EXECUTION_ID']
child = subprocess.Popen([sys.executable, '-c', "import time; time.sleep(.8); open('child-survived', 'w').close()"])
subprocess.run([sys.executable, '-P', '-m', 'app.main_finish'])
Path('executor-survived').touch()
"""
        real_kill = os.killpg

        def stop_group(pid, sig):
            records = list(
                (Path(self.temporary.name) / ".chatting-finish").glob("*.json")
            )
            self.assertEqual(
                len(records), 1, "completion must be recorded before killing"
            )
            self.assertEqual(
                json.loads(records[0].read_text())["task_id"], self.task.task_id
            )
            real_kill(pid, sig)

        with patch("app.worker.executor.codex.os.killpg", side_effect=stop_group):
            result = self.executor(script).execute(_envelope())
        self.assertFalse(result.errors, result)
        records = list((Path(self.temporary.name) / ".chatting-finish").glob("*.json"))
        self.assertEqual(len(records), 1)
        self.assertEqual(
            json.loads(records[0].read_text())["task_id"], self.task.task_id
        )
        import time

        time.sleep(1)
        self.assertFalse((Path(self.temporary.name) / "child-survived").exists())
        self.assertFalse((Path(self.temporary.name) / "executor-survived").exists())

    def test_unrequested_zero_and_nonzero_exits_restart_with_context(self):
        self.enable()
        script = """
import json, sys, subprocess
from pathlib import Path
p = json.load(sys.stdin)
counter = Path('passes')
n = int(counter.read_text()) if counter.exists() else 0
counter.write_text(str(n+1))
if n < 2:
    print('already did side effect', flush=True)
    sys.exit(n)
assert 'already did side effect' in str(p['task']['prompt_context'])
subprocess.run([sys.executable, '-P', '-m', 'app.main_finish'])
"""
        with patch("app.worker.executor.codex.time.sleep"):
            result = self.executor(script).execute(_envelope())
        self.assertFalse(result.errors, result)
        self.assertEqual((Path(self.temporary.name) / "passes").read_text(), "3")

    def test_restart_limit_fails_even_on_zero_exit(self):
        self.enable()
        with patch("app.worker.executor.codex.time.sleep"):
            result = self.executor(
                "import sys; sys.stdin.read(); print('exit')"
            ).execute(_envelope())
        self.assertEqual(result.errors, ["executor_finish_missing:restart_limit"])
        assert result.stdout is not None
        self.assertEqual(result.stdout.count("Pass "), 3)

    def test_timeout_is_failure_and_does_not_restart(self):
        self.enable()
        result = self.executor(
            "import sys,time; sys.stdin.read(); time.sleep(10)", timeout=1
        ).execute(_envelope())
        self.assertEqual(result.errors, ["executor_timeout"])

    def test_stale_execution_request_rejected(self):
        session = FinishSession(self.task.task_id, self.temporary.name)
        self.addCleanup(session.close)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(session.path)
            connection.sendall(
                json.dumps(
                    {"task_id": session.task_id, "execution_id": "stale"}
                ).encode()
                + b"\n"
            )
            self.assertFalse(session.poll())
            self.assertIn(b"invalid execution", connection.recv(4096))
        self.assertFalse(session.requested)
        self.assertFalse(session.record_dir.exists())

    def test_setting_persists_and_is_scoped_to_work_item(self):
        self.enable()
        reopened = SQLiteStateStore(self.store._db_path)
        self.assertTrue(reopened.get_work_explicit_finish(work_item_id=self.item))
        with self.assertRaises(KeyError):
            reopened.set_work_explicit_finish(work_item_id="other", enabled=True)
        self.store.set_work_explicit_finish(work_item_id=self.item, enabled=False)
        self.assertFalse(reopened.get_work_explicit_finish(work_item_id=self.item))

    def test_chat_command_changes_only_its_item_without_launching_executor(self):
        for number, mode in enumerate(("on", "show", "off")):
            envelope = replace(
                _envelope(),
                id=f"command-{number}",
                content="/finish" + (f" {mode}" if mode != "show" else ""),
            )
            message = replace(
                TaskQueueMessage.from_envelope(envelope, trace_id="command"),
                work_item_id="item_chat",
            )
            self.store.stage_inbox_task(message)
            with patch.object(
                CodexExecutor, "execute", side_effect=AssertionError("must not launch")
            ):
                result = process_task_message(
                    store=self.store,
                    task_message=message,
                    executor_impl=CodexExecutor(),
                    max_attempts=1,
                    activity_monitor=WorkerActivityMonitor(store=self.store),
                )
            self.assertEqual(result.run_record.result_status, "success")
            self.assertEqual(
                self.store.get_work_explicit_finish(work_item_id="item_chat"),
                mode != "off",
            )
            self.assertFalse(
                self.store.get_work_explicit_finish(work_item_id=self.item)
            )

    def test_low_escalation_can_finish_explicitly(self):
        self.enable()
        script = """
import json, subprocess, sys
from pathlib import Path
p = json.load(sys.stdin)
contract = p['escalation_contract']
Path(contract['request_path']).write_text(json.dumps({'task_id': p['task']['task_id'], 'reason': 'complex'}))
subprocess.run([sys.executable, '-P', '-m', 'app.main_finish'])
"""
        result = self.executor(script).for_model("low").execute(_envelope())
        self.assertFalse(result.errors, result)
        self.assertEqual(result.escalation_reason, "complex")

    def test_worker_completion_is_independent_of_visible_reply(self):
        self.enable()
        self.store.set_work_model_tier(work_item_id=self.item, tier="high")
        envelope = replace(
            _envelope(),
            id="telegram:finish",
            source="im",
            reply_channel=ReplyChannel(type="telegram", target="chat"),
        )
        message = replace(
            TaskQueueMessage.from_envelope(envelope, trace_id="finish"),
            work_item_id=self.item,
        )
        self.store.stage_inbox_task(message)
        script = "import sys,subprocess; sys.stdin.read(); subprocess.run([sys.executable, '-P', '-m', 'app.main_finish'])"
        with patch("app.worker.executor.codex.os.killpg", wraps=os.killpg) as kill:
            result = process_task_message(
                store=self.store,
                task_message=message,
                executor_impl=self.executor(script),
                max_attempts=1,
                activity_monitor=WorkerActivityMonitor(store=self.store),
            )
        self.assertTrue(kill.called)
        self.assertEqual(result.run_record.result_status, "success")
        self.assertNotIn("missing_visible_reply", result.reason_codes)
