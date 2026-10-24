from dataclasses import replace
import json
from pathlib import Path
import os
import signal
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from app.model_command import parse_model_command
from app.models import ExecutionResult
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor
from app.worker.executor.app_server import AppServerExecutor
from app.worker.executor.app_server_session import Session
from app.worker.executor.codex import CodexExecutor
from app.worker.executor.conversation import ConversationExecutor
from app.worker.runtime import process_task_message
from tests.test_worker_inbox import _telegram_task


class AppServerExecutorTests(unittest.TestCase):
    def setUp(self):
        # Keep Unix socket paths short, even on CI with a long checkout path.
        self.temporary = tempfile.TemporaryDirectory(prefix="chat-sessions-")
        self.root = Path(self.temporary.name)
        self.log = self.root / "peer.jsonl"
        self.store = SQLiteStateStore(str(self.root / "worker.db"))
        self.executor = (
            AppServerExecutor(
                command=(
                    sys.executable,
                    "-u",
                    str(Path(__file__).with_name("fake_app_server.py")),
                    str(self.log),
                ),
                cwd=str(self.root),
                workspace_root=str(self.root / "work"),
                session_root=str(self.root / "sessions"),
                idle_seconds=0.5,
                timeout_seconds=3,
                history_store=self.store,
                model_tier="high",
            )
            .for_conversation("conversation-a")
            .for_workspace(work_item_id="item_a")
        )

    def tearDown(self):
        deadline = time.monotonic() + 4
        while (
            list(self.root.glob("sessions/*/rpc.sock")) and time.monotonic() < deadline
        ):
            time.sleep(0.025)
        self.temporary.cleanup()

    def calls(self, method):
        return [
            entry
            for entry in map(json.loads, self.log.read_text().splitlines())
            if entry.get("method") == method
        ]

    def test_reuses_process_and_thread_changes_cwd_and_model_and_caches_retry(self):
        first = _telegram_task(1)
        self.store.stage_inbox_task(first)
        output = []
        executor = self.executor.with_output_callback(
            lambda stream, content: output.append((stream, content))
        )
        result = executor.execute(first.envelope)
        self.assertEqual(result.errors, [])
        self.assertTrue(output)
        self.assertIn("item/completed", result.stdout)
        # A transport retry with the same request must not start a new turn.
        self.assertEqual(executor.execute(first.envelope), result)
        second = _telegram_task(2)
        self.store.stage_inbox_task(second)
        executor = executor.for_model("low")
        self.assertEqual(executor.execute(second.envelope).errors, [])
        turns = self.calls("turn/start")
        self.assertEqual(len(turns), 2)
        self.assertEqual(len(self.calls("initialize")), 1)
        self.assertEqual(len(self.calls("thread/start")), 1)
        self.assertEqual(turns[0]["pid"], turns[1]["pid"])
        self.assertEqual(turns[0]["params"]["threadId"], turns[1]["params"]["threadId"])
        self.assertTrue(turns[1]["params"]["cwd"].endswith("item_a"))
        self.assertEqual(turns[1]["params"]["model"], "gpt-6-luna")
        payload = json.loads(turns[1]["params"]["input"][0]["text"])
        self.assertNotIn("termination_contract", payload)
        self.assertNotIn("recent_history", payload)
        self.assertIn("session_contract", payload)
        self.assertEqual(payload["task"]["task_id"], second.task_id)

    def test_idle_expiry_resumes_saved_thread_and_other_conversation_is_isolated(self):
        self.assertEqual(self.executor.execute(_telegram_task(1).envelope).errors, [])
        thread = self.calls("turn/start")[0]["params"]["threadId"]
        deadline = time.monotonic() + 4
        while (
            list(self.root.glob("sessions/*/rpc.sock")) and time.monotonic() < deadline
        ):
            time.sleep(0.025)
        self.assertFalse(list(self.root.glob("sessions/*/rpc.sock")))
        self.assertEqual(self.executor.execute(_telegram_task(2).envelope).errors, [])
        self.assertEqual(self.calls("thread/resume")[0]["params"]["threadId"], thread)
        self.assertTrue(self.calls("thread/resume")[0]["params"]["excludeTurns"])
        other = self.executor.for_conversation("conversation-b")
        self.assertEqual(other.execute(_telegram_task(3).envelope).errors, [])
        self.assertEqual(self.calls("turn/start")[-1]["params"]["threadId"], thread)
        other = other.for_workspace(work_item_id="item_b")
        self.assertEqual(other.execute(_telegram_task(4).envelope).errors, [])
        self.assertNotEqual(self.calls("turn/start")[-1]["params"]["threadId"], thread)

    def test_failed_interrupted_protocol_and_timeout_turns_are_errors_without_replay(
        self,
    ):
        for number, content in enumerate(
            ("fail", "interrupt", "crash", "unsupported", "timeout"), 1
        ):
            with self.subTest(content=content):
                executor = replace(
                    self.executor.for_workspace(work_item_id=f"item_{content}"),
                    timeout_seconds=1,
                )
                task = _telegram_task(number, content=content)
                first = executor.execute(task.envelope)
                self.assertTrue(first.errors)
                self.assertEqual(executor.execute(task.envelope).errors, first.errors)
        self.assertEqual(len(self.calls("turn/start")), 5)

    def test_live_legacy_owner_retires_and_thread_resumes_under_item_identity(self):
        first = _telegram_task(1)
        self.store.stage_inbox_task(first)
        legacy = "conv_before_upgrade"
        old_executor = replace(self.executor, work_item_id=None, conversation_id=legacy)
        self.assertEqual(old_executor.execute(first.envelope).errors, [])
        thread = self.calls("turn/start")[-1]["params"]["threadId"]
        with sqlite3.connect(self.root / "worker.db") as db:
            db.execute("UPDATE work_items SET origin_conversation_id = ?", (legacy,))
        executor = self.executor.for_workspace(work_item_id=first.work_item_id)
        self.assertEqual(executor.execute(_telegram_task(2).envelope).errors, [])
        self.assertEqual(self.calls("thread/resume")[-1]["params"]["threadId"], thread)

    def test_webhook_on_telegram_item_can_complete_without_visible_reply(self):
        task = _telegram_task(1)
        task = replace(task, envelope=replace(task.envelope, source="webhook"))
        self.store.stage_inbox_task(task)
        self.store.set_work_model_tier(work_item_id=task.work_item_id, tier="high")
        result = process_task_message(
            store=self.store,
            task_message=task,
            executor_impl=self.executor,
            max_attempts=1,
            activity_monitor=WorkerActivityMonitor(store=self.store),
        )
        self.assertNotIn("missing_visible_reply", result.reason_codes)
        self.assertEqual(len(self.calls("turn/start")), 1)

    def test_multiple_live_legacy_sessions_preserve_retries_in_a_fresh_thread(self):
        first, second = _telegram_task(1), _telegram_task(2)
        self.store.stage_inbox_task(first)
        old = replace(self.executor, work_item_id=None)
        first_result = old.for_conversation("conv_old_a").execute(first.envelope)
        second_result = old.for_conversation("conv_old_b").execute(second.envelope)
        self.assertEqual(first_result.errors, [])
        self.assertEqual(second_result.errors, [])
        old_threads = {call["params"]["threadId"] for call in self.calls("turn/start")}
        with sqlite3.connect(self.root / "worker.db") as db:
            for legacy in ("conv_old_a", "conv_old_b"):
                db.execute(
                    "INSERT INTO work_item_session_aliases VALUES (?, ?)",
                    (first.work_item_id, legacy),
                )
        executor = self.executor.for_workspace(work_item_id=first.work_item_id)
        # Simulate retries after completion but before worker acknowledgement.
        # Both results must survive without another turn or thread adoption.
        self.assertEqual(executor.execute(first.envelope), first_result)
        self.assertEqual(executor.execute(second.envelope), second_result)
        self.assertEqual(len(self.calls("turn/start")), 2)
        self.assertEqual(executor.execute(_telegram_task(3).envelope).errors, [])
        self.assertEqual(len(self.calls("turn/start")), 3)
        self.assertNotIn(
            self.calls("turn/start")[-1]["params"]["threadId"], old_threads
        )
        self.assertEqual(executor.execute(first.envelope), first_result)
        self.assertEqual(len(self.calls("turn/start")), 3)

    def test_low_model_escalation_is_returned_to_worker(self):
        result = self.executor.for_model("low").execute(
            _telegram_task(1, content="escalate").envelope
        )
        self.assertEqual(result.errors, [])
        self.assertEqual(result.escalation_reason, "Needs a broader change")

    def test_full_access_and_no_approvals_apply_to_new_and_resumed_threads(self):
        executor = self.executor
        self.assertEqual(executor.execute(_telegram_task(1).envelope).errors, [])
        self.assertEqual(
            self.calls("thread/start")[0]["params"]["sandbox"], "danger-full-access"
        )
        self.assertEqual(
            self.calls("thread/start")[0]["params"]["approvalPolicy"], "never"
        )
        deadline = time.monotonic() + 4
        while (
            list(self.root.glob("sessions/*/rpc.sock")) and time.monotonic() < deadline
        ):
            time.sleep(0.025)
        self.assertFalse(list(self.root.glob("sessions/*/rpc.sock")))
        self.assertEqual(executor.execute(_telegram_task(2).envelope).errors, [])
        self.assertEqual(
            self.calls("thread/resume")[0]["params"]["sandbox"], "danger-full-access"
        )
        self.assertEqual(
            self.calls("thread/resume")[0]["params"]["approvalPolicy"], "never"
        )

    def test_owner_death_leaves_uncertain_turn_blocked_after_restart(self):
        killed = False

        def output(name, content):
            nonlocal killed
            if (
                not killed
                and name == "stdout"
                and '"method": "item/started"' in content
            ):
                killed = True
                owner = next(self.root.glob("sessions/*/owner.lock"))
                os.kill(int(owner.read_text()), signal.SIGKILL)

        task = _telegram_task(1, content="timeout")
        first = self.executor.with_output_callback(output).execute(task.envelope)
        self.assertTrue(first.errors)
        self.assertTrue(killed)
        second = self.executor.execute(_telegram_task(2).envelope)
        self.assertIn("uncertain_request", second.errors[0])
        self.assertEqual(len(self.calls("turn/start")), 1)

    def test_configuration_change_retires_idle_owner_before_retry(self):
        self.assertEqual(self.executor.execute(_telegram_task(1).envelope).errors, [])
        changed = replace(
            self.executor, env={**os.environ, "CHATTING_SESSION_TEST": "new"}
        )
        task = _telegram_task(2)
        retired = changed.execute(task.envelope)
        self.assertIn("config_changed", retired.errors[0])
        self.assertEqual(changed.execute(task.envelope).errors, [])
        self.assertEqual(len(self.calls("turn/start")), 2)
        self.assertEqual(len(self.calls("thread/resume")), 1)

    def test_crash_journal_blocks_other_requests_without_starting_app_server(self):
        directory = self.root / "journal"
        directory.mkdir()
        (directory / ("request-" + "a" * 64 + ".json")).write_text(
            json.dumps({"state": "submitted"})
        )
        (directory / "pending.json").write_text(json.dumps({"request_id": "a" * 64}))
        session = Session(directory, {"command": ["does-not-exist"]})
        result = session.execute({"request_id": "b" * 64}, lambda *_: None)
        self.assertIn("uncertain_request", result.errors[0])
        self.assertIsNone(session.server)

    def test_setting_is_work_item_scoped_persistent_and_default_is_legacy(self):
        first = _telegram_task(1)
        same_chat_other_item = replace(_telegram_task(2), work_item_id="item_another")
        other = _telegram_task(3, thread_id=99)
        for task in (first, same_chat_other_item, other):
            self.store.stage_inbox_task(task)
        conversation = self.store.get_inbox_task(task_id=first.task_id).conversation_id
        self.assertEqual(
            self.store.get_conversation_executor(conversation_id=conversation), "exec"
        )
        self.store.set_conversation_executor(
            conversation_id=conversation, backend="app-server"
        )
        reopened = SQLiteStateStore(str(self.root / "worker.db"))
        self.assertEqual(
            reopened.get_conversation_executor(conversation_id=conversation),
            "app-server",
        )
        self.assertNotEqual(
            self.store.get_inbox_task(
                task_id=same_chat_other_item.task_id
            ).conversation_id,
            conversation,
        )
        other_id = self.store.get_inbox_task(task_id=other.task_id).conversation_id
        self.assertEqual(
            reopened.get_conversation_executor(conversation_id=other_id), "exec"
        )
        claimed = self.store.claim_next_inbox_task()
        self.assertEqual(claimed.task_message.task_id, first.task_id)
        # Different work items have independent sessions and may run together.
        claimed = self.store.claim_next_inbox_task()
        self.assertEqual(claimed.task_message.task_id, same_chat_other_item.task_id)
        claimed = self.store.claim_next_inbox_task()
        self.assertEqual(claimed.task_message.task_id, other.task_id)
        self.assertIsNone(self.store.claim_next_inbox_task())

    def test_command_selects_conversation_and_does_not_run_a_model(self):
        task = _telegram_task(1, content="/executor app-server")
        self.assertEqual(parse_model_command(task.envelope), ("executor", "app-server"))
        self.store.stage_inbox_task(task)
        result = process_task_message(
            store=self.store,
            task_message=task,
            executor_impl=self.executor,
            max_attempts=1,
            activity_monitor=WorkerActivityMonitor(store=self.store),
        )
        self.assertEqual(result.reason_codes, ["conversation_executor_command"])
        conversation = self.store.get_inbox_task(task_id=task.task_id).conversation_id
        self.assertEqual(
            self.store.get_conversation_executor(conversation_id=conversation),
            "app-server",
        )
        self.assertFalse(self.log.exists())
        self.assertEqual(
            parse_model_command(
                _telegram_task(2, content="[thread_id=99] /executor@bot exec").envelope
            ),
            ("executor", "exec"),
        )

    def test_switch_command_is_a_followup_barrier_for_a_running_turn(self):
        parent = _telegram_task(1)
        self.store.stage_inbox_task(parent)
        self.store.claim_next_inbox_task()
        switch = _telegram_task(2, content="/executor app-server")
        later = _telegram_task(3)
        self.store.stage_inbox_task(switch)
        self.store.stage_inbox_task(later)
        self.assertEqual(
            self.store.claim_conversation_followups(parent_task_id=parent.task_id), []
        )
        self.assertEqual(
            self.store.get_inbox_task(task_id=switch.task_id).state, "pending"
        )
        self.assertEqual(
            self.store.get_inbox_task(task_id=later.task_id).state, "pending"
        )

    def test_selector_and_missing_reply_recovery_use_the_same_thread(self):
        task = _telegram_task(1)
        self.store.stage_inbox_task(task)
        conversation = self.store.get_inbox_task(task_id=task.task_id).conversation_id
        selector = ConversationExecutor(
            legacy=CodexExecutor(), persistent=self.executor, store=self.store
        )
        with patch.object(
            CodexExecutor, "execute", return_value=ExecutionResult(errors=[])
        ) as legacy:
            selector.execute(task.envelope)
            legacy.assert_called_once()
        self.store.set_conversation_executor(
            conversation_id=conversation, backend="app-server"
        )
        self.store.set_work_model_tier(work_item_id=task.work_item_id, tier="high")
        # The fake peer never sends a visible reply, so a second turn must
        # attempt recovery, then fail the delivery gate.
        result = process_task_message(
            store=self.store,
            task_message=task,
            executor_impl=selector,
            max_attempts=1,
            activity_monitor=WorkerActivityMonitor(store=self.store),
        )
        self.assertIn("missing_visible_reply", result.reason_codes)
        self.assertEqual(len(self.calls("turn/start")), 2)
        self.assertEqual(len(self.calls("thread/start")), 1)


if __name__ == "__main__":
    unittest.main()
