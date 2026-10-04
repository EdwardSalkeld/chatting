import json
import tempfile
import unittest
import sqlite3
import subprocess
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.models import AuditEvent, ReplyChannel, RunRecord, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor
from app.worker.executor import CodexExecutor
from app.worker.runtime import process_task_message


def task(number, source, target, content="A task", actor=None, metadata=None):
    envelope = TaskEnvelope(
        id=f"example:{number}",
        source=source,
        received_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
        actor=actor,
        content=content,
        attachments=[],
        context_refs=[],
        reply_channel=ReplyChannel(
            type="telegram" if source == "im" else "email",
            target=target,
            metadata=metadata or {},
        ),
        dedupe_key=f"example:{number}",
    )
    return TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:{number}")


class WorkItemRoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = SQLiteStateStore(str(Path(self.tmp.name) / "worker.db"))

    def stage(self, message):
        self.assertTrue(self.store.stage_inbox_task(message))
        return self.store.get_work_assignment(task_id=message.task_id)

    def test_legacy_messages_share_one_fixed_lane(self):
        first = self.stage(task(1, "im", "chat", metadata={"message_id": 101}))
        followup = self.stage(task(2, "im", "chat", metadata={"message_id": 102}))
        reply = self.stage(
            task(
                3,
                "im",
                "chat",
                metadata={"message_id": 103, "reply_to_message_id": 101},
            )
        )
        self.assertEqual(first.work_item_id, followup.work_item_id)
        self.assertEqual(first.work_item_id, reply.work_item_id)
        self.assertEqual(reply.reason, "legacy_general")
        new_objective = self.stage(
            task(4, "im", "chat", content="Separately, fix backups")
        )
        self.assertEqual(first.work_item_id, new_objective.work_item_id)
        reopened = SQLiteStateStore(self.store._db_path)
        self.assertEqual(
            reopened.get_work_assignment(
                task_id=task(4, "im", "chat").task_id
            ).work_item_id,
            first.work_item_id,
        )

    def test_legacy_fallback_does_not_route_by_telegram_topic_or_email(self):
        topic_a = self.stage(
            task(1, "im", "chat", metadata={"message_id": 101, "message_thread_id": 7})
        )
        topic_b = self.stage(
            task(2, "im", "chat", metadata={"message_id": 102, "message_thread_id": 8})
        )
        self.assertEqual(topic_a.work_item_id, topic_b.work_item_id)
        email_a = self.stage(
            task(3, "email", "alice@example.com", metadata={"message_id": "<a@host>"})
        )
        email_b = self.stage(
            task(4, "email", "alice@example.com", metadata={"message_id": "<b@host>"})
        )
        email_reply = self.stage(
            task(
                5,
                "email",
                "alice@example.com",
                metadata={"message_id": "<c@host>", "references": "<a@host>"},
            )
        )
        self.assertEqual(email_a.work_item_id, email_b.work_item_id)
        self.assertEqual(email_a.work_item_id, email_reply.work_item_id)
        self.assertEqual(topic_a.work_item_id, email_a.work_item_id)

    def test_unknown_notifications_share_general_lane(self):
        unknown = self.stage(
            task(
                1,
                "email",
                "notifications@github.com",
                content="https://github.com/EdwardSalkeld/chatting/pull/99",
                actor="notifications@github.com",
            )
        )
        other = self.stage(
            task(
                2,
                "email",
                "notifications@github.com",
                content="https://github.com/EdwardSalkeld/chatting/pull/98",
                actor="notifications@github.com",
            )
        )
        self.assertEqual(unknown.work_item_id, other.work_item_id)
        self.assertEqual(unknown.reason, "legacy_general")

    def test_worker_records_carry_lane_id(self):
        message = task(1, "im", "chat", metadata={"message_id": 101})
        assignment = self.stage(message)
        run = RunRecord(
            run_id="run:example",
            envelope_id=message.envelope.id,
            source="im",
            workflow="default",
            latency_ms=1,
            result_status="success",
            created_at=datetime.now(timezone.utc),
            work_item_id=assignment.work_item_id,
        )
        self.store.append_run(run)
        self.store.append_audit_event(
            AuditEvent(
                run_id=run.run_id,
                envelope_id=run.envelope_id,
                source="im",
                workflow="default",
                result_status="success",
                detail={},
                created_at=run.created_at,
            )
        )
        self.store.append_worker_activity(
            occurred_at=run.created_at,
            phase="completed",
            summary="done",
            detail={},
            task_id=message.task_id,
            run_id=run.run_id,
        )
        with sqlite3.connect(self.store._db_path) as connection:
            for table in (
                "worker_inbox",
                "worker_telegram_history",
                "run_records",
                "audit_events",
                "worker_activity_events",
            ):
                (saved_id,) = connection.execute(
                    f"SELECT work_item_id FROM {table} LIMIT 1"
                ).fetchone()
                self.assertEqual(saved_id, assignment.work_item_id, table)

    def test_handler_assignment_is_authoritative(self):
        first = task(1, "im", "chat")
        assigned = replace(first, work_item_id="item_from_handler")
        lane = self.stage(assigned)
        self.assertEqual(lane.work_item_id, "item_from_handler")
        self.assertEqual(lane.reason, "handler_assignment")

    def test_assigned_lane_uses_persistent_workspace_directory(self):
        workspace_root = Path(self.tmp.name) / "workspaces"
        executor = CodexExecutor(cwd=self.tmp.name, workspace_root=str(workspace_root))
        first = task(1, "email", "alice@example.com")
        first_lane = self.stage(first)
        second = replace(
            task(2, "email", "bob@example.com"),
            work_item_id="item_other",
        )
        second_lane = self.stage(second)
        completed = subprocess.CompletedProcess(
            args=["codex"], returncode=0, stdout="", stderr=""
        )
        with patch(
            "app.worker.executor.codex.subprocess.run", return_value=completed
        ) as run:
            for message in (first, second, first):
                process_task_message(
                    store=self.store,
                    task_message=message,
                    executor_impl=executor,
                    max_attempts=1,
                    activity_monitor=WorkerActivityMonitor(
                        store=self.store, history_limit=10
                    ),
                )
        first_dir = workspace_root / first_lane.work_item_id
        second_dir = workspace_root / second_lane.work_item_id
        self.assertTrue(first_dir.is_dir())
        self.assertTrue(second_dir.is_dir())
        self.assertNotEqual(first_dir, second_dir)
        self.assertEqual(
            [call.kwargs["cwd"] for call in run.call_args_list],
            [str(first_dir), str(second_dir), str(first_dir)],
        )
        payload = json.loads(run.call_args.kwargs["input"])
        self.assertEqual(payload["task"]["work_item_id"], first_lane.work_item_id)
        self.assertEqual(
            payload["reply_contract"]["executor_working_dir"], str(first_dir)
        )
        self.assertIn(str(first_dir), payload["task"]["workspace_guidance"])
        self.assertIn("repository clones", payload["task"]["workspace_guidance"])

    def test_model_command_persists_per_work_item_and_selects_codex_model(self):
        first = replace(
            task(30, "email", "alice@example.com", "/set low"), work_item_id="item_a"
        )
        self.stage(first)
        result = process_task_message(
            store=self.store,
            task_message=first,
            executor_impl=CodexExecutor(),
            max_attempts=1,
            activity_monitor=WorkerActivityMonitor(store=self.store),
        )
        self.assertIn("gpt-6-luna", result.egress_messages[0].message.body)
        self.assertEqual(
            SQLiteStateStore(self.store._db_path).get_work_model_tier(
                work_item_id="item_a"
            ),
            "low",
        )
        second = replace(task(31, "email", "alice@example.com"), work_item_id="item_a")
        self.stage(second)
        completed = subprocess.CompletedProcess(
            args=["codex"], returncode=0, stdout="", stderr=""
        )
        with patch(
            "app.worker.executor.codex.subprocess.run", return_value=completed
        ) as run:
            process_task_message(
                store=self.store,
                task_message=second,
                executor_impl=CodexExecutor(
                    workspace_root=str(Path(self.tmp.name) / "workspaces")
                ),
                max_attempts=1,
                activity_monitor=WorkerActivityMonitor(store=self.store),
            )
        self.assertEqual(run.call_args.args[0][-2:], ("-m", "gpt-6-luna"))
        other = replace(
            task(32, "email", "bob@example.com", "/model"), work_item_id="item_b"
        )
        self.stage(other)
        shown = process_task_message(
            store=self.store,
            task_message=other,
            executor_impl=CodexExecutor(),
            max_attempts=1,
            activity_monitor=WorkerActivityMonitor(store=self.store),
        )
        self.assertIn("gpt-6.1-sol", shown.egress_messages[0].message.body)

    def test_low_task_handoff_runs_sol_once_without_changing_setting(self):
        message = replace(task(40, "email", "alice@example.com"), work_item_id="item_a")
        self.stage(message)
        self.store.set_work_model_tier(work_item_id="item_a", tier="low")
        calls = []

        def run(command, **kwargs):
            calls.append((command, json.loads(kwargs["input"])))
            contract = calls[-1][1].get("escalation_contract")
            if contract:
                Path(contract["request_path"]).write_text(
                    json.dumps(
                        {
                            "task_id": message.task_id,
                            "reason": "Needs a broad migration",
                        }
                    )
                )
            return subprocess.CompletedProcess(
                args=command, returncode=0, stdout="", stderr=""
            )

        with patch("app.worker.executor.codex.subprocess.run", side_effect=run):
            result = process_task_message(
                store=self.store,
                task_message=message,
                executor_impl=CodexExecutor(
                    workspace_root=str(Path(self.tmp.name) / "workspaces")
                ),
                max_attempts=1,
                activity_monitor=WorkerActivityMonitor(store=self.store),
            )
        self.assertEqual([call[0][-1] for call in calls], ["gpt-6-luna", "gpt-6.1-sol"])
        self.assertIn(
            "Needs a broad migration",
            calls[1][1]["task"]["prompt_context"]["task_instructions"][-1],
        )
        self.assertEqual(self.store.get_work_model_tier(work_item_id="item_a"), "low")
        self.assertEqual(result.run_record.result_status, "success")

    def test_work_item_id_cannot_escape_root(self):
        executor = CodexExecutor(workspace_root=str(Path(self.tmp.name) / "workspaces"))
        with self.assertRaisesRegex(ValueError, "invalid work_item_id"):
            executor.for_workspace(work_item_id="../shared")
        root = Path(self.tmp.name) / "workspaces"
        root.mkdir()
        (root / "ws_link").symlink_to(Path(self.tmp.name), target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "escapes workspace root"):
            executor.for_workspace(work_item_id="ws_link")


if __name__ == "__main__":
    unittest.main()
