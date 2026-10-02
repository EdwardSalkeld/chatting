import tempfile
import unittest
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from app.broker import TaskQueueMessage
from app.models import AuditEvent, ReplyChannel, RunRecord, TaskEnvelope
from app.state import SQLiteStateStore
from app.task_ledger import TaskLedgerStore


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

    def test_telegram_channel_is_persistent_even_for_unthreaded_new_objectives(self):
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
        self.assertEqual(first.workspace_id, reply.workspace_id)
        self.assertEqual(reply.reason, "telegram_channel")
        new_objective = self.stage(
            task(4, "im", "chat", content="Separately, fix backups")
        )
        self.assertEqual(first.work_item_id, new_objective.work_item_id)
        reopened = SQLiteStateStore(self.store._db_path)
        self.assertEqual(
            reopened.get_work_assignment(
                task_id=task(4, "im", "chat").task_id
            ).workspace_id,
            first.workspace_id,
        )

    def test_telegram_topics_and_general_email_lane(self):
        topic_a = self.stage(
            task(1, "im", "chat", metadata={"message_id": 101, "message_thread_id": 7})
        )
        topic_b = self.stage(
            task(2, "im", "chat", metadata={"message_id": 102, "message_thread_id": 8})
        )
        self.assertNotEqual(topic_a.work_item_id, topic_b.work_item_id)
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
        self.assertNotEqual(topic_a.work_item_id, email_a.work_item_id)

    def test_github_notification_returns_to_telegram_item(self):
        original = self.stage(task(1, "im", "chat", metadata={"message_id": 101}))
        self.store.register_work_artifact(
            work_item_id=original.work_item_id,
            kind="github_pr",
            key="https://github.com/EdwardSalkeld/chatting/pull/50",
        )
        notification = self.stage(
            task(
                2,
                "email",
                "notifications@github.com",
                content="CI failed: https://github.com/EdwardSalkeld/chatting/pull/50/checks",
                actor="notifications@github.com",
                metadata={"message_id": "<ci@github.com>"},
            )
        )
        self.assertEqual(notification.work_item_id, original.work_item_id)
        self.assertEqual(notification.workspace_id, original.workspace_id)
        self.assertEqual(notification.reason, "github_pr")
        self.assertEqual(
            self.store.preferred_work_reply(work_item_id=original.work_item_id).target,
            "chat",
        )

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
        self.assertEqual(unknown.reason, "general_lane")

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
        assigned = replace(
            first, work_item_id="item_from_handler", workspace_id="ws_from_handler"
        )
        lane = self.stage(assigned)
        self.assertEqual(lane.work_item_id, "item_from_handler")
        self.assertEqual(lane.workspace_id, "ws_from_handler")
        self.assertEqual(lane.reason, "handler_assignment")

    def test_pr_registration_records_handler_mapping(self):
        handler_path = str(Path(self.tmp.name) / "handler.db")
        handler = TaskLedgerStore(handler_path)
        with sqlite3.connect(handler_path) as connection:
            connection.execute(
                "CREATE TABLE task_assignments (task_id TEXT PRIMARY KEY, work_item_id TEXT)"
            )
            connection.execute(
                "CREATE TABLE work_item_artifacts (kind TEXT, artifact_key TEXT, work_item_id TEXT, PRIMARY KEY (kind, artifact_key))"
            )
            connection.execute(
                "INSERT INTO task_assignments VALUES ('task:1', 'item_1')"
            )
        handler.register_pr(
            task_id="task:1",
            work_item_id="item_1",
            pr_url="https://github.com/Owner/Repo/pull/50",
        )
        with sqlite3.connect(handler_path) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT artifact_key, work_item_id FROM work_item_artifacts"
                ).fetchone(),
                ("owner/repo#50", "item_1"),
            )
        with self.assertRaisesRegex(ValueError, "do not match"):
            handler.register_pr(
                task_id="task:1",
                work_item_id="item_wrong",
                pr_url="https://github.com/Owner/Repo/pull/51",
            )


if __name__ == "__main__":
    unittest.main()
