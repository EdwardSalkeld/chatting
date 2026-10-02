import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.broker import TaskQueueMessage
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore


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

    def test_telegram_reply_and_single_item_followup_use_workspace(self):
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
        self.assertEqual(reply.reason, "telegram_reply")

    def test_telegram_topics_and_distinct_email_threads(self):
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
        self.assertNotEqual(email_a.work_item_id, email_b.work_item_id)
        self.assertEqual(email_a.work_item_id, email_reply.work_item_id)

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

    def test_unknown_notification_and_schedules_get_distinct_items(self):
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
        self.assertNotEqual(unknown.work_item_id, other.work_item_id)


if __name__ == "__main__":
    unittest.main()
