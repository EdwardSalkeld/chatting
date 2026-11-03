import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from app.models import ReplyChannel
from app.worker.notification_triage import NotificationTriage, from_environment
from app.worker.owner_rules import RulesSnapshot
from tests.test_worker_inbox import _telegram_task


def task(number=1, body="Build queued", *, second=None, author="builder[bot]"):
    message = _telegram_task(number)
    return replace(
        message,
        envelope=replace(
            message.envelope,
            source="webhook",
            actor=author,
            reply_channel=ReplyChannel(
                type="github",
                target="https://github.com/o/r/pull/1",
                metadata={
                    "github_event": "issue_comment",
                    "github_subject": "https://github.com/o/r/pull/1",
                    "action": "edited",
                    "notification_candidate": {
                        "version": 1,
                        "object_id": 10,
                        "author": author,
                        "body": body,
                        "updated_at": f"2026-11-03T10:00:{second if second is not None else number:02d}Z",
                    },
                },
            ),
        ),
    )


def answer(probability):
    return {"answers": {"needs_attention": {"type": "noul", "noul": probability}}}


class NotificationTriageTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = str(Path(temp.name) / "triage.db")
        self.rules = patch(
            "app.worker.notification_triage.load_rules",
            return_value=RulesSnapshot("abc123", "Trusted test rule", ("test",)),
        ).start()
        self.post = patch(
            "app.worker.notification_triage.typesafe.post", return_value=answer(0.1)
        ).start()
        self.addCleanup(patch.stopall)

    def triage(self, mode="enforce"):
        return NotificationTriage(self.path, repo="repo", key="test-key", mode=mode)

    def test_observation_forwards_and_records_what_would_be_suppressed(self):
        self.assertTrue(self.triage("observe").forward(task()))
        with sqlite3.connect(self.path) as db:
            detail = json.loads(
                db.execute("SELECT detail FROM decisions").fetchone()[0]
            )
        self.assertFalse(detail["would_forward"])
        self.assertTrue(detail["forward"])
        self.assertEqual(detail["rules_revision"], "abc123")

    def test_sequence_restart_and_redelivery_preserve_comparison_and_decision(self):
        triage = self.triage()
        self.post.return_value = answer(0.95)
        self.assertTrue(triage.forward(task(1, "Ready commit abc")))
        self.post.return_value = answer(0.05)
        self.assertFalse(triage.forward(task(2, "Building commit abc")))
        state = self.post.call_args.args[2]["state"]
        self.assertEqual(state["last_forwarded"]["event_id"], task(1).task_id)
        triage = self.triage()
        self.assertFalse(triage.forward(task(3, "Ready commit abc")))
        state = self.post.call_args.args[2]["state"]
        self.assertEqual(state["previous"]["event_id"], task(2).task_id)
        self.assertEqual(state["last_forwarded"]["event_id"], task(1).task_id)
        calls = self.post.call_count
        self.assertFalse(triage.forward(task(3, "Ready commit abc")))
        self.assertEqual(self.post.call_count, calls)
        self.post.return_value = answer(0.9)
        self.assertTrue(triage.forward(task(4, "Failed commit def")))
        self.assertTrue(triage.forward(task(5, "Recovered commit def")))

    def test_uncertainty_invalid_answers_and_transport_failure_forward(self):
        triage = self.triage()
        for number, response in enumerate(
            (
                answer(0.31),
                answer(0.5),
                answer(0.7),
                answer(True),
                answer(float("nan")),
                answer(2),
                {},
            ),
            start=1,
        ):
            self.post.return_value = response
            self.assertTrue(triage.forward(task(number)))
        self.post.side_effect = TimeoutError()
        self.assertTrue(triage.forward(task(8)))
        self.rules.side_effect = RuntimeError("fetch failed")
        self.assertTrue(triage.forward(task(9)))

    def test_out_of_order_event_does_not_replace_newer_context(self):
        triage = self.triage()
        self.assertFalse(triage.forward(task(5)))
        calls = self.post.call_count
        self.assertTrue(triage.forward(task(2)))
        self.assertEqual(self.post.call_count, calls)
        self.assertFalse(triage.forward(task(6)))
        self.assertEqual(
            self.post.call_args.args[2]["state"]["previous"]["event_id"],
            task(5).task_id,
        )

    def test_non_candidates_and_incomplete_data_never_call_provider(self):
        triage = self.triage()
        self.assertTrue(triage.forward(_telegram_task(1)))
        for field, value in (
            ("updated_at", ""),
            ("body", "x" * 48001),
            ("object_id", True),
            ("author", "human"),
        ):
            message = task()
            message.envelope.reply_channel.metadata["notification_candidate"][field] = (
                value
            )
            self.assertTrue(triage.forward(message))
        self.post.assert_not_called()

    def test_disabled_by_default_and_missing_configuration_is_safe(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertIsNone(from_environment(self.path))
        with patch.dict(
            "os.environ", {"CHATTING_NOTIFICATION_TRIAGE_MODE": "observe"}, clear=True
        ):
            self.assertIsNone(from_environment(self.path))

    def test_failed_ack_reuses_suppression_without_an_executor_task(self):
        from app.broker import PickedMessage
        from app.state import SQLiteStateStore
        from app.worker.activity import WorkerActivityMonitor
        from app.worker.inbox import InboxCollector
        from unittest.mock import Mock

        broker = Mock()
        broker.pickup_json.return_value = PickedMessage(
            guid="guid", payload=task().to_dict()
        )
        broker.ack.side_effect = [OSError("lost ack"), None]
        store = SQLiteStateStore(self.path + ".worker")
        collector = InboxCollector(
            broker, store, WorkerActivityMonitor(store=store), 1, self.triage()
        )
        with self.assertRaises(OSError):
            collector.collect_once()
        self.assertTrue(collector.collect_once())
        self.assertIsNone(store.claim_next_inbox_task())
        self.assertEqual(self.post.call_count, 1)
