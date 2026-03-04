import unittest
from datetime import datetime, timezone

from app.models import ReplyChannel, TaskEnvelope
from app.router import RuleBasedRouter


class RuleBasedRouterTests(unittest.TestCase):
    def test_routes_email_to_respond_workflow_with_defaults(self) -> None:
        envelope = TaskEnvelope(
            id="email:msg_1",
            source="email",
            received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
            actor="alice@example.com",
            content="Subject: hello\n\nPlease summarize this thread.",
            attachments=[],
            context_refs=["repo:/home/edward/chatting"],
            policy_profile="default",
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:msg_1",
        )

        task = RuleBasedRouter().route(envelope)

        self.assertEqual(task.task_id, "task:email:msg_1")
        self.assertEqual(task.envelope_id, envelope.id)
        self.assertEqual(task.workflow, "respond_and_optionally_edit")
        self.assertEqual(task.priority, "normal")
        self.assertEqual(task.execution_constraints.timeout_seconds, 1800)
        self.assertEqual(task.execution_constraints.max_tokens, 12000)
        self.assertEqual(task.policy_profile, "default")
        self.assertEqual(task.source, "email")
        self.assertEqual(task.actor, "alice@example.com")
        self.assertEqual(task.content, envelope.content)
        self.assertEqual(task.reply_channel, envelope.reply_channel)

    def test_routes_cron_to_scheduled_automation_with_cron_constraints(self) -> None:
        envelope = TaskEnvelope(
            id="cron:daily:2026-02-27T00:00:00+00:00",
            source="cron",
            received_at=datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc),
            actor=None,
            content="Run maintenance checks",
            attachments=[],
            context_refs=["repo:/home/edward/chatting"],
            policy_profile="default",
            reply_channel=ReplyChannel(type="log", target="daily"),
            dedupe_key="cron:daily:2026-02-27T00:00:00+00:00",
        )

        task = RuleBasedRouter().route(envelope)

        self.assertEqual(task.workflow, "scheduled_automation")
        self.assertEqual(task.priority, "low")
        self.assertEqual(task.execution_constraints.timeout_seconds, 120)
        self.assertEqual(task.execution_constraints.max_tokens, 8000)

    def test_urgent_email_is_prioritized_high(self) -> None:
        envelope = TaskEnvelope(
            id="email:msg_urgent",
            source="email",
            received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
            actor="alice@example.com",
            content="Subject: URGENT\n\nNeed this done ASAP.",
            attachments=[],
            context_refs=[],
            policy_profile="default",
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:msg_urgent",
        )

        task = RuleBasedRouter().route(envelope)

        self.assertEqual(task.priority, "high")


if __name__ == "__main__":
    unittest.main()
