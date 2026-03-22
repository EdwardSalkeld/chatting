import unittest
from datetime import datetime, timezone

from app.models import AttachmentRef, PromptContext, ReplyChannel, TaskEnvelope
from app.worker.router import RuleBasedRouter
class RuleBasedRouterTests(unittest.TestCase):
    def test_routes_email_to_respond_workflow_with_defaults(self) -> None:
        envelope = TaskEnvelope(
            id="email:msg_1",
            source="email",
            received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
            actor="alice@example.com",
            content="Subject: hello\n\nPlease summarize this thread.",
            attachments=[AttachmentRef(uri="file:///tmp/request.txt", name="request.txt")],
            context_refs=["repo:/home/edward/chatting"],
            prompt_context=PromptContext(
                global_instructions=["Keep replies concise."],
                reply_channel_instructions=["Use email formatting."],
            ),
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
        self.assertEqual(task.event_time, envelope.received_at)
        self.assertEqual(task.source, "email")
        self.assertEqual(task.actor, "alice@example.com")
        self.assertEqual(task.content, envelope.content)
        self.assertEqual(task.attachments, envelope.attachments)
        self.assertEqual(task.prompt_context, envelope.prompt_context)
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
            reply_channel=ReplyChannel(type="log", target="daily"),
            dedupe_key="cron:daily:2026-02-27T00:00:00+00:00",
        )

        task = RuleBasedRouter().route(envelope)

        self.assertEqual(task.workflow, "scheduled_automation")
        self.assertEqual(task.priority, "low")
        self.assertEqual(task.execution_constraints.timeout_seconds, 1800)
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
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:msg_urgent",
        )

        task = RuleBasedRouter().route(envelope)

        self.assertEqual(task.priority, "high")
if __name__ == "__main__":
    unittest.main()
