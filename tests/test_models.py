import unittest
from datetime import datetime, timezone

from app.models import (
    AttachmentRef,
    ExecutionConstraints,
    ReplyChannel,
    RoutedTask,
    TaskEnvelope,
)


class TaskEnvelopeTests(unittest.TestCase):
    def test_task_envelope_serializes_expected_shape(self) -> None:
        envelope = TaskEnvelope(
            id="evt_123",
            source="email",
            received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
            actor="alice@example.com",
            content="Please summarize and reply",
            attachments=[AttachmentRef(uri="s3://bucket/file.txt", name="file.txt")],
            context_refs=["repo:/home/edward/chatting"],
            policy_profile="default",
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:provider_msg_id",
        )

        self.assertEqual(
            envelope.to_dict(),
            {
                "schema_version": "1.0",
                "id": "evt_123",
                "source": "email",
                "received_at": "2026-02-27T16:00:00Z",
                "actor": "alice@example.com",
                "content": "Please summarize and reply",
                "attachments": [{"uri": "s3://bucket/file.txt", "name": "file.txt"}],
                "context_refs": ["repo:/home/edward/chatting"],
                "policy_profile": "default",
                "reply_channel": {"type": "email", "target": "alice@example.com"},
                "dedupe_key": "email:provider_msg_id",
            },
        )

    def test_task_envelope_requires_timezone_aware_timestamp(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            TaskEnvelope(
                id="evt_1",
                source="cron",
                received_at=datetime(2026, 2, 27, 16, 0),
                actor=None,
                content="hello",
                attachments=[],
                context_refs=[],
                policy_profile="default",
                reply_channel=ReplyChannel(type="noop", target="stdout"),
                dedupe_key="cron:job:daily",
            )


class RoutedTaskTests(unittest.TestCase):
    def test_routed_task_serializes_expected_shape(self) -> None:
        task = RoutedTask(
            task_id="task_123",
            envelope_id="evt_123",
            workflow="respond_and_optionally_edit",
            priority="normal",
            execution_constraints=ExecutionConstraints(timeout_seconds=180, max_tokens=12000),
            policy_profile="default",
        )

        self.assertEqual(
            task.to_dict(),
            {
                "schema_version": "1.0",
                "task_id": "task_123",
                "envelope_id": "evt_123",
                "workflow": "respond_and_optionally_edit",
                "priority": "normal",
                "execution_constraints": {
                    "timeout_seconds": 180,
                    "max_tokens": 12000,
                },
                "policy_profile": "default",
            },
        )

    def test_execution_constraints_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "timeout_seconds"):
            ExecutionConstraints(timeout_seconds=0, max_tokens=10)

        with self.assertRaisesRegex(ValueError, "max_tokens"):
            ExecutionConstraints(timeout_seconds=10, max_tokens=0)


if __name__ == "__main__":
    unittest.main()
