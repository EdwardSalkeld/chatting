import unittest
from datetime import datetime, timezone

from app.models import (
    ActionProposal,
    AuditEvent,
    ApplyResult,
    AttachmentRef,
    ConfigUpdate,
    ConfigUpdateDecision,
    ExecutionResult,
    ExecutionConstraints,
    OutboundMessage,
    PolicyDecision,
    ReplyChannel,
    RunRecord,
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
            source="email",
            actor="alice@example.com",
            content="Please summarize and reply",
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
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
                "source": "email",
                "actor": "alice@example.com",
                "content": "Please summarize and reply",
                "reply_channel": {"type": "email", "target": "alice@example.com"},
            },
        )

    def test_execution_constraints_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "timeout_seconds"):
            ExecutionConstraints(timeout_seconds=0, max_tokens=10)

        with self.assertRaisesRegex(ValueError, "max_tokens"):
            ExecutionConstraints(timeout_seconds=10, max_tokens=0)


class ExecutionResultTests(unittest.TestCase):
    def test_execution_result_serializes_expected_shape(self) -> None:
        result = ExecutionResult(
            messages=[OutboundMessage(channel="email", target="alice@example.com", body="Done.")],
            actions=[ActionProposal(type="write_file", path="docs/notes.md", content="hello")],
            config_updates=[ConfigUpdate(path="routing.default_timeout", value=240)],
            requires_human_review=False,
            errors=[],
        )

        self.assertEqual(
            result.to_dict(),
            {
                "schema_version": "1.0",
                "messages": [
                    {"channel": "email", "target": "alice@example.com", "body": "Done."}
                ],
                "actions": [
                    {"type": "write_file", "path": "docs/notes.md", "content": "hello"}
                ],
                "config_updates": [{"path": "routing.default_timeout", "value": 240}],
                "requires_human_review": False,
                "errors": [],
            },
        )

    def test_execution_result_message_requires_body(self) -> None:
        with self.assertRaisesRegex(ValueError, "body is required"):
            OutboundMessage(channel="email", target="alice@example.com", body="")


class PolicyDecisionTests(unittest.TestCase):
    def test_policy_decision_serializes_expected_shape(self) -> None:
        decision = PolicyDecision(
            approved_actions=[],
            blocked_actions=[ActionProposal(type="write_file", path="secrets.txt")],
            approved_messages=[OutboundMessage(channel="email", target="alice@example.com", body="Blocked.")],
            config_updates=ConfigUpdateDecision(
                approved=[],
                pending_review=[ConfigUpdate(path="routing.default_timeout", value=240)],
                rejected=[],
            ),
            reason_codes=["action_not_allowed"],
        )

        self.assertEqual(
            decision.to_dict(),
            {
                "schema_version": "1.0",
                "approved_actions": [],
                "blocked_actions": [{"type": "write_file", "path": "secrets.txt"}],
                "approved_messages": [
                    {"channel": "email", "target": "alice@example.com", "body": "Blocked."}
                ],
                "config_updates": {
                    "approved": [],
                    "pending_review": [{"path": "routing.default_timeout", "value": 240}],
                    "rejected": [],
                },
                "reason_codes": ["action_not_allowed"],
            },
        )

    def test_action_requires_type(self) -> None:
        with self.assertRaisesRegex(ValueError, "type is required"):
            ActionProposal(type="")


class RunRecordTests(unittest.TestCase):
    def test_run_record_serializes_expected_shape(self) -> None:
        record = RunRecord(
            run_id="run_123",
            envelope_id="evt_123",
            source="email",
            workflow="respond_and_optionally_edit",
            policy_profile="default",
            latency_ms=42,
            result_status="success",
            created_at=datetime(2026, 2, 27, 16, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(
            record.to_dict(),
            {
                "schema_version": "1.0",
                "run_id": "run_123",
                "envelope_id": "evt_123",
                "source": "email",
                "workflow": "respond_and_optionally_edit",
                "policy_profile": "default",
                "latency_ms": 42,
                "result_status": "success",
                "created_at": "2026-02-27T16:05:00Z",
            },
        )

    def test_run_record_requires_timezone_aware_timestamp(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            RunRecord(
                run_id="run_1",
                envelope_id="evt_1",
                source="cron",
                workflow="respond_and_optionally_edit",
                policy_profile="default",
                latency_ms=0,
                result_status="success",
                created_at=datetime(2026, 2, 27, 16, 5),
            )


class ApplyResultTests(unittest.TestCase):
    def test_apply_result_serializes_expected_shape(self) -> None:
        result = ApplyResult(
            applied_actions=[],
            skipped_actions=[ActionProposal(type="write_file", path="docs/notes.md")],
            dispatched_messages=[OutboundMessage(channel="email", target="alice@example.com", body="Done.")],
            reason_codes=["noop_applier_skipped_actions"],
        )

        self.assertEqual(
            result.to_dict(),
            {
                "schema_version": "1.0",
                "applied_actions": [],
                "skipped_actions": [{"type": "write_file", "path": "docs/notes.md"}],
                "dispatched_messages": [
                    {"channel": "email", "target": "alice@example.com", "body": "Done."}
                ],
                "reason_codes": ["noop_applier_skipped_actions"],
            },
        )


class AuditEventTests(unittest.TestCase):
    def test_audit_event_serializes_expected_shape(self) -> None:
        event = AuditEvent(
            run_id="run_123",
            envelope_id="evt_123",
            source="email",
            workflow="respond_and_optionally_edit",
            policy_profile="default",
            result_status="success",
            detail={"approved_action_count": 1, "reason_codes": []},
            created_at=datetime(2026, 2, 27, 16, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(
            event.to_dict(),
            {
                "schema_version": "1.0",
                "run_id": "run_123",
                "envelope_id": "evt_123",
                "source": "email",
                "workflow": "respond_and_optionally_edit",
                "policy_profile": "default",
                "result_status": "success",
                "detail": {"approved_action_count": 1, "reason_codes": []},
                "created_at": "2026-02-27T16:05:00Z",
            },
        )

    def test_audit_event_requires_timezone_aware_timestamp(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            AuditEvent(
                run_id="run_1",
                envelope_id="evt_1",
                source="cron",
                workflow="respond_and_optionally_edit",
                policy_profile="default",
                result_status="success",
                detail={},
                created_at=datetime(2026, 2, 27, 16, 5),
            )


class SchemaVersionValidationTests(unittest.TestCase):
    def test_top_level_models_require_non_empty_schema_version(self) -> None:
        with self.assertRaisesRegex(ValueError, "schema_version is required"):
            TaskEnvelope(
                id="evt_1",
                source="email",
                received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
                actor="alice@example.com",
                content="content",
                attachments=[],
                context_refs=[],
                policy_profile="default",
                reply_channel=ReplyChannel(type="email", target="alice@example.com"),
                dedupe_key="email:1",
                schema_version="",
            )

        with self.assertRaisesRegex(ValueError, "schema_version is required"):
            RoutedTask(
                task_id="task_1",
                envelope_id="evt_1",
                workflow="respond_and_optionally_edit",
                priority="normal",
                execution_constraints=ExecutionConstraints(timeout_seconds=10, max_tokens=1000),
                policy_profile="default",
                schema_version="",
            )

        with self.assertRaisesRegex(ValueError, "schema_version is required"):
            ExecutionResult(
                messages=[],
                actions=[],
                config_updates=[],
                requires_human_review=False,
                errors=[],
                schema_version="",
            )

        with self.assertRaisesRegex(ValueError, "schema_version is required"):
            PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
                schema_version="",
            )

        with self.assertRaisesRegex(ValueError, "schema_version is required"):
            ApplyResult(
                applied_actions=[],
                skipped_actions=[],
                dispatched_messages=[],
                reason_codes=[],
                schema_version="",
            )

        with self.assertRaisesRegex(ValueError, "schema_version is required"):
            RunRecord(
                run_id="run_1",
                envelope_id="evt_1",
                source="email",
                workflow="respond_and_optionally_edit",
                policy_profile="default",
                latency_ms=1,
                result_status="success",
                created_at=datetime(2026, 2, 27, 16, 5, tzinfo=timezone.utc),
                schema_version="",
            )

        with self.assertRaisesRegex(ValueError, "schema_version is required"):
            AuditEvent(
                run_id="run_1",
                envelope_id="evt_1",
                source="email",
                workflow="respond_and_optionally_edit",
                policy_profile="default",
                result_status="success",
                detail={},
                created_at=datetime(2026, 2, 27, 16, 5, tzinfo=timezone.utc),
                schema_version="",
            )

    def test_top_level_models_reject_unsupported_schema_version(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported_schema_version:2.0"):
            TaskEnvelope(
                id="evt_1",
                source="email",
                received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
                actor="alice@example.com",
                content="content",
                attachments=[],
                context_refs=[],
                policy_profile="default",
                reply_channel=ReplyChannel(type="email", target="alice@example.com"),
                dedupe_key="email:1",
                schema_version="2.0",
            )

        with self.assertRaisesRegex(ValueError, "unsupported_schema_version:2.0"):
            ExecutionResult(
                messages=[],
                actions=[],
                config_updates=[],
                requires_human_review=False,
                errors=[],
                schema_version="2.0",
            )


if __name__ == "__main__":
    unittest.main()
