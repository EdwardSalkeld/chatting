import unittest
from dataclasses import replace
from datetime import datetime, timezone

from app.models import (
    AttachmentRef,
    AuditEvent,
    ExecutionResult,
    OutboundMessage,
    PromptContext,
    ReplyChannel,
    RunRecord,
    TaskEnvelope,
)


class ModelTests(unittest.TestCase):
    def setUp(self):
        self.timestamp = datetime(2026, 2, 27, 16, 5, tzinfo=timezone.utc)
        self.envelope = TaskEnvelope(
            id="evt_1",
            source="email",
            received_at=self.timestamp,
            actor=None,
            content="hello",
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(type="email", target="alice@example.com"),
            dedupe_key="email:1",
        )
        self.run = RunRecord(
            run_id="run_1",
            envelope_id="evt_1",
            source="email",
            workflow="default",
            latency_ms=42,
            result_status="success",
            created_at=self.timestamp,
        )
        self.audit = AuditEvent(
            run_id="run_1",
            envelope_id="evt_1",
            source="email",
            workflow="default",
            result_status="success",
            detail={"reason_codes": []},
            created_at=self.timestamp,
        )

    def test_record_serialization(self):
        common = {
            "schema_version": "1.0",
            "run_id": "run_1",
            "envelope_id": "evt_1",
            "source": "email",
            "workflow": "default",
            "result_status": "success",
            "created_at": "2026-02-27T16:05:00Z",
        }
        for model, expected in (
            (self.run, {**common, "latency_ms": 42}),
            (self.audit, {**common, "detail": {"reason_codes": []}}),
            (
                ExecutionResult(errors=[], stdout="out", stderr="err"),
                {
                    "schema_version": "1.0",
                    "errors": [],
                    "stdout": "out",
                    "stderr": "err",
                },
            ),
        ):
            with self.subTest(model=type(model).__name__):
                self.assertEqual(model.to_dict(), expected)

    def test_top_level_schema_versions_and_timestamps(self):
        for model in (self.envelope, self.run, self.audit, ExecutionResult(errors=[])):
            for version, error in (
                ("", "schema_version is required"),
                ("2.0", "unsupported_schema_version:2.0"),
            ):
                with self.subTest(model=type(model).__name__, version=version):
                    with self.assertRaisesRegex(ValueError, error):
                        replace(model, schema_version=version)
        for model, field in (
            (self.envelope, "received_at"),
            (self.run, "created_at"),
            (self.audit, "created_at"),
        ):
            with self.subTest(model=type(model).__name__):
                with self.assertRaisesRegex(ValueError, "timezone-aware"):
                    replace(model, **{field: self.timestamp.replace(tzinfo=None)})

    def test_required_strings(self):
        for model, field in (
            (AttachmentRef(uri="file:///a", name="a"), "uri"),
            (AttachmentRef(uri="file:///a", name="a"), "name"),
            (self.envelope.reply_channel, "type"),
            (self.envelope.reply_channel, "target"),
            (self.envelope, "content"),
            (self.run, "result_status"),
            (OutboundMessage(channel="email", target="alice", body="hello"), "body"),
        ):
            with self.subTest(model=type(model).__name__, field=field):
                with self.assertRaisesRegex(ValueError, f"{field} is required"):
                    replace(model, **{field: "   "})

    def test_collection_and_transcript_validation(self):
        for model, field, value, error in (
            (self.envelope, "context_refs", ["repo:/valid", " "], "context_refs items"),
            (self.envelope, "attachments", [object()], "attachments items"),
            (
                PromptContext(),
                "global_instructions",
                [" "],
                "prompt_context.global_instructions",
            ),
            (ExecutionResult(errors=[]), "errors", [" "], "errors items"),
            (ExecutionResult(errors=[]), "stdout", object(), "stdout must be a string"),
            (ExecutionResult(errors=[]), "stderr", object(), "stderr must be a string"),
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, error):
                    replace(model, **{field: value})

    def test_outbound_message_requires_body_or_attachment(self):
        with self.assertRaisesRegex(ValueError, "body or attachment is required"):
            OutboundMessage(channel="telegram", target="12345")
        message = OutboundMessage(
            channel="telegram",
            target="12345",
            attachment=AttachmentRef(uri="file:///report.pdf", name="report.pdf"),
        )
        self.assertEqual(
            message.to_dict(),
            {
                "channel": "telegram",
                "target": "12345",
                "attachment": {"uri": "file:///report.pdf", "name": "report.pdf"},
            },
        )
