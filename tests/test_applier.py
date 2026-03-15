import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from tempfile import TemporaryDirectory

from app.applier import (
    IntegratedApplier,
    MessageDispatchError,
    NoOpApplier,
    SmtpEmailSender,
    TelegramMessageSender,
)
from app.models import (
    ActionProposal,
    AttachmentRef,
    ConfigUpdateDecision,
    OutboundMessage,
    PolicyDecision,
    ReplyChannel,
    TaskEnvelope,
)


class NoOpApplierTests(unittest.TestCase):
    def test_skips_approved_actions_and_dispatches_messages(self) -> None:
        decision = PolicyDecision(
            approved_actions=[ActionProposal(type="write_file", path="docs/notes.md")],
            blocked_actions=[],
            approved_messages=[OutboundMessage(channel="email", target="alice@example.com", body="Done.")],
            config_updates=ConfigUpdateDecision(),
            reason_codes=[],
        )

        result = NoOpApplier().apply(decision)

        self.assertEqual(result.applied_actions, [])
        self.assertEqual([action.type for action in result.skipped_actions], ["write_file"])
        self.assertEqual(
            [message.channel for message in result.dispatched_messages],
            ["email"],
        )
        self.assertEqual(result.reason_codes, ["noop_applier_skipped_actions"])

    def test_emits_reason_when_blocked_actions_exist(self) -> None:
        decision = PolicyDecision(
            approved_actions=[],
            blocked_actions=[ActionProposal(type="run_shell", path="rm -rf /")],
            approved_messages=[],
            config_updates=ConfigUpdateDecision(),
            reason_codes=["action_not_allowed"],
        )

        result = NoOpApplier().apply(decision)

        self.assertEqual(result.reason_codes, ["policy_blocked_actions_present"])


@dataclass
class _RecordingEmailSender:
    sent: list[tuple[str, str, str | None]]

    def send(self, target: str, body: str, *, subject: str | None = None) -> None:
        self.sent.append((target, body, subject))


class IntegratedApplierTests(unittest.TestCase):
    def test_apply_writes_files_and_dispatches_email_and_log_messages(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingEmailSender(sent=[])
            decision = PolicyDecision(
                approved_actions=[
                    ActionProposal(
                        type="write_file",
                        path="docs/generated.txt",
                        content="hello from applier",
                    )
                ],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="email",
                        target="alice@example.com",
                        body="Done.",
                    ),
                    OutboundMessage(channel="log", target="ops", body="Applied."),
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            result = IntegratedApplier(base_dir=tmpdir, email_sender=sender).apply(decision)

            written_path = Path(tmpdir) / "docs" / "generated.txt"
            self.assertTrue(written_path.exists())
            self.assertEqual(written_path.read_text(encoding="utf-8"), "hello from applier")
            self.assertEqual(result.applied_actions, decision.approved_actions)
            self.assertEqual(result.skipped_actions, [])
            self.assertEqual(
                sender.sent,
                [("alice@example.com", "Done.", None)],
            )
            self.assertEqual(
                [message.channel for message in result.dispatched_messages],
                ["email", "log"],
            )
            self.assertEqual(result.reason_codes, [])

    def test_apply_email_reply_keeps_subject_and_quotes_original(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingEmailSender(sent=[])
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="email",
                        target="alice@example.com",
                        body="Here is the answer.",
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )
            envelope = _email_envelope(
                subject="Quarterly update",
                body="Can you summarize the key points?",
            )

            result = IntegratedApplier(base_dir=tmpdir, email_sender=sender).apply(
                decision,
                envelope=envelope,
            )

            self.assertEqual(result.reason_codes, [])
            self.assertEqual(len(sender.sent), 1)
            target, body, subject = sender.sent[0]
            self.assertEqual(target, "alice@example.com")
            self.assertEqual(subject, "Re: Quarterly update")
            self.assertIn("Here is the answer.", body)
            self.assertIn("Original message:", body)
            self.assertIn("> Can you summarize the key points?", body)

    def test_apply_email_reply_strips_subject_line_from_body(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingEmailSender(sent=[])
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="email",
                        target="alice@example.com",
                        body="Subject: Re: Ice-cream\n\nGreat choice. Let's go classic.",
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )
            envelope = _email_envelope(
                subject="Ice-cream",
                body="Yes please, let's focus on classic",
            )

            IntegratedApplier(base_dir=tmpdir, email_sender=sender).apply(
                decision,
                envelope=envelope,
            )

            _, body, subject = sender.sent[0]
            self.assertEqual(subject, "Re: Ice-cream")
            self.assertFalse(body.lstrip().startswith("Subject:"))
            self.assertIn("Great choice. Let's go classic.", body)

    def test_apply_dispatches_telegram_message_with_sender(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingTelegramSender(sent=[])
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        body="Done via telegram.",
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            result = IntegratedApplier(base_dir=tmpdir, telegram_sender=sender).apply(decision)

            self.assertEqual(
                sender.sent,
                [("12345", OutboundMessage(channel="telegram", target="12345", body="Done via telegram."))],
            )
            self.assertEqual(
                [message.channel for message in result.dispatched_messages],
                ["telegram"],
            )
            self.assertEqual(result.reason_codes, [])

    def test_apply_dispatches_telegram_photo_attachment(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingTelegramSender(sent=[])
            image_path = Path(tmpdir) / "menu.png"
            image_path.write_bytes(b"png")
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        body="caption",
                        attachment=AttachmentRef(uri=image_path.as_uri(), name="menu.png"),
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            result = IntegratedApplier(base_dir=tmpdir, telegram_sender=sender).apply(decision)

            self.assertEqual(len(sender.sent), 1)
            self.assertEqual(sender.sent[0][0], "12345")
            self.assertEqual(sender.sent[0][1].attachment, AttachmentRef(uri=image_path.as_uri(), name="menu.png"))
            self.assertEqual(result.dispatched_messages[0].attachment, AttachmentRef(uri=image_path.as_uri(), name="menu.png"))

    def test_apply_dispatches_telegram_document_attachment(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingTelegramSender(sent=[])
            pdf_path = Path(tmpdir) / "menu.pdf"
            pdf_path.write_bytes(b"%PDF")
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        attachment=AttachmentRef(uri=pdf_path.as_uri(), name="menu.pdf"),
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            result = IntegratedApplier(base_dir=tmpdir, telegram_sender=sender).apply(decision)

            self.assertEqual(len(sender.sent), 1)
            self.assertEqual(sender.sent[0][1].attachment, AttachmentRef(uri=pdf_path.as_uri(), name="menu.pdf"))
            self.assertIsNone(result.dispatched_messages[0].body)

    def test_apply_raises_attachment_dispatch_error_for_missing_file(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = TelegramMessageSender(bot_token="token", http_post_multipart=lambda *_args: {"ok": True})
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        attachment=AttachmentRef(uri="file:///does/not/exist.pdf", name="missing.pdf"),
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            with self.assertRaises(MessageDispatchError) as context:
                IntegratedApplier(base_dir=tmpdir, telegram_sender=sender).apply(decision)

            self.assertEqual(context.exception.reason_code, "telegram_attachment_missing")

    def test_apply_raises_attachment_dispatch_error_on_telegram_api_failure(self) -> None:
        with TemporaryDirectory() as tmpdir:
            pdf_path = Path(tmpdir) / "menu.pdf"
            pdf_path.write_bytes(b"%PDF")
            sender = TelegramMessageSender(
                bot_token="token",
                http_post_multipart=lambda *_args: {"ok": False},
            )
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        body="menu",
                        attachment=AttachmentRef(uri=pdf_path.as_uri(), name="menu.pdf"),
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            with self.assertRaises(MessageDispatchError) as context:
                IntegratedApplier(base_dir=tmpdir, telegram_sender=sender).apply(decision)

            self.assertEqual(context.exception.reason_code, "telegram_attachment_send_failed")

    def test_apply_skips_write_outside_base_dir(self) -> None:
        with TemporaryDirectory() as tmpdir:
            decision = PolicyDecision(
                approved_actions=[
                    ActionProposal(type="write_file", path="../escape.txt", content="nope")
                ],
                blocked_actions=[],
                approved_messages=[],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            result = IntegratedApplier(base_dir=tmpdir).apply(decision)

            self.assertEqual(result.applied_actions, [])
            self.assertEqual(len(result.skipped_actions), 1)
            self.assertEqual(result.reason_codes, ["write_file_outside_base_dir"])

    def test_apply_marks_email_dispatch_unconfigured(self) -> None:
        with TemporaryDirectory() as tmpdir:
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="email",
                        target="alice@example.com",
                        body="Done.",
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            result = IntegratedApplier(base_dir=tmpdir).apply(decision)

            self.assertEqual(result.dispatched_messages, [])
            self.assertEqual(result.reason_codes, ["email_dispatch_not_configured"])

    def test_apply_marks_telegram_dispatch_unconfigured(self) -> None:
        with TemporaryDirectory() as tmpdir:
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        body="Done.",
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            result = IntegratedApplier(base_dir=tmpdir).apply(decision)

            self.assertEqual(result.dispatched_messages, [])
            self.assertEqual(result.reason_codes, ["telegram_dispatch_not_configured"])

    def test_apply_maps_final_channel_to_envelope_reply_channel_for_telegram(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingTelegramSender(sent=[])
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(
                        channel="final",
                        target="user",
                        body="Answer from model.",
                    )
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )
            envelope = TaskEnvelope(
                id="telegram:test",
                source="im",
                received_at=datetime(2026, 3, 2, tzinfo=timezone.utc),
                actor="8605042448:edsalkeld",
                content="Question",
                attachments=[],
                context_refs=["repo:/home/edward/chatting"],
                policy_profile="default",
                reply_channel=ReplyChannel(type="telegram", target="8605042448"),
                dedupe_key="telegram:test",
            )

            result = IntegratedApplier(base_dir=tmpdir, telegram_sender=sender).apply(
                decision,
                envelope=envelope,
            )

            self.assertEqual(
                sender.sent,
                [("8605042448", OutboundMessage(channel="final", target="user", body="Answer from model."))],
            )
            self.assertEqual(
                [message.channel for message in result.dispatched_messages],
                ["telegram"],
            )
            self.assertEqual(
                [message.target for message in result.dispatched_messages],
                ["8605042448"],
            )
            self.assertEqual(result.reason_codes, [])

    def test_apply_raises_dispatch_error_with_partial_progress_on_telegram_failure(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _FailingTelegramSender()
            decision = PolicyDecision(
                approved_actions=[],
                blocked_actions=[],
                approved_messages=[
                    OutboundMessage(channel="telegram", target="12345", body="👀"),
                    OutboundMessage(channel="telegram", target="12345", body="working"),
                ],
                config_updates=ConfigUpdateDecision(),
                reason_codes=[],
            )

            with self.assertRaises(MessageDispatchError) as context:
                IntegratedApplier(base_dir=tmpdir, telegram_sender=sender).apply(decision)

            self.assertEqual(context.exception.reason_code, "telegram_dispatch_failed")
            self.assertEqual(
                context.exception.dispatched_messages,
                [OutboundMessage(channel="telegram", target="12345", body="👀")],
            )


class SmtpEmailSenderTests(unittest.TestCase):
    def test_send_uses_smtp_client(self) -> None:
        client = _FakeSmtpClient()
        sender = SmtpEmailSender(
            host="smtp.example.com",
            port=465,
            from_address="bot@example.com",
            username="bot",
            password="secret",
            smtp_client_factory=lambda _host, _port: client,
        )

        sender.send("alice@example.com", "hello")

        self.assertEqual(client.login_args, ("bot", "secret"))
        self.assertEqual(len(client.messages), 1)
        message: EmailMessage = client.messages[0]
        self.assertEqual(message["To"], "alice@example.com")
        self.assertEqual(message["From"], "bot@example.com")
        self.assertEqual(message["Subject"], "Automation response")
        self.assertTrue(client.quit_called)


class TelegramMessageSenderTests(unittest.TestCase):
    def test_send_posts_message_and_validates_ok_response(self) -> None:
        seen_calls: list[tuple[str, dict[str, object], float]] = []

        def fake_http_post_json(
            url: str,
            payload: dict[str, object],
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, timeout))
            return {"ok": True, "result": {"message_id": 1}}

        sender = TelegramMessageSender(
            bot_token="token",
            http_post_json=fake_http_post_json,
        )

        sender.send("12345", OutboundMessage(channel="telegram", target="12345", body="hello"))

        self.assertEqual(len(seen_calls), 1)
        call_url, call_payload, call_timeout = seen_calls[0]
        self.assertEqual(
            call_url,
            "https://api.telegram.org/bottoken/sendMessage",
        )
        self.assertEqual(
            call_payload,
            {"chat_id": "12345", "text": "hello", "parse_mode": "Markdown"},
        )
        self.assertEqual(call_timeout, 10.0)

    def test_send_retries_without_parse_mode_when_telegram_rejects_entities(self) -> None:
        seen_calls: list[tuple[str, dict[str, object], float]] = []

        def fake_http_post_json(
            url: str,
            payload: dict[str, object],
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, timeout))
            if len(seen_calls) == 1:
                return {"ok": False, "description": "Bad Request: can't parse entities"}
            return {"ok": True, "result": {"message_id": 2}}

        sender = TelegramMessageSender(
            bot_token="token",
            http_post_json=fake_http_post_json,
        )

        sender.send("12345", OutboundMessage(channel="telegram", target="12345", body="hello"))

        self.assertEqual(len(seen_calls), 2)
        self.assertEqual(
            seen_calls[0][1],
            {"chat_id": "12345", "text": "hello", "parse_mode": "Markdown"},
        )
        self.assertEqual(
            seen_calls[1][1],
            {"chat_id": "12345", "text": "hello"},
        )

    def test_send_raises_when_response_not_ok(self) -> None:
        sender = TelegramMessageSender(
            bot_token="token",
            http_post_json=lambda _url, _payload, _timeout: {"ok": False},
        )

        with self.assertRaisesRegex(RuntimeError, "telegram_send_failed"):
            sender.send("12345", OutboundMessage(channel="telegram", target="12345", body="hello"))

    def test_send_photo_attachment_posts_multipart_with_caption(self) -> None:
        seen_calls: list[tuple[str, dict[str, object], str, Path, float]] = []

        def fake_http_post_multipart(
            url: str,
            payload: dict[str, object],
            field_name: str,
            file_path: Path,
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, field_name, file_path, timeout))
            return {"ok": True, "result": {"message_id": 3}}

        with TemporaryDirectory() as tmpdir:
            image_path = Path(tmpdir) / "menu.jpg"
            image_path.write_bytes(b"jpg")
            sender = TelegramMessageSender(
                bot_token="token",
                http_post_multipart=fake_http_post_multipart,
            )

            sender.send(
                "12345",
                OutboundMessage(
                    channel="telegram",
                    target="12345",
                    body="fresh menu",
                    attachment=AttachmentRef(uri=image_path.as_uri(), name="menu.jpg"),
                ),
            )

        self.assertEqual(len(seen_calls), 1)
        call_url, call_payload, field_name, file_path, call_timeout = seen_calls[0]
        self.assertEqual(call_url, "https://api.telegram.org/bottoken/sendPhoto")
        self.assertEqual(
            call_payload,
            {"chat_id": "12345", "caption": "fresh menu", "parse_mode": "Markdown"},
        )
        self.assertEqual(field_name, "photo")
        self.assertEqual(file_path.name, "menu.jpg")
        self.assertEqual(call_timeout, 10.0)

    def test_send_document_attachment_retries_without_parse_mode(self) -> None:
        seen_calls: list[tuple[str, dict[str, object], str, Path, float]] = []

        def fake_http_post_multipart(
            url: str,
            payload: dict[str, object],
            field_name: str,
            file_path: Path,
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, field_name, file_path, timeout))
            if len(seen_calls) == 1:
                return {"ok": False, "description": "Bad Request: can't parse entities"}
            return {"ok": True, "result": {"message_id": 4}}

        with TemporaryDirectory() as tmpdir:
            pdf_path = Path(tmpdir) / "menu.pdf"
            pdf_path.write_bytes(b"%PDF")
            sender = TelegramMessageSender(
                bot_token="token",
                http_post_multipart=fake_http_post_multipart,
            )

            sender.send(
                "12345",
                OutboundMessage(
                    channel="telegram",
                    target="12345",
                    body="**menu**",
                    attachment=AttachmentRef(uri=pdf_path.as_uri(), name="menu.pdf"),
                ),
            )

        self.assertEqual(len(seen_calls), 2)
        self.assertEqual(seen_calls[0][0], "https://api.telegram.org/bottoken/sendDocument")
        self.assertEqual(seen_calls[0][1]["parse_mode"], "Markdown")
        self.assertNotIn("parse_mode", seen_calls[1][1])
        self.assertEqual(seen_calls[0][2], "document")

    def test_send_attachment_raises_when_telegram_response_not_ok(self) -> None:
        with TemporaryDirectory() as tmpdir:
            pdf_path = Path(tmpdir) / "menu.pdf"
            pdf_path.write_bytes(b"%PDF")
            sender = TelegramMessageSender(
                bot_token="token",
                http_post_multipart=lambda *_args: {"ok": False},
            )

            with self.assertRaisesRegex(RuntimeError, "telegram_attachment_send_failed"):
                sender.send(
                    "12345",
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        attachment=AttachmentRef(uri=pdf_path.as_uri(), name="menu.pdf"),
                    ),
                )


class _FakeSmtpClient:
    def __init__(self) -> None:
        self.login_args: tuple[str, str] | None = None
        self.messages: list[EmailMessage] = []
        self.quit_called = False

    def login(self, username: str, password: str) -> None:
        self.login_args = (username, password)

    def send_message(self, message: EmailMessage) -> None:
        self.messages.append(message)

    def quit(self) -> None:
        self.quit_called = True


@dataclass
class _RecordingTelegramSender:
    sent: list[tuple[str, OutboundMessage]]

    def send(self, target: str, message: OutboundMessage) -> None:
        self.sent.append((target, message))


class _FailingTelegramSender:
    def __init__(self) -> None:
        self._count = 0

    def send(self, target: str, message: OutboundMessage) -> None:
        self._count += 1
        if self._count == 2:
            raise RuntimeError("simulated dispatch failure")


def _email_envelope(*, subject: str, body: str):
    from app.models import ReplyChannel, TaskEnvelope

    return TaskEnvelope(
        id="email:test",
        source="email",
        received_at=datetime(2026, 3, 2, tzinfo=timezone.utc),
        actor="alice@example.com",
        content=f"Subject: {subject}\n\n{body}",
        attachments=[],
        context_refs=["repo:/home/edward/chatting"],
        policy_profile="default",
        reply_channel=ReplyChannel(type="email", target="alice@example.com"),
        dedupe_key="email:test",
    )


if __name__ == "__main__":
    unittest.main()
