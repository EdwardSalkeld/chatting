import io
import unittest
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.handler.applier import (
    GitHubIssueCommentSender,
    IntegratedApplier,
    MessageDispatchError,
    SmtpEmailSender,
    TelegramMessageSender,
)
from app.handler.applier.integrated import _default_http_post_json
from app.models import (
    AttachmentRef,
    OutboundMessage,
    ReplyChannel,
    TaskEnvelope,
)


@dataclass
class _RecordingEmailSender:
    sent: list[tuple[str, str, str | None]]

    def send(self, target: str, body: str, *, subject: str | None = None) -> None:
        self.sent.append((target, body, subject))


class IntegratedApplierTests(unittest.TestCase):
    def test_dispatch_sends_email_message(self) -> None:
        sender = _RecordingEmailSender(sent=[])
        message = OutboundMessage(
            channel="email",
            target="alice@example.com",
            body="Done.",
        )

        dispatched = IntegratedApplier(email_sender=sender).dispatch(message)

        self.assertEqual(sender.sent, [("alice@example.com", "Done.", None)])
        self.assertIsNotNone(dispatched)
        assert dispatched is not None
        self.assertEqual(dispatched.channel, "email")

    def test_dispatch_log_channel_records_normalized_message(self) -> None:
        message = OutboundMessage(channel="log", target="ops", body="Applied.")

        dispatched = IntegratedApplier().dispatch(message)

        self.assertEqual(dispatched, message)

    def test_dispatch_email_reply_keeps_subject_and_quotes_original(self) -> None:
        sender = _RecordingEmailSender(sent=[])
        message = OutboundMessage(
            channel="email",
            target="alice@example.com",
            body="Here is the answer.",
        )
        envelope = _email_envelope(
            subject="Quarterly update",
            body="Can you summarize the key points?",
        )

        IntegratedApplier(email_sender=sender).dispatch(message, envelope=envelope)

        self.assertEqual(len(sender.sent), 1)
        target, body, subject = sender.sent[0]
        self.assertEqual(target, "alice@example.com")
        self.assertEqual(subject, "Re: Quarterly update")
        self.assertIn("Here is the answer.", body)
        self.assertIn("Original message:", body)
        self.assertIn("> Can you summarize the key points?", body)

    def test_dispatch_email_reply_strips_subject_line_from_body(self) -> None:
        sender = _RecordingEmailSender(sent=[])
        message = OutboundMessage(
            channel="email",
            target="alice@example.com",
            body="Subject: Re: Ice-cream\n\nGreat choice. Let's go classic.",
        )
        envelope = _email_envelope(
            subject="Ice-cream",
            body="Yes please, let's focus on classic",
        )

        IntegratedApplier(email_sender=sender).dispatch(message, envelope=envelope)

        _, body, subject = sender.sent[0]
        self.assertEqual(subject, "Re: Ice-cream")
        self.assertFalse(body.lstrip().startswith("Subject:"))
        self.assertIn("Great choice. Let's go classic.", body)

    def test_dispatch_sends_telegram_message(self) -> None:
        sender = _RecordingTelegramSender(sent=[])
        message = OutboundMessage(
            channel="telegram",
            target="12345",
            body="Done via telegram.",
        )

        dispatched = IntegratedApplier(telegram_sender=sender).dispatch(message)

        self.assertEqual(
            sender.sent,
            [
                (
                    "12345",
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        body="Done via telegram.",
                    ),
                )
            ],
        )
        assert dispatched is not None
        self.assertEqual(dispatched.channel, "telegram")

    def test_dispatch_sends_telegram_reaction(self) -> None:
        sender = _RecordingTelegramSender(sent=[])
        message = OutboundMessage(
            channel="telegram_reaction",
            target="12345",
            body="👍",
            metadata={"message_id": 321},
        )

        dispatched = IntegratedApplier(telegram_sender=sender).dispatch(message)

        self.assertEqual(sender.reactions, [("12345", 321, "👍")])
        self.assertEqual(dispatched, message)

    def test_dispatch_sends_telegram_photo_attachment(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingTelegramSender(sent=[])
            image_path = Path(tmpdir) / "menu.png"
            image_path.write_bytes(b"png")
            message = OutboundMessage(
                channel="telegram",
                target="12345",
                body="caption",
                attachment=AttachmentRef(uri=image_path.as_uri(), name="menu.png"),
            )

            dispatched = IntegratedApplier(telegram_sender=sender).dispatch(message)

            self.assertEqual(len(sender.sent), 1)
            self.assertEqual(sender.sent[0][0], "12345")
            self.assertEqual(
                sender.sent[0][1].attachment,
                AttachmentRef(uri=image_path.as_uri(), name="menu.png"),
            )
            assert dispatched is not None
            self.assertEqual(
                dispatched.attachment,
                AttachmentRef(uri=image_path.as_uri(), name="menu.png"),
            )

    def test_dispatch_sends_telegram_document_attachment(self) -> None:
        with TemporaryDirectory() as tmpdir:
            sender = _RecordingTelegramSender(sent=[])
            pdf_path = Path(tmpdir) / "menu.pdf"
            pdf_path.write_bytes(b"%PDF")
            message = OutboundMessage(
                channel="telegram",
                target="12345",
                attachment=AttachmentRef(uri=pdf_path.as_uri(), name="menu.pdf"),
            )

            dispatched = IntegratedApplier(telegram_sender=sender).dispatch(message)

            self.assertEqual(len(sender.sent), 1)
            self.assertEqual(
                sender.sent[0][1].attachment,
                AttachmentRef(uri=pdf_path.as_uri(), name="menu.pdf"),
            )
            assert dispatched is not None
            self.assertIsNone(dispatched.body)

    def test_dispatch_raises_attachment_dispatch_error_for_missing_file(self) -> None:
        sender = TelegramMessageSender(
            bot_token="token", http_post_multipart=lambda *_args: {"ok": True}
        )
        message = OutboundMessage(
            channel="telegram",
            target="12345",
            attachment=AttachmentRef(
                uri="file:///does/not/exist.pdf", name="missing.pdf"
            ),
        )

        with self.assertRaises(MessageDispatchError) as context:
            IntegratedApplier(telegram_sender=sender).dispatch(message)

        self.assertEqual(context.exception.reason_code, "telegram_attachment_missing")

    def test_dispatch_raises_attachment_dispatch_error_on_telegram_api_failure(
        self,
    ) -> None:
        with TemporaryDirectory() as tmpdir:
            pdf_path = Path(tmpdir) / "menu.pdf"
            pdf_path.write_bytes(b"%PDF")
            sender = TelegramMessageSender(
                bot_token="token",
                http_post_multipart=lambda *_args: {"ok": False},
            )
            message = OutboundMessage(
                channel="telegram",
                target="12345",
                body="menu",
                attachment=AttachmentRef(uri=pdf_path.as_uri(), name="menu.pdf"),
            )

            with self.assertRaises(MessageDispatchError) as context:
                IntegratedApplier(telegram_sender=sender).dispatch(message)

            self.assertEqual(
                context.exception.reason_code, "telegram_attachment_send_failed"
            )

    def test_dispatch_returns_none_when_email_sender_not_configured(self) -> None:
        message = OutboundMessage(
            channel="email",
            target="alice@example.com",
            body="Done.",
        )

        self.assertIsNone(IntegratedApplier().dispatch(message))

    def test_dispatch_returns_none_when_telegram_sender_not_configured(self) -> None:
        message = OutboundMessage(channel="telegram", target="12345", body="Done.")

        self.assertIsNone(IntegratedApplier().dispatch(message))

    def test_dispatch_returns_none_when_github_sender_not_configured(self) -> None:
        message = OutboundMessage(
            channel="github",
            target="https://github.com/brokensbone/chatting/issues/12",
            body="Done.",
        )

        self.assertIsNone(IntegratedApplier().dispatch(message))

    def test_dispatch_sends_github_comment(self) -> None:
        sender = _RecordingGitHubSender(sent=[])
        message = OutboundMessage(
            channel="github",
            target="https://github.com/brokensbone/chatting/issues/12",
            body="Done via GitHub.",
        )

        dispatched = IntegratedApplier(github_sender=sender).dispatch(message)

        self.assertEqual(
            sender.sent,
            [
                (
                    "https://github.com/brokensbone/chatting/issues/12",
                    "Done via GitHub.",
                )
            ],
        )
        assert dispatched is not None
        self.assertEqual(dispatched.channel, "github")

    def test_dispatch_raises_github_dispatch_error_on_failure(self) -> None:
        sender = _FailingGitHubSender()
        message = OutboundMessage(
            channel="github",
            target="https://github.com/brokensbone/chatting/issues/12",
            body="one",
        )

        with self.assertRaises(MessageDispatchError) as context:
            IntegratedApplier(github_sender=sender).dispatch(message)

        self.assertEqual(context.exception.reason_code, "github_dispatch_failed")

    def test_dispatch_maps_final_channel_to_envelope_reply_channel_for_telegram(
        self,
    ) -> None:
        sender = _RecordingTelegramSender(sent=[])
        message = OutboundMessage(
            channel="final",
            target="user",
            body="Answer from model.",
        )
        envelope = TaskEnvelope(
            id="telegram:test",
            source="im",
            received_at=datetime(2026, 3, 2, tzinfo=timezone.utc),
            actor="8605042448:edsalkeld",
            content="Question",
            attachments=[],
            context_refs=["repo:/home/edward/chatting"],
            reply_channel=ReplyChannel(type="telegram", target="8605042448"),
            dedupe_key="telegram:test",
        )

        dispatched = IntegratedApplier(telegram_sender=sender).dispatch(
            message, envelope=envelope
        )

        self.assertEqual(
            sender.sent,
            [
                (
                    "8605042448",
                    OutboundMessage(
                        channel="final", target="user", body="Answer from model."
                    ),
                )
            ],
        )
        assert dispatched is not None
        self.assertEqual(dispatched.channel, "telegram")
        self.assertEqual(dispatched.target, "8605042448")

    def test_dispatch_raises_telegram_dispatch_error_on_failure(self) -> None:
        sender = _FailingTelegramSender()
        message = OutboundMessage(channel="telegram", target="12345", body="working")

        with self.assertRaises(MessageDispatchError) as context:
            IntegratedApplier(telegram_sender=sender).dispatch(message)

        self.assertEqual(context.exception.reason_code, "telegram_dispatch_failed")


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

        sender.send(
            "12345", OutboundMessage(channel="telegram", target="12345", body="hello")
        )

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

    def test_send_retries_without_parse_mode_when_telegram_rejects_entities(
        self,
    ) -> None:
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

        sender.send(
            "12345", OutboundMessage(channel="telegram", target="12345", body="hello")
        )

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
            sender.send(
                "12345",
                OutboundMessage(channel="telegram", target="12345", body="hello"),
            )

    def test_send_escapes_markdown_sensitive_text_before_first_send(self) -> None:
        seen_calls: list[tuple[str, dict[str, object], float]] = []

        def fake_http_post_json(
            url: str,
            payload: dict[str, object],
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, timeout))
            return {"ok": True, "result": {"message_id": 5}}

        sender = TelegramMessageSender(
            bot_token="token",
            http_post_json=fake_http_post_json,
        )

        sender.send(
            "12345",
            OutboundMessage(
                channel="telegram",
                target="12345",
                body="Issue [81](x) and _beta_ *now* `code`",
            ),
        )

        self.assertEqual(len(seen_calls), 1)
        self.assertEqual(
            seen_calls[0][1],
            {
                "chat_id": "12345",
                "text": r"Issue \[81\]\(x\) and \_beta\_ \*now\* \`code\`",
                "parse_mode": "Markdown",
            },
        )

    def test_send_normalizes_literal_escape_sequences_before_markdown_escape(
        self,
    ) -> None:
        seen_calls: list[tuple[str, dict[str, object], float]] = []

        def fake_http_post_json(
            url: str,
            payload: dict[str, object],
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, timeout))
            return {"ok": True, "result": {"message_id": 6}}

        sender = TelegramMessageSender(
            bot_token="token",
            http_post_json=fake_http_post_json,
        )

        sender.send(
            "12345",
            OutboundMessage(
                channel="telegram",
                target="12345",
                body=r"Your three 1960s saved albums are all from 1969:\n\n- Gal Costa \(second edition\)",
            ),
        )

        self.assertEqual(len(seen_calls), 1)
        self.assertEqual(
            seen_calls[0][1],
            {
                "chat_id": "12345",
                "text": "Your three 1960s saved albums are all from 1969:\n\n- Gal Costa \\(second edition\\)",
                "parse_mode": "Markdown",
            },
        )

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
        self.assertEqual(
            seen_calls[0][0], "https://api.telegram.org/bottoken/sendDocument"
        )
        self.assertEqual(seen_calls[0][1]["parse_mode"], "Markdown")
        self.assertNotIn("parse_mode", seen_calls[1][1])
        self.assertEqual(seen_calls[0][2], "document")
        self.assertEqual(seen_calls[1][1]["caption"], "**menu**")

    def test_send_retries_with_normalized_plain_text_after_parse_error(self) -> None:
        seen_calls: list[tuple[str, dict[str, object], float]] = []

        def fake_http_post_json(
            url: str,
            payload: dict[str, object],
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, timeout))
            if len(seen_calls) == 1:
                return {"ok": False, "description": "Bad Request: can't parse entities"}
            return {"ok": True, "result": {"message_id": 7}}

        sender = TelegramMessageSender(
            bot_token="token",
            http_post_json=fake_http_post_json,
        )

        sender.send(
            "12345",
            OutboundMessage(
                channel="telegram",
                target="12345",
                body=r"Heading\n\n- Item \(note\)",
            ),
        )

        self.assertEqual(len(seen_calls), 2)
        self.assertEqual(
            seen_calls[1][1],
            {"chat_id": "12345", "text": "Heading\n\n- Item (note)"},
        )

    def test_send_normalizes_double_escaped_sequences_before_markdown_escape(
        self,
    ) -> None:
        seen_calls: list[tuple[str, dict[str, object], float]] = []

        def fake_http_post_json(
            url: str,
            payload: dict[str, object],
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, timeout))
            return {"ok": True, "result": {"message_id": 8}}

        sender = TelegramMessageSender(
            bot_token="token",
            http_post_json=fake_http_post_json,
        )

        sender.send(
            "12345",
            OutboundMessage(
                channel="telegram",
                target="12345",
                body=r"Your three 1960s saved albums are all from 1969:\\n\\n- Gal Costa \\(second edition\\)",
            ),
        )

        self.assertEqual(len(seen_calls), 1)
        self.assertEqual(
            seen_calls[0][1],
            {
                "chat_id": "12345",
                "text": "Your three 1960s saved albums are all from 1969:\n\n- Gal Costa \\(second edition\\)",
                "parse_mode": "Markdown",
            },
        )

    def test_send_attachment_raises_when_telegram_response_not_ok(self) -> None:
        with TemporaryDirectory() as tmpdir:
            pdf_path = Path(tmpdir) / "menu.pdf"
            pdf_path.write_bytes(b"%PDF")
            sender = TelegramMessageSender(
                bot_token="token",
                http_post_multipart=lambda *_args: {"ok": False},
            )

            with self.assertRaisesRegex(
                RuntimeError, "telegram_attachment_send_failed"
            ):
                sender.send(
                    "12345",
                    OutboundMessage(
                        channel="telegram",
                        target="12345",
                        attachment=AttachmentRef(
                            uri=pdf_path.as_uri(), name="menu.pdf"
                        ),
                    ),
                )

    def test_default_http_post_json_returns_error_payload_and_logs_http_error_body(
        self,
    ) -> None:
        http_error = urllib.error.HTTPError(
            url="https://api.telegram.org/bottoken/sendMessage",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=io.BytesIO(b'{"ok":false,"description":"Bad Request: chat not found"}'),
        )

        with (
            patch(
                "app.handler.applier.integrated.urllib.request.urlopen",
                side_effect=http_error,
            ),
            self.assertLogs(
                "app.handler.applier.integrated", level="ERROR"
            ) as captured,
        ):
            response = _default_http_post_json(
                "https://api.telegram.org/bottoken/sendMessage",
                {"chat_id": "12345", "text": "hello"},
                10.0,
            )

        self.assertEqual(
            response,
            {"ok": False, "description": "Bad Request: chat not found"},
        )
        self.assertEqual(len(captured.records), 1)
        self.assertIn("status=400", captured.output[0])
        self.assertIn("Bad Request", captured.output[0])
        self.assertIn("chat not found", captured.output[0])

    def test_react_posts_set_message_reaction_request(self) -> None:
        seen_calls: list[tuple[str, dict[str, object], float]] = []

        def fake_http_post_json(
            url: str,
            payload: dict[str, object],
            timeout: float,
        ) -> dict[str, object]:
            seen_calls.append((url, payload, timeout))
            return {"ok": True, "result": True}

        sender = TelegramMessageSender(
            bot_token="token",
            http_post_json=fake_http_post_json,
        )

        sender.react("12345", 99, "👍")

        self.assertEqual(
            seen_calls,
            [
                (
                    "https://api.telegram.org/bottoken/setMessageReaction",
                    {
                        "chat_id": "12345",
                        "message_id": 99,
                        "reaction": [{"type": "emoji", "emoji": "👍"}],
                    },
                    10.0,
                )
            ],
        )


class GitHubIssueCommentSenderTests(unittest.TestCase):
    def test_send_uses_gh_cli_for_issue_url_target(self) -> None:
        seen_commands: list[list[str]] = []

        class _Result:
            returncode = 0

        def runner(command: list[str]) -> object:
            seen_commands.append(command)
            return _Result()

        sender = GitHubIssueCommentSender(command_runner=runner)
        sender.send("https://github.com/brokensbone/chatting/issues/12", "hello")

        self.assertEqual(
            seen_commands,
            [
                [
                    "gh",
                    "issue",
                    "comment",
                    "12",
                    "--repo",
                    "brokensbone/chatting",
                    "--body",
                    "hello",
                ]
            ],
        )

    def test_send_uses_gh_cli_for_slug_target(self) -> None:
        seen_commands: list[list[str]] = []

        class _Result:
            returncode = 0

        def runner(command: list[str]) -> object:
            seen_commands.append(command)
            return _Result()

        sender = GitHubIssueCommentSender(command_runner=runner)
        sender.send("brokensbone/chatting#13", "hello")

        self.assertEqual(
            seen_commands,
            [
                [
                    "gh",
                    "issue",
                    "comment",
                    "13",
                    "--repo",
                    "brokensbone/chatting",
                    "--body",
                    "hello",
                ]
            ],
        )

    def test_send_uses_gh_cli_for_pull_request_url_target(self) -> None:
        seen_commands: list[list[str]] = []

        class _Result:
            returncode = 0

        def runner(command: list[str]) -> object:
            seen_commands.append(command)
            return _Result()

        sender = GitHubIssueCommentSender(command_runner=runner)
        sender.send("https://github.com/brokensbone/chatting/pull/60", "hello")

        self.assertEqual(
            seen_commands,
            [
                [
                    "gh",
                    "issue",
                    "comment",
                    "60",
                    "--repo",
                    "brokensbone/chatting",
                    "--body",
                    "hello",
                ]
            ],
        )

    def test_send_raises_for_invalid_target(self) -> None:
        sender = GitHubIssueCommentSender(command_runner=lambda _command: object())
        with self.assertRaisesRegex(ValueError, "github_issue_target_invalid"):
            sender.send("not-a-github-target", "hello")

    def test_send_raises_when_gh_cli_fails(self) -> None:
        class _Result:
            returncode = 1

        sender = GitHubIssueCommentSender(command_runner=lambda _command: _Result())
        with self.assertRaisesRegex(RuntimeError, "github_issue_comment_failed"):
            sender.send("brokensbone/chatting#13", "hello")


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
    reactions: list[tuple[str, int, str]] = field(default_factory=list)

    def send(self, target: str, message: OutboundMessage) -> None:
        self.sent.append((target, message))

    def react(self, target: str, message_id: int, emoji: str) -> None:
        self.reactions.append((target, message_id, emoji))


class _FailingTelegramSender:
    def send(self, target: str, message: OutboundMessage) -> None:
        del target, message
        raise RuntimeError("simulated dispatch failure")


@dataclass
class _RecordingGitHubSender:
    sent: list[tuple[str, str]]

    def send(self, target: str, body: str) -> None:
        self.sent.append((target, body))


class _FailingGitHubSender:
    def send(self, target: str, body: str) -> None:
        del target, body
        raise RuntimeError("simulated dispatch failure")


def _email_envelope(*, subject: str, body: str):
    return TaskEnvelope(
        id="email:test",
        source="email",
        received_at=datetime(2026, 3, 2, tzinfo=timezone.utc),
        actor="alice@example.com",
        content=f"Subject: {subject}\n\n{body}",
        attachments=[],
        context_refs=["repo:/home/edward/chatting"],
        reply_channel=ReplyChannel(type="email", target="alice@example.com"),
        dedupe_key="email:test",
    )


if __name__ == "__main__":
    unittest.main()
