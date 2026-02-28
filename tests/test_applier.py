import unittest
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from tempfile import TemporaryDirectory

from app.applier import IntegratedApplier, NoOpApplier, SmtpEmailSender
from app.models import (
    ActionProposal,
    ConfigUpdateDecision,
    OutboundMessage,
    PolicyDecision,
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
    sent: list[tuple[str, str]]

    def send(self, target: str, body: str) -> None:
        self.sent.append((target, body))


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
                [("alice@example.com", "Done.")],
            )
            self.assertEqual(
                [message.channel for message in result.dispatched_messages],
                ["email", "log"],
            )
            self.assertEqual(result.reason_codes, [])

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


if __name__ == "__main__":
    unittest.main()
