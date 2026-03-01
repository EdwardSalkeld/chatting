import unittest
from datetime import datetime, timezone
from email.message import EmailMessage as ParsedEmailMessage

from app.connectors.fake_cron_connector import CronTrigger, FakeCronConnector
from app.connectors.fake_email_connector import EmailMessage, FakeEmailConnector
from app.connectors.imap_email_connector import ImapEmailConnector
from app.connectors.interval_schedule_connector import (
    IntervalScheduleConnector,
    IntervalScheduleJob,
)
from app.connectors.telegram_connector import (
    TelegramConnector,
    TelegramGetUpdatesResponse,
)
from app.connectors.slack_connector import SlackConnector
from app.connectors.webhook_connector import WebhookConnector, WebhookEvent


class FakeCronConnectorTests(unittest.TestCase):
    def test_poll_normalizes_cron_trigger_to_envelope(self) -> None:
        connector = FakeCronConnector(
            triggers=[
                CronTrigger(
                    job_name="daily-summary",
                    content="Generate daily summary",
                    scheduled_for=datetime(2026, 2, 27, 9, 0, tzinfo=timezone.utc),
                    context_refs=["repo:/home/edward/chatting"],
                )
            ]
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertEqual(envelope.source, "cron")
        self.assertEqual(envelope.actor, None)
        self.assertEqual(envelope.reply_channel.type, "log")
        self.assertEqual(envelope.reply_channel.target, "daily-summary")
        self.assertEqual(envelope.dedupe_key, "cron:daily-summary:2026-02-27T09:00:00+00:00")
        self.assertEqual(envelope.context_refs, ["repo:/home/edward/chatting"])

    def test_poll_rejects_naive_scheduled_time(self) -> None:
        connector = FakeCronConnector(
            triggers=[
                CronTrigger(
                    job_name="daily-summary",
                    content="Generate daily summary",
                    scheduled_for=datetime(2026, 2, 27, 9, 0),
                    context_refs=[],
                )
            ]
        )

        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            connector.poll()


class FakeEmailConnectorTests(unittest.TestCase):
    def test_poll_normalizes_email_to_envelope(self) -> None:
        connector = FakeEmailConnector(
            messages=[
                EmailMessage(
                    provider_message_id="provider-123",
                    from_address="alice@example.com",
                    subject="Please summarize",
                    body="Can you summarize yesterday's logs?",
                    received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
                    context_refs=["repo:/home/edward/chatting"],
                )
            ]
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertEqual(envelope.source, "email")
        self.assertEqual(envelope.actor, "alice@example.com")
        self.assertEqual(envelope.reply_channel.type, "email")
        self.assertEqual(envelope.reply_channel.target, "alice@example.com")
        self.assertEqual(envelope.dedupe_key, "email:provider-123")
        self.assertEqual(
            envelope.content,
            "Subject: Please summarize\n\nCan you summarize yesterday's logs?",
        )

    def test_poll_rejects_naive_received_time(self) -> None:
        connector = FakeEmailConnector(
            messages=[
                EmailMessage(
                    provider_message_id="provider-123",
                    from_address="alice@example.com",
                    subject="Please summarize",
                    body="Can you summarize yesterday's logs?",
                    received_at=datetime(2026, 2, 27, 16, 0),
                    context_refs=[],
                )
            ]
        )

        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            connector.poll()


class IntervalScheduleConnectorTests(unittest.TestCase):
    def test_poll_emits_due_job_and_respects_interval(self) -> None:
        clock = _MutableClock(datetime(2026, 2, 28, 10, 0, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="heartbeat",
                    content="Run scheduled heartbeat",
                    interval_seconds=60,
                    start_at=datetime(2026, 2, 28, 10, 0, tzinfo=timezone.utc),
                    context_refs=["repo:/home/edward/chatting"],
                )
            ],
            now_provider=clock.now,
        )

        first_poll = connector.poll()
        self.assertEqual(len(first_poll), 1)
        self.assertEqual(
            first_poll[0].dedupe_key,
            "cron:heartbeat:2026-02-28T10:00:00+00:00",
        )

        self.assertEqual(connector.poll(), [])

        clock.set(datetime(2026, 2, 28, 10, 1, tzinfo=timezone.utc))
        second_poll = connector.poll()
        self.assertEqual(len(second_poll), 1)
        self.assertEqual(
            second_poll[0].dedupe_key,
            "cron:heartbeat:2026-02-28T10:01:00+00:00",
        )

    def test_job_rejects_naive_start_at(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            IntervalScheduleJob(
                job_name="daily",
                content="Run",
                interval_seconds=30,
                start_at=datetime(2026, 2, 28, 10, 0),
                context_refs=[],
            )


class ImapEmailConnectorTests(unittest.TestCase):
    def test_poll_normalizes_imap_messages_to_envelopes(self) -> None:
        raw_message = _build_raw_email(
            sender="alice@example.com",
            subject="Please summarize",
            body="Summarize this inbox thread.",
            date_value="Sat, 28 Feb 2026 10:15:00 +0000",
        )
        fake_client = _FakeImapClient({b"101": raw_message})
        connector = ImapEmailConnector(
            host="imap.example.com",
            username="bot@example.com",
            password="secret",
            imap_client_factory=lambda _host, _port: fake_client,
            context_refs=["repo:/home/edward/chatting"],
            now_provider=lambda: datetime(2026, 2, 28, 10, 30, tzinfo=timezone.utc),
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertEqual(envelope.source, "email")
        self.assertEqual(envelope.actor, "alice@example.com")
        self.assertEqual(envelope.reply_channel.target, "alice@example.com")
        self.assertEqual(envelope.dedupe_key, "email:101")
        self.assertEqual(
            envelope.content,
            "Subject: Please summarize\n\nSummarize this inbox thread.",
        )
        self.assertEqual(
            envelope.received_at,
            datetime(2026, 2, 28, 10, 15, tzinfo=timezone.utc),
        )

    def test_poll_falls_back_to_now_for_invalid_date_header(self) -> None:
        raw_message = _build_raw_email(
            sender="alice@example.com",
            subject="No Date",
            body="Body",
            date_value="not-a-date",
        )
        fake_client = _FakeImapClient({b"201": raw_message})
        fallback_now = datetime(2026, 2, 28, 11, 0, tzinfo=timezone.utc)
        connector = ImapEmailConnector(
            host="imap.example.com",
            username="bot@example.com",
            password="secret",
            imap_client_factory=lambda _host, _port: fake_client,
            now_provider=lambda: fallback_now,
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        self.assertEqual(envelopes[0].received_at, fallback_now)


class TelegramConnectorTests(unittest.TestCase):
    def test_poll_normalizes_supported_updates_and_advances_offset(self) -> None:
        responses = [
            TelegramGetUpdatesResponse(
                ok=True,
                result=[
                    {
                        "update_id": 1001,
                        "message": {
                            "message_id": 1,
                            "date": 1772272800,
                            "text": "hello from telegram",
                            "chat": {"id": 12345},
                            "from": {"id": 77, "username": "alice"},
                        },
                    },
                    {
                        "update_id": 1002,
                        "channel_post": {"message_id": 2},
                    },
                ],
            ),
            TelegramGetUpdatesResponse(ok=True, result=[]),
        ]
        seen_urls: list[str] = []

        def fake_http_get_json(url: str, _timeout: float) -> TelegramGetUpdatesResponse:
            seen_urls.append(url)
            return responses.pop(0)

        connector = TelegramConnector(
            bot_token="token",
            api_base_url="https://api.telegram.org",
            context_refs=["repo:/home/edward/chatting"],
            http_get_json=fake_http_get_json,
        )

        first_poll = connector.poll()
        second_poll = connector.poll()

        self.assertEqual(len(first_poll), 1)
        envelope = first_poll[0]
        self.assertEqual(envelope.source, "im")
        self.assertEqual(envelope.id, "telegram:1001")
        self.assertEqual(envelope.dedupe_key, "telegram:1001")
        self.assertEqual(envelope.actor, "77:alice")
        self.assertEqual(envelope.reply_channel.type, "telegram")
        self.assertEqual(envelope.reply_channel.target, "12345")
        self.assertEqual(envelope.content, "hello from telegram")
        self.assertEqual(envelope.context_refs, ["repo:/home/edward/chatting"])
        self.assertEqual(second_poll, [])
        self.assertIn("timeout=20", seen_urls[0])
        self.assertIn("offset=1003", seen_urls[1])

    def test_poll_respects_allowed_chat_ids_and_skips_unsupported_payloads(self) -> None:
        connector = TelegramConnector(
            bot_token="token",
            allowed_chat_ids=["12345"],
            http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                ok=True,
                result=[
                    {
                        "update_id": 2001,
                        "message": {
                            "message_id": 1,
                            "date": 1772272800,
                            "text": "allowed",
                            "chat": {"id": 12345},
                        },
                    },
                    {
                        "update_id": 2002,
                        "message": {
                            "message_id": 2,
                            "date": 1772272801,
                            "text": "blocked",
                            "chat": {"id": 67890},
                        },
                    },
                    {
                        "update_id": 2003,
                        "message": {
                            "message_id": 3,
                            "date": 1772272802,
                            "text": "   ",
                            "chat": {"id": 12345},
                        },
                    },
                ],
            ),
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        self.assertEqual(envelopes[0].id, "telegram:2001")

    def test_poll_raises_when_telegram_returns_not_ok(self) -> None:
        connector = TelegramConnector(
            bot_token="token",
            http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                ok=False,
                result=[],
            ),
        )

        with self.assertRaisesRegex(RuntimeError, "telegram_get_updates_failed"):
            connector.poll()


class SlackConnectorTests(unittest.TestCase):
    def test_poll_normalizes_messages_to_im_envelopes(self) -> None:
        connector = SlackConnector(
            fetch_messages=lambda: [
                {
                    "id": "m-1",
                    "user": "U123",
                    "channel": "C999",
                    "text": "Ship it",
                    "ts": "1772272800.100",
                }
            ],
            context_refs=["repo:/home/edward/chatting"],
            allowed_channel_ids=["C999"],
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertEqual(envelope.source, "im")
        self.assertEqual(envelope.id, "slack:m-1")
        self.assertEqual(envelope.actor, "U123")
        self.assertEqual(envelope.reply_channel.type, "slack")
        self.assertEqual(envelope.reply_channel.target, "C999")
        self.assertEqual(envelope.dedupe_key, "slack:m-1")

    def test_poll_skips_disallowed_channels_and_invalid_payloads(self) -> None:
        connector = SlackConnector(
            fetch_messages=lambda: [
                {"id": "m-1", "user": "U123", "channel": "C111", "text": "Hi"},
                {"id": "m-2", "user": "U123", "channel": "C999", "text": "   "},
                {"id": "m-3", "user": "U123", "channel": "C999", "text": "ok"},
            ],
            allowed_channel_ids=["C999"],
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        self.assertEqual(envelopes[0].id, "slack:m-3")


class WebhookConnectorTests(unittest.TestCase):
    def test_poll_drains_enqueued_webhook_events(self) -> None:
        connector = WebhookConnector()
        connector.enqueue(
            WebhookEvent(
                event_id="evt-1",
                actor="svc:deploy",
                content="Deploy finished",
                received_at=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
                reply_target="https://example.com/reply",
                context_refs=["repo:/home/edward/chatting"],
            )
        )

        first = connector.poll()
        second = connector.poll()

        self.assertEqual(len(first), 1)
        self.assertEqual(first[0].source, "webhook")
        self.assertEqual(first[0].reply_channel.type, "webhook")
        self.assertEqual(first[0].reply_channel.target, "https://example.com/reply")
        self.assertEqual(first[0].dedupe_key, "webhook:evt-1")
        self.assertEqual(second, [])

    def test_poll_rejects_naive_received_time(self) -> None:
        connector = WebhookConnector(
            events=[
                WebhookEvent(
                    event_id="evt-1",
                    actor="svc:deploy",
                    content="Deploy finished",
                    received_at=datetime(2026, 3, 1, 12, 0),
                    reply_target="https://example.com/reply",
                    context_refs=[],
                )
            ]
        )

        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            connector.poll()


class _MutableClock:
    def __init__(self, current: datetime) -> None:
        self._current = current

    def now(self) -> datetime:
        return self._current

    def set(self, value: datetime) -> None:
        self._current = value


class _FakeImapClient:
    def __init__(self, raw_messages_by_uid: dict[bytes, bytes]) -> None:
        self._raw_messages_by_uid = raw_messages_by_uid

    def login(self, _username: str, _password: str):
        return "OK", [b"logged-in"]

    def select(self, _mailbox: str):
        return "OK", [b"1"]

    def search(self, _charset, _criterion: str):
        joined = b" ".join(sorted(self._raw_messages_by_uid.keys()))
        return "OK", [joined]

    def fetch(self, uid: bytes, _payload: str):
        raw_message = self._raw_messages_by_uid[uid]
        return "OK", [(b"RFC822", raw_message)]

    def logout(self):
        return "BYE", [b"logged-out"]


def _build_raw_email(*, sender: str, subject: str, body: str, date_value: str) -> bytes:
    parsed = ParsedEmailMessage()
    parsed["From"] = sender
    parsed["To"] = "bot@example.com"
    parsed["Subject"] = subject
    parsed["Date"] = date_value
    parsed.set_content(body)
    return parsed.as_bytes()


if __name__ == "__main__":
    unittest.main()
