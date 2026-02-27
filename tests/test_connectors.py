import unittest
from datetime import datetime, timezone

from app.connectors.fake_cron_connector import CronTrigger, FakeCronConnector
from app.connectors.fake_email_connector import EmailMessage, FakeEmailConnector


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


if __name__ == "__main__":
    unittest.main()
