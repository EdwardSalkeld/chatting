import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.applier import Applier, IntegratedApplier, NoOpApplier
from app.connectors import (
    Connector,
    CronTrigger,
    EmailMessage,
    FakeCronConnector,
    FakeEmailConnector,
    SlackConnector,
    TelegramConnector,
    WebhookConnector,
    WebhookEvent,
)
from app.connectors.telegram_connector import TelegramGetUpdatesResponse
from app.executor import CodexExecutor, Executor, StubExecutor
from app.policy import AllowlistPolicyEngine, PolicyEngine
from app.router import Router, RuleBasedRouter
from app.state import SQLiteStateStore, StateStore


class InterfaceContractTests(unittest.TestCase):
    def test_connector_implementations_match_protocol(self) -> None:
        cron = FakeCronConnector(
            triggers=[
                CronTrigger(
                    job_name="daily-summary",
                    content="Generate summary",
                    scheduled_for=datetime(2026, 2, 27, 9, 0, tzinfo=timezone.utc),
                    context_refs=[],
                )
            ]
        )
        email = FakeEmailConnector(
            messages=[
                EmailMessage(
                    provider_message_id="provider-1",
                    from_address="alice@example.com",
                    subject="Hi",
                    body="Hello",
                    received_at=datetime(2026, 2, 27, 16, 0, tzinfo=timezone.utc),
                    context_refs=[],
                )
            ]
        )

        self.assertIsInstance(cron, Connector)
        self.assertIsInstance(email, Connector)
        self.assertIsInstance(
            SlackConnector(fetch_messages=lambda: []),
            Connector,
        )
        self.assertIsInstance(
            TelegramConnector(
                bot_token="token",
                http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(ok=True, result=[]),
            ),
            Connector,
        )
        self.assertIsInstance(
            WebhookConnector(
                events=[
                    WebhookEvent(
                        event_id="evt-1",
                        actor="svc:test",
                        content="hello",
                        received_at=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
                        reply_target="https://example.com",
                        context_refs=[],
                    )
                ]
            ),
            Connector,
        )

    def test_router_implementation_matches_protocol(self) -> None:
        self.assertIsInstance(RuleBasedRouter(), Router)

    def test_executor_implementations_match_protocol(self) -> None:
        self.assertIsInstance(StubExecutor(), Executor)
        self.assertIsInstance(CodexExecutor(), Executor)

    def test_policy_implementation_matches_protocol(self) -> None:
        self.assertIsInstance(AllowlistPolicyEngine(), PolicyEngine)

    def test_applier_implementation_matches_protocol(self) -> None:
        self.assertIsInstance(NoOpApplier(), Applier)
        self.assertIsInstance(IntegratedApplier(base_dir="."), Applier)

    def test_state_store_implementation_matches_protocol(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            self.assertIsInstance(store, StateStore)


if __name__ == "__main__":
    unittest.main()
