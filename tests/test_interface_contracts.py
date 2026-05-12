import tempfile
import unittest
from datetime import datetime, timezone

from app.handler.connectors import (
    AuxiliaryIngressConnector,
    Connector,
    GitHubIssueAssignmentConnector,
    GitHubPullRequestReviewConnector,
    InternalHeartbeatConnector,
    TelegramConnector,
)
from app.handler.github_ingress import GitHubAssignmentCheckpointStore
from app.handler.connectors.telegram_connector import TelegramGetUpdatesResponse
from app.worker.executor import CodexExecutor, Executor
from tests.fixtures import (
    CronTrigger,
    EmailMessage,
    FakeCronConnector,
    FakeEmailConnector,
)


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
            InternalHeartbeatConnector(
                now_provider=lambda: datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc),
            ),
            Connector,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertIsInstance(
                GitHubIssueAssignmentConnector(
                    repository_patterns=["brokensbone/chatting"],
                    assignee_login="BillyAcachofa",
                    context_refs=[],
                    checkpoint_store=GitHubAssignmentCheckpointStore(
                        f"{tmpdir}/state.db"
                    ),
                    graphql_runner=lambda _query, _variables: {
                        "data": {"repository": None}
                    },
                ),
                Connector,
            )
            self.assertIsInstance(
                GitHubPullRequestReviewConnector(
                    repository_patterns=["brokensbone/chatting"],
                    author_login="BillyAcachofa",
                    context_refs=[],
                    checkpoint_store=GitHubAssignmentCheckpointStore(
                        f"{tmpdir}/state.db"
                    ),
                    graphql_runner=lambda _query, _variables: {
                        "data": {"repository": None}
                    },
                ),
                Connector,
            )
        self.assertIsInstance(
            TelegramConnector(
                bot_token="token",
                http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                    ok=True, result=[]
                ),
            ),
            Connector,
        )
        class _AuxiliaryIngressBroker:
            def pickup_json(
                self, queue_name: str, timeout_seconds: int, wait_seconds: int
            ):
                del queue_name, timeout_seconds, wait_seconds
                return None

            def ack(self, queue_name: str, guid: str) -> None:
                del queue_name, guid

        self.assertIsInstance(
            AuxiliaryIngressConnector(
                broker=_AuxiliaryIngressBroker(),  # type: ignore[arg-type]
                queue_name="chatting.ingress.github.generic-post.v1",
            ),
            Connector,
        )

    def test_executor_implementations_match_protocol(self) -> None:
        self.assertIsInstance(CodexExecutor(), Executor)


if __name__ == "__main__":
    unittest.main()
