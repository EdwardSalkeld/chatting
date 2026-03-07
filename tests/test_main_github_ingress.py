import json
import tempfile
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.github_ingress_runtime import GitHubIssueAssignmentEvent
from app.main_message_handler import _load_config, main


@dataclass
class _FakeBroker:
    ensured: list[str] = field(default_factory=list)
    published: list[tuple[str, dict[str, object]]] = field(default_factory=list)
    acked: list[tuple[str, str]] = field(default_factory=list)

    def ensure_queue(self, queue_name: str) -> None:
        self.ensured.append(queue_name)

    def publish_json(self, queue_name: str, payload: dict[str, object]) -> None:
        self.published.append((queue_name, payload))

    def pickup_json(self, queue_name: str, timeout_seconds: int, wait_seconds: int):
        del queue_name, timeout_seconds, wait_seconds
        return None

    def ack(self, queue_name: str, guid: str) -> None:
        self.acked.append((queue_name, guid))


class MainGitHubIngressTests(unittest.TestCase):
    def test_load_config_rejects_unknown_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "message-handler.json"
            config_path.write_text(json.dumps({"unexpected": True}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unknown keys"):
                _load_config(str(config_path))

    def test_main_message_handler_publishes_assignment_event_once_and_uses_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            broker = _FakeBroker()
            event = GitHubIssueAssignmentEvent(
                event_id="AE_1",
                event_created_at=datetime(2026, 3, 7, 10, 47, 35, tzinfo=timezone.utc),
                repository_id="R_1",
                repository_name_with_owner="brokensbone/chatting",
                issue_id="I_1",
                issue_number=12,
                issue_title="Plan milestone 5",
                issue_body="Body",
                issue_url="https://github.com/brokensbone/chatting/issues/12",
                assignee_login="BillyAcachofa",
                actor_login="edward",
                labels=["enhancement"],
            )

            with (
                patch(
                    "app.main_message_handler.BBMBQueueAdapter",
                    return_value=broker,
                ),
                patch(
                    "app.main_message_handler.fetch_assignment_events_for_repository",
                    side_effect=[[event], [event]],
                ),
                patch("app.main_message_handler._build_live_connectors", return_value=[]),
                patch(
                    "sys.argv",
                    [
                        "main_message_handler.py",
                        "--db-path",
                        db_path,
                        "--bbmb-address",
                        "127.0.0.1:9876",
                        "--github-repository",
                        "brokensbone/chatting",
                        "--github-assignee-login",
                        "BillyAcachofa",
                        "--github-reply-channel-type",
                        "telegram",
                        "--github-reply-channel-target",
                        "8605042448",
                        "--max-loops",
                        "2",
                        "--poll-interval-seconds",
                        "0.01",
                    ],
                ),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            self.assertEqual(broker.ensured, ["chatting.tasks.v1", "chatting.egress.v1"])
            self.assertEqual(len(broker.published), 1)
            self.assertEqual(broker.published[0][0], "chatting.tasks.v1")
            self.assertEqual(
                broker.published[0][1]["task_id"],
                "task:github-assignment:brokensbone/chatting:12:AE_1",
            )


if __name__ == "__main__":
    unittest.main()
