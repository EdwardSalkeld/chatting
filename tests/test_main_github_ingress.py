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


@dataclass
class _FakeMetricsServer:
    shutdown_calls: int = 0

    def shutdown(self) -> None:
        self.shutdown_calls += 1


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
                    "app.main_message_handler._start_metrics_server",
                    return_value=_FakeMetricsServer(),
                ),
                patch(
                    "app.connectors.github_issue_assignment_connector.fetch_assignment_events_for_repository",
                    side_effect=[[event], [event]],
                ),
                patch(
                    "app.connectors.github_issue_assignment_connector.expand_repository_patterns",
                    return_value=["brokensbone/chatting"],
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
            self.assertEqual(
                broker.published[0][1]["envelope"]["reply_channel"],
                {
                    "type": "telegram",
                    "target": "8605042448",
                },
            )

    def test_main_message_handler_resolves_assignee_from_authenticated_gh_user(self) -> None:
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
                    "app.main_message_handler._start_metrics_server",
                    return_value=_FakeMetricsServer(),
                ),
                patch(
                    "app.main_message_handler.fetch_authenticated_viewer_login",
                    return_value="BillyAcachofa",
                ),
                patch(
                    "app.connectors.github_issue_assignment_connector.expand_repository_patterns",
                    return_value=["brokensbone/chatting"],
                ),
                patch(
                    "app.connectors.github_issue_assignment_connector.fetch_assignment_events_for_repository",
                    return_value=[event],
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
                        "brokensbone/*",
                        "--github-reply-channel-type",
                        "telegram",
                        "--github-reply-channel-target",
                        "8605042448",
                        "--max-loops",
                        "1",
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

    def test_main_message_handler_disables_misconfigured_github_ingress_and_keeps_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            broker = _FakeBroker()

            with (
                patch(
                    "app.main_message_handler.BBMBQueueAdapter",
                    return_value=broker,
                ),
                patch(
                    "app.main_message_handler._start_metrics_server",
                    return_value=_FakeMetricsServer(),
                ),
                patch(
                    "app.main_message_handler.LOGGER.error",
                ) as error_logger,
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
            self.assertEqual(len(broker.published), 0)
            disabled_log_calls = [
                call
                for call in error_logger.call_args_list
                if call.args and call.args[0] == "ingress_connector_disabled connector=%s loop=%s error=%s"
            ]
            self.assertEqual(len(disabled_log_calls), 2)
            self.assertTrue(all(call.args[1] == "github" for call in disabled_log_calls))

    def test_main_message_handler_continues_with_healthy_connectors_when_imap_misconfigured(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "heartbeat",
                            "content": "daily check",
                            "interval_seconds": 1,
                            "reply_channel_type": "log",
                            "reply_channel_target": "ops",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            broker = _FakeBroker()

            with (
                patch(
                    "app.main_message_handler.BBMBQueueAdapter",
                    return_value=broker,
                ),
                patch(
                    "app.main_message_handler._start_metrics_server",
                    return_value=_FakeMetricsServer(),
                ),
                patch(
                    "app.main_message_handler.LOGGER.error",
                ) as error_logger,
                patch(
                    "sys.argv",
                    [
                        "main_message_handler.py",
                        "--db-path",
                        db_path,
                        "--bbmb-address",
                        "127.0.0.1:9876",
                        "--schedule-file",
                        str(schedule_path),
                        "--imap-host",
                        "imap.example.com",
                        "--max-loops",
                        "1",
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
            disabled_log_calls = [
                call
                for call in error_logger.call_args_list
                if call.args and call.args[0] == "ingress_connector_disabled connector=%s loop=%s error=%s"
            ]
            self.assertEqual(len(disabled_log_calls), 1)
            self.assertEqual(disabled_log_calls[0].args[1], "imap")

    def test_main_message_handler_starts_and_stops_metrics_server_with_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            broker = _FakeBroker()
            metrics_server = _FakeMetricsServer()

            with (
                patch(
                    "app.main_message_handler.BBMBQueueAdapter",
                    return_value=broker,
                ),
                patch(
                    "app.main_message_handler._build_live_connectors",
                    return_value=[],
                ),
                patch(
                    "app.main_message_handler._start_metrics_server",
                    return_value=metrics_server,
                ) as start_metrics_server,
                patch(
                    "sys.argv",
                    [
                        "main_message_handler.py",
                        "--db-path",
                        db_path,
                        "--bbmb-address",
                        "127.0.0.1:9876",
                        "--max-loops",
                        "1",
                        "--poll-interval-seconds",
                        "0.01",
                    ],
                ),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            self.assertEqual(metrics_server.shutdown_calls, 1)
            _, kwargs = start_metrics_server.call_args
            self.assertEqual(kwargs["host"], "127.0.0.1")
            self.assertEqual(kwargs["port"], 9464)

if __name__ == "__main__":
    unittest.main()
