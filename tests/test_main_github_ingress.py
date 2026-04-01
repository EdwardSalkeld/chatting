import argparse
import json
import tempfile
import unittest
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.handler.github_ingress import (
    GitHubIssueAssignmentEvent,
    GitHubPullRequestReviewEvent,
)
from app.broker import EgressQueueMessage, TaskQueueMessage
from app.handler.connectors.auxiliary_ingress_connector import AuxiliaryIngressConnector
from app.main_message_handler import (
    _build_live_connectors,
    _load_config,
    _load_schedule_jobs,
    main,
)
from app.models import OutboundMessage, ReplyChannel, TaskEnvelope


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
    def _non_heartbeat_published(
        self, broker: _FakeBroker
    ) -> list[tuple[str, dict[str, object]]]:
        return [
            item
            for item in broker.published
            if item[1].get("envelope", {}).get("source") != "internal"
        ]

    def test_load_config_rejects_unknown_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "message-handler.json"
            config_path.write_text(json.dumps({"unexpected": True}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unknown keys"):
                _load_config(str(config_path))

    def test_build_live_connectors_supports_auxiliary_ingress(self) -> None:
        config = {
            "bbmb_address": "127.0.0.1:9876",
            "auxiliary_ingress_enabled": True,
            "auxiliary_ingress_routes": ["generic-post:12334", "new-service:/two"],
            "auxiliary_ingress_context_refs": ["repo:/workspace/chatting"],
        }
        broker = _FakeBroker()

        with patch("app.main_message_handler.BBMBQueueAdapter", return_value=broker):
            connectors = _build_live_connectors(
                argparse.Namespace(
                    bbmb_address=None,
                    auxiliary_ingress_enabled=False,
                    auxiliary_ingress_route=[],
                    auxiliary_ingress_context_ref=[],
                    schedule_file=None,
                    imap_host=None,
                    imap_port=None,
                    imap_username=None,
                    imap_password_env=None,
                    imap_mailbox=None,
                    imap_search=None,
                    telegram_enabled=False,
                    telegram_bot_token_env=None,
                    telegram_api_base_url=None,
                    telegram_poll_timeout_seconds=None,
                    telegram_attachment_dir=None,
                    telegram_allowed_chat_id=[],
                    telegram_allowed_channel_id=[],
                    telegram_context_ref=[],
                    context_ref=[],
                ),
                config,
            )

        self.assertEqual(len(connectors), 2)
        self.assertIsInstance(connectors[0], AuxiliaryIngressConnector)
        self.assertIsInstance(connectors[1], AuxiliaryIngressConnector)
        self.assertEqual(broker.ensured, ["generic-post", "new-service"])
        self.assertEqual(connectors[0]._reply_target, "generic-post")
        self.assertEqual(connectors[1]._reply_target, "new-service")

    def test_load_schedule_jobs_accepts_cron_timezone_and_interval_fallback(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                            "timezone": "Europe/London",
                            "interval_seconds": 86400,
                        },
                        {
                            "job_name": "interval-job",
                            "content": "Run interval job",
                            "interval_seconds": 300,
                            "start_at": "2026-03-07T00:00:00Z",
                            "prompt_context": ["Mention overdue alerts first."],
                        },
                    ]
                ),
                encoding="utf-8",
            )

            jobs = _load_schedule_jobs(str(schedule_path))

            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[0].cron, "0 8 * * *")
            self.assertEqual(jobs[0].timezone_name, "Europe/London")
            self.assertEqual(jobs[0].interval_seconds, 86400)
            self.assertEqual(jobs[1].interval_seconds, 300)
            self.assertEqual(jobs[1].cron, None)
            self.assertEqual(jobs[1].prompt_context, ["Mention overdue alerts first."])

    def test_load_schedule_jobs_rejects_invalid_prompt_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "interval-job",
                            "content": "Run interval job",
                            "interval_seconds": 300,
                            "prompt_context": [""],
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "prompt_context must be a list of non-empty strings",
            ):
                _load_schedule_jobs(str(schedule_path))

    def test_load_schedule_jobs_defaults_cron_timezone_to_utc(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            jobs = _load_schedule_jobs(str(schedule_path))

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].timezone_name, "UTC")

    def test_load_schedule_jobs_ignores_interval_anchors_when_cron_is_present(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                            "interval_seconds": "legacy-value",
                            "start_at": 123,
                        }
                    ]
                ),
                encoding="utf-8",
            )

            jobs = _load_schedule_jobs(str(schedule_path))

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].cron, "0 8 * * *")
            self.assertEqual(jobs[0].interval_seconds, None)
            self.assertEqual(jobs[0].start_at, None)

    def test_load_schedule_jobs_rejects_invalid_cron_timezone(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                            "timezone": "Not/AZone",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "invalid timezone"):
                _load_schedule_jobs(str(schedule_path))

    def test_load_schedule_jobs_rejects_timezone_without_cron(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "interval-job",
                            "content": "Run interval job",
                            "interval_seconds": 60,
                            "timezone": "UTC",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "timezone is only valid with cron"):
                _load_schedule_jobs(str(schedule_path))

    def test_main_message_handler_publishes_assignment_event_once_and_uses_checkpoint(
        self,
    ) -> None:
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
                    "app.handler.connectors.github_issue_assignment_connector.fetch_assignment_events_for_repository",
                    side_effect=[[event], [event]],
                ),
                patch(
                    "app.handler.connectors.github_issue_assignment_connector.expand_repository_patterns",
                    return_value=["brokensbone/chatting"],
                ),
                patch(
                    "app.handler.connectors.github_pull_request_review_connector.fetch_pull_request_review_events_for_repository",
                    side_effect=[[], []],
                ),
                patch(
                    "app.handler.connectors.github_pull_request_review_connector.expand_repository_patterns",
                    return_value=["brokensbone/chatting"],
                ),
                patch(
                    "app.main_message_handler._build_live_connectors", return_value=[]
                ),
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
                        "--max-loops",
                        "2",
                        "--poll-interval-seconds",
                        "0.01",
                    ],
                ),
            ):
                exit_code = main()

            published = self._non_heartbeat_published(broker)
            self.assertEqual(exit_code, 0)
            self.assertEqual(
                broker.ensured, ["chatting.tasks.v1", "chatting.egress.v1"]
            )
            self.assertEqual(len(published), 1)
            self.assertEqual(published[0][0], "chatting.tasks.v1")
            self.assertEqual(
                published[0][1]["task_id"],
                "task:github-assignment:brokensbone/chatting:12:AE_1",
            )
            self.assertEqual(
                published[0][1]["envelope"]["reply_channel"],
                {
                    "type": "github",
                    "target": "https://github.com/brokensbone/chatting/issues/12",
                },
            )

    def test_main_message_handler_resolves_assignee_from_authenticated_gh_user(
        self,
    ) -> None:
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
                    "app.handler.connectors.github_issue_assignment_connector.expand_repository_patterns",
                    return_value=["brokensbone/chatting"],
                ),
                patch(
                    "app.handler.connectors.github_issue_assignment_connector.fetch_assignment_events_for_repository",
                    return_value=[event],
                ),
                patch(
                    "app.handler.connectors.github_pull_request_review_connector.expand_repository_patterns",
                    return_value=["brokensbone/chatting"],
                ),
                patch(
                    "app.handler.connectors.github_pull_request_review_connector.fetch_pull_request_review_events_for_repository",
                    return_value=[],
                ),
                patch(
                    "app.main_message_handler._build_live_connectors", return_value=[]
                ),
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
                        "--max-loops",
                        "1",
                        "--poll-interval-seconds",
                        "0.01",
                    ],
                ),
            ):
                exit_code = main()

            published = self._non_heartbeat_published(broker)
            self.assertEqual(exit_code, 0)
            self.assertEqual(
                broker.ensured, ["chatting.tasks.v1", "chatting.egress.v1"]
            )
            self.assertEqual(len(published), 1)
            self.assertEqual(published[0][0], "chatting.tasks.v1")

    def test_main_message_handler_publishes_pull_request_review_event_once_and_uses_checkpoint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            broker = _FakeBroker()
            review_event = GitHubPullRequestReviewEvent(
                event_id="PRR_1",
                event_created_at=datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc),
                repository_id="R_1",
                repository_name_with_owner="brokensbone/chatting",
                pull_request_id="PR_1",
                pull_request_number=60,
                pull_request_title="Add ingress from GitHub reviews",
                pull_request_body="PR body",
                pull_request_url="https://github.com/brokensbone/chatting/pull/60",
                pull_request_author_login="BillyAcachofa",
                review_author_login="brokensbone",
                review_state="CHANGES_REQUESTED",
                review_body="Please address the comments.",
                review_url="https://github.com/brokensbone/chatting/pull/60#pullrequestreview-1",
                review_comment_count=2,
                closing_issue_refs=["#60 Add ingress from GitHub reviews"],
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
                    "app.handler.connectors.github_issue_assignment_connector.fetch_assignment_events_for_repository",
                    side_effect=[[], []],
                ),
                patch(
                    "app.handler.connectors.github_issue_assignment_connector.expand_repository_patterns",
                    return_value=["brokensbone/chatting"],
                ),
                patch(
                    "app.handler.connectors.github_pull_request_review_connector.fetch_pull_request_review_events_for_repository",
                    side_effect=[[review_event], [review_event]],
                ),
                patch(
                    "app.handler.connectors.github_pull_request_review_connector.expand_repository_patterns",
                    return_value=["brokensbone/chatting"],
                ),
                patch(
                    "app.main_message_handler._build_live_connectors", return_value=[]
                ),
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
                        "--max-loops",
                        "2",
                        "--poll-interval-seconds",
                        "0.01",
                    ],
                ),
            ):
                exit_code = main()

            published = self._non_heartbeat_published(broker)
            self.assertEqual(exit_code, 0)
            self.assertEqual(len(published), 1)
            self.assertEqual(
                published[0][1]["task_id"],
                "task:github-review:brokensbone/chatting:60:PRR_1",
            )
            self.assertEqual(
                published[0][1]["envelope"]["reply_channel"],
                {
                    "type": "github",
                    "target": "https://github.com/brokensbone/chatting/pull/60",
                },
            )

    def test_main_message_handler_disables_misconfigured_github_ingress_and_keeps_running(
        self,
    ) -> None:
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
                    "app.main_message_handler.fetch_authenticated_viewer_login",
                    side_effect=RuntimeError("no gh login"),
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

            published = self._non_heartbeat_published(broker)
            self.assertEqual(exit_code, 0)
            self.assertEqual(
                broker.ensured, ["chatting.tasks.v1", "chatting.egress.v1"]
            )
            self.assertEqual(len(published), 0)
            self.assertEqual(len(broker.published), 2)
            disabled_log_calls = [
                call
                for call in error_logger.call_args_list
                if call.args
                and call.args[0]
                == "ingress_connector_disabled connector=%s loop=%s error=%s"
            ]
            self.assertEqual(len(disabled_log_calls), 2)
            self.assertTrue(
                all(call.args[1] == "github" for call in disabled_log_calls)
            )

    def test_main_message_handler_continues_with_healthy_connectors_when_imap_misconfigured(
        self,
    ) -> None:
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

            published = self._non_heartbeat_published(broker)
            self.assertEqual(exit_code, 0)
            self.assertEqual(
                broker.ensured, ["chatting.tasks.v1", "chatting.egress.v1"]
            )
            self.assertEqual(len(published), 1)
            self.assertEqual(published[0][0], "chatting.tasks.v1")
            disabled_log_calls = [
                call
                for call in error_logger.call_args_list
                if call.args
                and call.args[0]
                == "ingress_connector_disabled connector=%s loop=%s error=%s"
            ]
            self.assertEqual(len(disabled_log_calls), 1)
            self.assertEqual(disabled_log_calls[0].args[1], "imap")

    def test_main_message_handler_publishes_internal_heartbeat_by_default(self) -> None:
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
            self.assertEqual(len(broker.published), 1)
            self.assertEqual(broker.published[0][0], "chatting.tasks.v1")
            self.assertEqual(broker.published[0][1]["envelope"]["source"], "internal")
            self.assertEqual(
                broker.published[0][1]["envelope"]["reply_channel"],
                {"type": "internal", "target": "heartbeat"},
            )

    def test_main_message_handler_starts_and_stops_metrics_server_with_defaults(
        self,
    ) -> None:
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

    def test_main_message_handler_drains_egress_queue_until_empty(self) -> None:
        @dataclass
        class _QueueDrainingBroker(_FakeBroker):
            egress_messages: deque[tuple[str, dict[str, object]]] = field(
                default_factory=deque
            )
            pickup_waits: list[int] = field(default_factory=list)

            def pickup_json(
                self, queue_name: str, timeout_seconds: int, wait_seconds: int
            ):
                del timeout_seconds
                self.pickup_waits.append(wait_seconds)
                if queue_name != "chatting.egress.v1" or not self.egress_messages:
                    return None
                guid, payload = self.egress_messages.popleft()
                return type("PickedMessage", (), {"guid": guid, "payload": payload})()

        @dataclass
        class _SingleEnvelopeConnector:
            envelope: TaskEnvelope
            emitted: bool = False

            def poll(self):
                if self.emitted:
                    return []
                self.emitted = True
                return [self.envelope]

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            broker = _QueueDrainingBroker()
            envelope = TaskEnvelope(
                id="email:drain-1",
                source="email",
                received_at=datetime(2026, 3, 7, 10, 47, 35, tzinfo=timezone.utc),
                actor="alice@example.com",
                content="hello",
                attachments=[],
                context_refs=[],
                reply_channel=ReplyChannel(type="email", target="alice@example.com"),
                dedupe_key="email:drain-1",
            )
            task_message = TaskQueueMessage.from_envelope(
                envelope, trace_id="trace:email:drain-1"
            )
            first_egress = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=0,
                event_count=2,
                message=OutboundMessage(
                    channel="email", target="alice@example.com", body="first"
                ),
                emitted_at=datetime(2026, 3, 7, 10, 47, 36, tzinfo=timezone.utc),
                event_id="evt:task:email:drain-1:0",
                sequence=0,
                event_kind="incremental",
                message_type="chatting.egress.v2",
            )
            second_egress = EgressQueueMessage(
                task_id=task_message.task_id,
                envelope_id=task_message.envelope.id,
                trace_id=task_message.trace_id,
                event_index=1,
                event_count=2,
                message=OutboundMessage(channel="internal", target="task", body="done"),
                emitted_at=datetime(2026, 3, 7, 10, 47, 37, tzinfo=timezone.utc),
                event_id="evt:task:email:drain-1:1",
                sequence=1,
                event_kind="completion",
                message_type="chatting.egress.v2",
            )
            broker.egress_messages.extend(
                [
                    ("guid-egress-1", first_egress.to_dict()),
                    ("guid-egress-2", second_egress.to_dict()),
                ]
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
                    "app.main_message_handler._build_live_connectors",
                    return_value=[_SingleEnvelopeConnector(envelope=envelope)],
                ),
                patch(
                    "app.main_message_handler._build_email_sender",
                    return_value=lambda message: None,
                ),
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
                        "--allowed-egress-channel",
                        "email",
                    ],
                ),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            self.assertEqual(
                broker.acked,
                [
                    ("chatting.egress.v1", "guid-egress-1"),
                    ("chatting.egress.v1", "guid-egress-2"),
                ],
            )
            self.assertEqual(broker.pickup_waits[:3], [5, 0, 0])
            self.assertIn(broker.pickup_waits[3:], ([], [5]))


if __name__ == "__main__":
    unittest.main()
