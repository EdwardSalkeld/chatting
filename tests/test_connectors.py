import unittest
from datetime import datetime, timezone
from email.message import EmailMessage as ParsedEmailMessage
from tempfile import TemporaryDirectory

from app.handler.connectors.github_issue_assignment_connector import (
    GitHubIssueAssignmentConnector,
)
from app.handler.connectors.github_pull_request_review_connector import (
    GitHubPullRequestReviewConnector,
)
from app.handler.connectors.imap_email_connector import ImapEmailConnector
from app.handler.connectors.internal_heartbeat_connector import (
    InternalHeartbeatConnector,
)
from app.handler.connectors.interval_schedule_connector import (
    IntervalScheduleConnector,
    IntervalScheduleJob,
)
from app.handler.connectors.auxiliary_ingress_connector import AuxiliaryIngressConnector
from app.handler.connectors.telegram_connector import (
    TelegramFileMetadata,
    TelegramConnector,
    TelegramGetUpdatesResponse,
)
from app.handler.connectors.slack_connector import SlackConnector
from app.handler.connectors.webhook_connector import WebhookConnector, WebhookEvent
from app.handler.github_ingress import GitHubAssignmentCheckpointStore
from app.models import PromptContext
from tests.fixtures import (
    CronTrigger,
    EmailMessage,
    FakeCronConnector,
    FakeEmailConnector,
)


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
        self.assertEqual(
            envelope.dedupe_key, "cron:daily-summary:2026-02-27T09:00:00+00:00"
        )
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

    def test_poll_merges_global_source_and_job_prompt_context(self) -> None:
        clock = _MutableClock(datetime(2026, 2, 28, 10, 0, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="heartbeat",
                    content="Run scheduled heartbeat",
                    interval_seconds=60,
                    start_at=datetime(2026, 2, 28, 10, 0, tzinfo=timezone.utc),
                    context_refs=[],
                    prompt_context=["Include yesterday's failures first."],
                )
            ],
            global_prompt_context=["Keep scheduled reports terse."],
            source_prompt_context=["This is a scheduled automation task."],
            now_provider=clock.now,
        )

        envelope = connector.poll()[0]

        self.assertEqual(
            envelope.prompt_context.assembled_instructions(),
            [
                "Keep scheduled reports terse.",
                "This is a scheduled automation task.",
                "Include yesterday's failures first.",
            ],
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

    def test_job_rejects_invalid_cron_expression(self) -> None:
        with self.assertRaises(ValueError):
            IntervalScheduleJob(
                job_name="daily-brief",
                content="Run",
                cron="0 24 * * *",
                timezone_name="UTC",
                context_refs=[],
            )

    def test_job_rejects_invalid_timezone(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid timezone"):
            IntervalScheduleJob(
                job_name="daily-brief",
                content="Run",
                cron="0 8 * * *",
                timezone_name="Mars/Olympus",
                context_refs=[],
            )

    def test_cron_job_defaults_timezone_to_utc(self) -> None:
        job = IntervalScheduleJob(
            job_name="utc-default",
            content="Run",
            cron="0 8 * * *",
            context_refs=[],
        )

        self.assertIsNone(job.timezone_name)

    def test_poll_emits_cron_job_in_configured_timezone(self) -> None:
        clock = _MutableClock(datetime(2026, 2, 28, 8, 0, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="london-morning",
                    content="Send daily briefing",
                    cron="0 8 * * *",
                    timezone_name="Europe/London",
                    context_refs=["repo:/home/edward/chatting"],
                )
            ],
            now_provider=clock.now,
        )

        first_poll = connector.poll()
        self.assertEqual(len(first_poll), 1)
        self.assertEqual(
            first_poll[0].dedupe_key,
            "cron:london-morning:2026-02-28T08:00:00+00:00",
        )

        self.assertEqual(connector.poll(), [])

        clock.set(datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc))
        second_poll = connector.poll()
        self.assertEqual(len(second_poll), 1)
        self.assertEqual(
            second_poll[0].dedupe_key,
            "cron:london-morning:2026-03-01T08:00:00+00:00",
        )

    def test_cron_schedule_takes_precedence_over_interval_fields(self) -> None:
        clock = _MutableClock(datetime(2026, 2, 28, 8, 0, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="cron-wins",
                    content="Use the cron schedule",
                    interval_seconds=60,
                    start_at=datetime(2026, 2, 28, 7, 0, tzinfo=timezone.utc),
                    cron="0 8 * * *",
                    timezone_name="UTC",
                    context_refs=[],
                )
            ],
            now_provider=clock.now,
        )

        first_poll = connector.poll()
        self.assertEqual(len(first_poll), 1)
        self.assertEqual(
            first_poll[0].dedupe_key,
            "cron:cron-wins:2026-02-28T08:00:00+00:00",
        )

        clock.set(datetime(2026, 2, 28, 8, 1, tzinfo=timezone.utc))
        self.assertEqual(connector.poll(), [])

        clock.set(datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc))
        second_poll = connector.poll()
        self.assertEqual(len(second_poll), 1)
        self.assertEqual(
            second_poll[0].dedupe_key,
            "cron:cron-wins:2026-03-01T08:00:00+00:00",
        )

    def test_cron_schedule_fires_near_nonexistent_dst_local_times(self) -> None:
        # On March 29 2026 in Europe/London, clocks spring forward at 01:00
        # so 01:30 local doesn't exist. croniter picks the nearest valid time.
        clock = _MutableClock(datetime(2026, 3, 29, 0, 45, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="dst-skip",
                    content="Run at 1:30 local time",
                    cron="30 1 * * *",
                    timezone_name="Europe/London",
                    context_refs=[],
                )
            ],
            now_provider=clock.now,
        )

        # First poll: next fire is 01:00 UTC (02:00 BST), still in the future
        self.assertEqual(connector.poll(), [])

        clock.set(datetime(2026, 3, 29, 1, 0, tzinfo=timezone.utc))
        second_poll = connector.poll()
        self.assertEqual(len(second_poll), 1)
        self.assertEqual(
            second_poll[0].dedupe_key,
            "cron:dst-skip:2026-03-29T01:00:00+00:00",
        )

    def test_cron_schedule_runs_twice_for_repeated_dst_local_time(self) -> None:
        clock = _MutableClock(datetime(2026, 10, 25, 0, 30, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="dst-repeat",
                    content="Run at 1:30 local time",
                    cron="30 1 * * *",
                    timezone_name="Europe/London",
                    context_refs=[],
                )
            ],
            now_provider=clock.now,
        )

        first_poll = connector.poll()
        self.assertEqual(len(first_poll), 1)
        self.assertEqual(
            first_poll[0].dedupe_key,
            "cron:dst-repeat:2026-10-25T00:30:00+00:00",
        )

        clock.set(datetime(2026, 10, 25, 1, 30, tzinfo=timezone.utc))
        second_poll = connector.poll()
        self.assertEqual(len(second_poll), 1)
        self.assertEqual(
            second_poll[0].dedupe_key,
            "cron:dst-repeat:2026-10-25T01:30:00+00:00",
        )

    def test_poll_allows_custom_reply_channel_for_scheduled_job(self) -> None:
        clock = _MutableClock(datetime(2026, 2, 28, 10, 0, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="morning-weather",
                    content="Check weather in Leeds",
                    interval_seconds=60,
                    start_at=datetime(2026, 2, 28, 10, 0, tzinfo=timezone.utc),
                    context_refs=["repo:/home/edward/chatting"],
                    reply_channel_type="telegram",
                    reply_channel_target="8605042448",
                )
            ],
            now_provider=clock.now,
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        self.assertEqual(envelopes[0].reply_channel.type, "telegram")
        self.assertEqual(envelopes[0].reply_channel.target, "8605042448")

    def test_poll_emits_due_cron_job_in_configured_timezone(self) -> None:
        clock = _MutableClock(datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="london-morning",
                    content="Run London morning task",
                    cron="0 8 * * *",
                    timezone_name="Europe/London",
                    context_refs=[],
                )
            ],
            now_provider=clock.now,
        )

        first_poll = connector.poll()

        self.assertEqual(len(first_poll), 1)
        self.assertEqual(
            first_poll[0].dedupe_key,
            "cron:london-morning:2026-03-01T08:00:00+00:00",
        )
        self.assertEqual(connector.poll(), [])

        clock.set(datetime(2026, 3, 2, 8, 0, tzinfo=timezone.utc))
        second_poll = connector.poll()
        self.assertEqual(len(second_poll), 1)
        self.assertEqual(
            second_poll[0].dedupe_key,
            "cron:london-morning:2026-03-02T08:00:00+00:00",
        )

    def test_poll_uses_cron_precedence_when_interval_is_also_present(self) -> None:
        clock = _MutableClock(datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc))
        connector = IntervalScheduleConnector(
            jobs=[
                IntervalScheduleJob(
                    job_name="cron-wins",
                    content="Run cron-scheduled task",
                    interval_seconds=60,
                    cron="0 8 * * *",
                    timezone_name="Europe/London",
                    context_refs=[],
                )
            ],
            now_provider=clock.now,
        )

        first_poll = connector.poll()
        self.assertEqual(len(first_poll), 1)

        clock.set(datetime(2026, 3, 1, 8, 1, tzinfo=timezone.utc))
        self.assertEqual(connector.poll(), [])

    def test_cron_job_rejects_invalid_timezone(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid timezone"):
            IntervalScheduleJob(
                job_name="bad-timezone",
                content="Run",
                cron="0 8 * * *",
                timezone_name="Europe/NotARealPlace",
                context_refs=[],
            )

    def test_cron_job_rejects_invalid_cron_expression(self) -> None:
        with self.assertRaises(ValueError):
            IntervalScheduleJob(
                job_name="bad-cron",
                content="Run",
                cron="61 8 * * *",
                timezone_name="Europe/London",
                context_refs=[],
            )


class GitHubIssueAssignmentConnectorTests(unittest.TestCase):
    def test_poll_normalizes_new_assignment_events_to_envelopes(self) -> None:
        responses = [
            {
                "data": {
                    "repository": {
                        "id": "R_1",
                        "nameWithOwner": "brokensbone/chatting",
                        "issues": {
                            "nodes": [
                                {
                                    "id": "I_1",
                                    "number": 12,
                                    "title": "Plan milestone 5",
                                    "body": "Body text",
                                    "url": "https://github.com/brokensbone/chatting/issues/12",
                                    "labels": {"nodes": [{"name": "enhancement"}]},
                                    "timelineItems": {
                                        "nodes": [
                                            {
                                                "id": "AE_1",
                                                "createdAt": "2026-03-07T10:47:35Z",
                                                "actor": {"login": "edward"},
                                                "assignee": {
                                                    "__typename": "User",
                                                    "login": "BillyAcachofa",
                                                },
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                }
            },
            {
                "data": {
                    "repository": {
                        "id": "R_1",
                        "nameWithOwner": "brokensbone/chatting",
                        "issues": {
                            "nodes": [
                                {
                                    "id": "I_1",
                                    "number": 12,
                                    "title": "Plan milestone 5",
                                    "body": "Body text",
                                    "url": "https://github.com/brokensbone/chatting/issues/12",
                                    "labels": {"nodes": [{"name": "enhancement"}]},
                                    "timelineItems": {
                                        "nodes": [
                                            {
                                                "id": "AE_1",
                                                "createdAt": "2026-03-07T10:47:35Z",
                                                "actor": {"login": "edward"},
                                                "assignee": {
                                                    "__typename": "User",
                                                    "login": "BillyAcachofa",
                                                },
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                }
            },
        ]

        def _graphql_runner(
            query: str, variables: dict[str, object]
        ) -> dict[str, object]:
            del query
            self.assertEqual(variables["repoOwner"], "brokensbone")
            self.assertEqual(variables["repoName"], "chatting")
            return responses.pop(0)

        with TemporaryDirectory() as tmpdir:
            connector = GitHubIssueAssignmentConnector(
                repository_patterns=["brokensbone/chatting"],
                assignee_login="BillyAcachofa",
                context_refs=["repo:/home/edward/chatting"],
                checkpoint_store=GitHubAssignmentCheckpointStore(f"{tmpdir}/state.db"),
                graphql_runner=_graphql_runner,
            )

            first_poll = connector.poll()
            second_poll = connector.poll()

        self.assertEqual(len(first_poll), 1)
        envelope = first_poll[0]
        self.assertEqual(envelope.id, "github-assignment:brokensbone/chatting:12:AE_1")
        self.assertEqual(envelope.reply_channel.type, "github")
        self.assertEqual(
            envelope.reply_channel.target,
            "https://github.com/brokensbone/chatting/issues/12",
        )
        self.assertEqual(envelope.context_refs, ["repo:/home/edward/chatting"])
        self.assertEqual(envelope.dedupe_key, "github:R_1:I_1:AE_1")
        self.assertEqual(second_poll, [])
        self.assertEqual(connector.last_poll_scanned_events, 1)
        self.assertEqual(connector.last_poll_new_events, 0)
        self.assertEqual(connector.last_poll_checkpoint_id, "AE_1")

    def test_poll_continues_when_one_repository_fetch_fails(self) -> None:
        calls: list[tuple[str, str]] = []

        def _graphql_runner(
            query: str, variables: dict[str, object]
        ) -> dict[str, object]:
            del query
            calls.append((str(variables["repoOwner"]), str(variables["repoName"])))
            if variables["repoName"] == "chatting":
                raise RuntimeError("boom")
            return {
                "data": {
                    "repository": {
                        "id": "R_2",
                        "nameWithOwner": "brokensbone/bbmb",
                        "issues": {
                            "nodes": [
                                {
                                    "id": "I_2",
                                    "number": 34,
                                    "title": "Build something",
                                    "body": "",
                                    "url": "https://github.com/brokensbone/bbmb/issues/34",
                                    "labels": {"nodes": []},
                                    "timelineItems": {
                                        "nodes": [
                                            {
                                                "id": "AE_2",
                                                "createdAt": "2026-03-07T11:00:00Z",
                                                "actor": {"login": "edward"},
                                                "assignee": {
                                                    "__typename": "User",
                                                    "login": "BillyAcachofa",
                                                },
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                }
            }

        with TemporaryDirectory() as tmpdir:
            connector = GitHubIssueAssignmentConnector(
                repository_patterns=["brokensbone/chatting", "brokensbone/bbmb"],
                assignee_login="BillyAcachofa",
                context_refs=[],
                checkpoint_store=GitHubAssignmentCheckpointStore(f"{tmpdir}/state.db"),
                graphql_runner=_graphql_runner,
            )

            with self.assertLogs(
                "app.handler.connectors.github_issue_assignment_connector",
                level="ERROR",
            ) as logs:
                envelopes = connector.poll()

        self.assertEqual([call[1] for call in calls], ["chatting", "bbmb"])
        self.assertEqual(len(envelopes), 1)
        self.assertEqual(envelopes[0].id, "github-assignment:brokensbone/bbmb:34:AE_2")
        self.assertTrue(
            any(
                "github_assignment_poll_failed repository=brokensbone/chatting assignee=BillyAcachofa"
                in line
                for line in logs.output
            )
        )


class GitHubPullRequestReviewConnectorTests(unittest.TestCase):
    def test_poll_normalizes_new_review_events_to_envelopes(self) -> None:
        responses = [
            {
                "data": {
                    "repository": {
                        "id": "R_1",
                        "nameWithOwner": "brokensbone/chatting",
                        "pullRequests": {
                            "nodes": [
                                {
                                    "id": "PR_1",
                                    "number": 60,
                                    "title": "Add ingress from GitHub reviews",
                                    "body": "PR body",
                                    "url": "https://github.com/brokensbone/chatting/pull/60",
                                    "author": {"login": "BillyAcachofa"},
                                    "closingIssuesReferences": {
                                        "nodes": [
                                            {
                                                "number": 60,
                                                "title": "Add ingress from GitHub reviews",
                                                "url": "https://github.com/brokensbone/chatting/issues/60",
                                            }
                                        ]
                                    },
                                    "reviews": {
                                        "nodes": [
                                            {
                                                "id": "PRR_1",
                                                "submittedAt": "2026-03-07T12:00:00Z",
                                                "state": "CHANGES_REQUESTED",
                                                "body": "Please address the comments.",
                                                "url": "https://github.com/brokensbone/chatting/pull/60#pullrequestreview-1",
                                                "author": {"login": "brokensbone"},
                                                "comments": {"totalCount": 2},
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                }
            },
            {
                "data": {
                    "repository": {
                        "id": "R_1",
                        "nameWithOwner": "brokensbone/chatting",
                        "pullRequests": {
                            "nodes": [
                                {
                                    "id": "PR_1",
                                    "number": 60,
                                    "title": "Add ingress from GitHub reviews",
                                    "body": "PR body",
                                    "url": "https://github.com/brokensbone/chatting/pull/60",
                                    "author": {"login": "BillyAcachofa"},
                                    "closingIssuesReferences": {"nodes": []},
                                    "reviews": {
                                        "nodes": [
                                            {
                                                "id": "PRR_1",
                                                "submittedAt": "2026-03-07T12:00:00Z",
                                                "state": "CHANGES_REQUESTED",
                                                "body": "Please address the comments.",
                                                "url": "https://github.com/brokensbone/chatting/pull/60#pullrequestreview-1",
                                                "author": {"login": "brokensbone"},
                                                "comments": {"totalCount": 2},
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                }
            },
        ]

        def _graphql_runner(
            query: str, variables: dict[str, object]
        ) -> dict[str, object]:
            del query
            self.assertEqual(variables["repoOwner"], "brokensbone")
            self.assertEqual(variables["repoName"], "chatting")
            return responses.pop(0)

        with TemporaryDirectory() as tmpdir:
            connector = GitHubPullRequestReviewConnector(
                repository_patterns=["brokensbone/chatting"],
                author_login="BillyAcachofa",
                context_refs=["repo:/home/edward/chatting"],
                checkpoint_store=GitHubAssignmentCheckpointStore(f"{tmpdir}/state.db"),
                graphql_runner=_graphql_runner,
            )

            first_poll = connector.poll()
            second_poll = connector.poll()

        self.assertEqual(len(first_poll), 1)
        envelope = first_poll[0]
        self.assertEqual(envelope.id, "github-review:brokensbone/chatting:60:PRR_1")
        self.assertEqual(envelope.reply_channel.type, "github")
        self.assertEqual(
            envelope.reply_channel.target,
            "https://github.com/brokensbone/chatting/pull/60",
        )
        self.assertEqual(envelope.context_refs, ["repo:/home/edward/chatting"])
        self.assertEqual(envelope.dedupe_key, "github-review:R_1:PR_1:PRR_1")
        self.assertIn(
            "Linked issues: #60 Add ingress from GitHub reviews", envelope.content
        )
        self.assertEqual(second_poll, [])
        self.assertEqual(connector.last_poll_scanned_events, 1)
        self.assertEqual(connector.last_poll_new_events, 0)
        self.assertEqual(connector.last_poll_checkpoint_id, "PRR_1")

    def test_poll_continues_when_one_repository_fetch_fails(self) -> None:
        calls: list[tuple[str, str]] = []

        def _graphql_runner(
            query: str, variables: dict[str, object]
        ) -> dict[str, object]:
            del query
            calls.append((str(variables["repoOwner"]), str(variables["repoName"])))
            if variables["repoName"] == "chatting":
                raise RuntimeError("boom")
            return {
                "data": {
                    "repository": {
                        "id": "R_2",
                        "nameWithOwner": "brokensbone/bbmb",
                        "pullRequests": {
                            "nodes": [
                                {
                                    "id": "PR_2",
                                    "number": 34,
                                    "title": "Build something",
                                    "body": "",
                                    "url": "https://github.com/brokensbone/bbmb/pull/34",
                                    "author": {"login": "BillyAcachofa"},
                                    "closingIssuesReferences": {"nodes": []},
                                    "reviews": {
                                        "nodes": [
                                            {
                                                "id": "PRR_2",
                                                "submittedAt": "2026-03-07T13:00:00Z",
                                                "state": "COMMENTED",
                                                "body": "",
                                                "url": "https://github.com/brokensbone/bbmb/pull/34#pullrequestreview-2",
                                                "author": {"login": "brokensbone"},
                                                "comments": {"totalCount": 1},
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                }
            }

        with TemporaryDirectory() as tmpdir:
            connector = GitHubPullRequestReviewConnector(
                repository_patterns=["brokensbone/chatting", "brokensbone/bbmb"],
                author_login="BillyAcachofa",
                context_refs=[],
                checkpoint_store=GitHubAssignmentCheckpointStore(f"{tmpdir}/state.db"),
                graphql_runner=_graphql_runner,
            )

            with self.assertLogs(
                "app.handler.connectors.github_pull_request_review_connector",
                level="ERROR",
            ) as logs:
                envelopes = connector.poll()

        self.assertEqual([call[1] for call in calls], ["chatting", "bbmb"])
        self.assertEqual(len(envelopes), 1)
        self.assertEqual(envelopes[0].id, "github-review:brokensbone/bbmb:34:PRR_2")
        self.assertTrue(
            any(
                "github_pull_request_review_poll_failed repository=brokensbone/chatting author=BillyAcachofa"
                in line
                for line in logs.output
            )
        )


class InternalHeartbeatConnectorTests(unittest.TestCase):
    def test_poll_emits_internal_heartbeat_with_unique_ids(self) -> None:
        current = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
        connector = InternalHeartbeatConnector(now_provider=lambda: current)

        first = connector.poll()
        second = connector.poll()

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(first[0].source, "internal")
        self.assertEqual(first[0].actor, "message-handler")
        self.assertEqual(first[0].reply_channel.type, "internal")
        self.assertEqual(first[0].reply_channel.target, "heartbeat")
        self.assertNotEqual(first[0].id, second[0].id)
        self.assertEqual(first[0].dedupe_key, first[0].id)
        self.assertEqual(second[0].dedupe_key, second[0].id)


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

    def test_poll_attaches_prompt_context_to_email_envelope(self) -> None:
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
            prompt_context=PromptContext(
                global_instructions=["Keep replies concise."],
                reply_channel_instructions=["Use a clear email subject line."],
            ),
            now_provider=lambda: datetime(2026, 2, 28, 10, 30, tzinfo=timezone.utc),
        )

        envelope = connector.poll()[0]

        self.assertEqual(
            envelope.prompt_context.assembled_instructions(),
            [
                "Keep replies concise.",
                "Use a clear email subject line.",
            ],
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
        self.assertEqual(envelope.reply_channel.metadata, {"message_id": 1})
        self.assertEqual(envelope.content, "hello from telegram")
        self.assertEqual(envelope.context_refs, ["repo:/home/edward/chatting"])
        self.assertEqual(second_poll, [])
        self.assertIn("timeout=20", seen_urls[0])
        self.assertIn("offset=1003", seen_urls[1])

    def test_poll_attaches_prompt_context_to_telegram_envelope(self) -> None:
        connector = TelegramConnector(
            bot_token="token",
            allowed_chat_ids=["12345"],
            prompt_context=PromptContext(
                global_instructions=["Keep replies concise."],
                reply_channel_instructions=["Write like a short Telegram chat reply."],
            ),
            http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
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
                    }
                ],
            ),
        )

        envelope = connector.poll()[0]

        self.assertEqual(
            envelope.prompt_context.assembled_instructions(),
            [
                "Keep replies concise.",
                "Write like a short Telegram chat reply.",
            ],
        )

    def test_poll_respects_allowed_chat_ids_and_skips_unsupported_payloads(
        self,
    ) -> None:
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
                    {
                        "update_id": 2004,
                        "channel_post": {
                            "message_id": 4,
                            "date": 1772272803,
                            "text": "channel post should be ignored by default",
                            "chat": {"id": -100123, "type": "channel"},
                            "sender_chat": {"id": -100123, "title": "release-feed"},
                        },
                    },
                ],
            ),
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        self.assertEqual(envelopes[0].id, "telegram:2001")

    def test_poll_accepts_channel_post_when_channel_id_is_explicitly_allowed(
        self,
    ) -> None:
        connector = TelegramConnector(
            bot_token="token",
            allowed_channel_ids=["-100123"],
            http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                ok=True,
                result=[
                    {
                        "update_id": 3001,
                        "channel_post": {
                            "message_id": 1,
                            "date": 1772272800,
                            "text": "deploy completed",
                            "chat": {"id": -100123, "type": "channel"},
                            "sender_chat": {"id": -100123, "title": "release-feed"},
                        },
                    },
                    {
                        "update_id": 3002,
                        "channel_post": {
                            "message_id": 2,
                            "date": 1772272801,
                            "text": "should be blocked",
                            "chat": {"id": -100999, "type": "channel"},
                            "sender_chat": {"id": -100999, "title": "other"},
                        },
                    },
                ],
            ),
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertEqual(envelope.id, "telegram:3001")
        self.assertEqual(envelope.reply_channel.type, "telegram")
        self.assertEqual(envelope.reply_channel.target, "-100123")
        self.assertEqual(envelope.reply_channel.metadata, {"message_id": 1})
        self.assertEqual(envelope.actor, "-100123:release-feed")
        self.assertEqual(envelope.content, "deploy completed")

    def test_poll_logs_ignored_channel_post_ids(self) -> None:
        connector = TelegramConnector(
            bot_token="token",
            allowed_channel_ids=["-100123"],
            http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                ok=True,
                result=[
                    {
                        "update_id": 3101,
                        "channel_post": {
                            "message_id": 1,
                            "date": 1772272800,
                            "text": "blocked",
                            "chat": {"id": -100999, "type": "channel"},
                        },
                    }
                ],
            ),
        )

        with self.assertLogs(
            "app.handler.connectors.telegram_connector", level="INFO"
        ) as logs:
            envelopes = connector.poll()

        self.assertEqual(envelopes, [])
        self.assertTrue(
            any(
                "ignoring telegram channel_post update_id=3101 channel_id=-100999 reason=channel_not_allowlisted"
                in line
                for line in logs.output
            )
        )

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

    def test_poll_downloads_photo_attachment_and_uses_caption_as_content(self) -> None:
        with TemporaryDirectory() as tmpdir:
            connector = TelegramConnector(
                bot_token="token",
                attachment_root_dir=tmpdir,
                http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                    ok=True,
                    result=[
                        {
                            "update_id": 4001,
                            "message": {
                                "message_id": 10,
                                "date": 1772272800,
                                "caption": "what is this plant?",
                                "chat": {"id": 12345},
                                "photo": [
                                    {
                                        "file_id": "small",
                                        "file_unique_id": "u-small",
                                        "width": 90,
                                        "height": 90,
                                        "file_size": 1200,
                                    },
                                    {
                                        "file_id": "large",
                                        "file_unique_id": "u-large",
                                        "width": 1280,
                                        "height": 960,
                                        "file_size": 450000,
                                    },
                                ],
                            },
                        }
                    ],
                ),
                resolve_file_metadata=lambda url, _timeout: (
                    self.assertIn("file_id=large", url)
                    or TelegramFileMetadata(file_path="photos/leaf.jpg")
                ),
                download_file_bytes=lambda url, _timeout: (
                    self.assertIn("/file/bottoken/photos/leaf.jpg", url)
                    or b"jpeg-bytes"
                ),
            )

            envelopes = connector.poll()

            self.assertEqual(len(envelopes), 1)
            envelope = envelopes[0]
            self.assertEqual(envelope.content, "what is this plant?")
            self.assertEqual(len(envelope.attachments), 1)
            self.assertEqual(envelope.attachments[0].name, "leaf.jpg")
            self.assertTrue(envelope.attachments[0].uri.startswith("file://"))

    def test_poll_accepts_photo_only_message_with_placeholder_content(self) -> None:
        with TemporaryDirectory() as tmpdir:
            connector = TelegramConnector(
                bot_token="token",
                attachment_root_dir=tmpdir,
                http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                    ok=True,
                    result=[
                        {
                            "update_id": 4002,
                            "message": {
                                "message_id": 11,
                                "date": 1772272800,
                                "chat": {"id": 12345},
                                "photo": [
                                    {"file_id": "only", "width": 800, "height": 600}
                                ],
                            },
                        }
                    ],
                ),
                resolve_file_metadata=lambda _url, _timeout: TelegramFileMetadata(
                    file_path="photos/photo.jpg"
                ),
                download_file_bytes=lambda _url, _timeout: b"jpeg-bytes",
            )

            envelopes = connector.poll()

            self.assertEqual(len(envelopes), 1)
            self.assertEqual(envelopes[0].content, "[photo attached]")
            self.assertEqual(envelopes[0].attachments[0].name, "photo.jpg")

    def test_poll_accepts_location_only_message_and_includes_coordinates(self) -> None:
        connector = TelegramConnector(
            bot_token="token",
            http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                ok=True,
                result=[
                    {
                        "update_id": 4003,
                        "message": {
                            "message_id": 12,
                            "date": 1772272800,
                            "chat": {"id": 12345},
                            "from": {"id": 77, "username": "alice"},
                            "location": {
                                "latitude": 53.800755,
                                "longitude": -1.549077,
                                "horizontal_accuracy": 14.5,
                            },
                        },
                    }
                ],
            ),
        )

        envelope = connector.poll()[0]

        self.assertEqual(
            envelope.content,
            "[location shared]\n"
            "latitude: 53.800755\n"
            "longitude: -1.549077\n"
            "horizontal_accuracy: 14.5\n"
            "map: https://maps.google.com/?q=53.800755,-1.549077",
        )
        self.assertEqual(
            envelope.reply_channel.metadata,
            {
                "message_id": 12,
                "location": {
                    "latitude": 53.800755,
                    "longitude": -1.549077,
                    "horizontal_accuracy": 14.5,
                    "map_url": "https://maps.google.com/?q=53.800755,-1.549077",
                },
            },
        )

    def test_poll_appends_location_details_to_text_messages(self) -> None:
        connector = TelegramConnector(
            bot_token="token",
            http_get_json=lambda _url, _timeout: TelegramGetUpdatesResponse(
                ok=True,
                result=[
                    {
                        "update_id": 4004,
                        "message": {
                            "message_id": 13,
                            "date": 1772272800,
                            "chat": {"id": 12345},
                            "text": "Can you plot this?",
                            "location": {
                                "latitude": 51.507351,
                                "longitude": -0.127758,
                                "live_period": 300,
                            },
                        },
                    }
                ],
            ),
        )

        envelope = connector.poll()[0]

        self.assertEqual(
            envelope.content,
            "Can you plot this?\n\n"
            "[location shared]\n"
            "latitude: 51.507351\n"
            "longitude: -0.127758\n"
            "live_period: 300\n"
            "map: https://maps.google.com/?q=51.507351,-0.127758",
        )
        self.assertEqual(
            envelope.reply_channel.metadata["location"],
            {
                "latitude": 51.507351,
                "longitude": -0.127758,
                "live_period": 300,
                "map_url": "https://maps.google.com/?q=51.507351,-0.127758",
            },
        )


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


class AuxiliaryIngressConnectorTests(unittest.TestCase):
    def test_poll_drains_queue_and_renders_body_only_content(self) -> None:
        class _FakeBroker:
            def __init__(self) -> None:
                self.messages = [
                    (
                        "guid-1",
                        {
                            "schema_version": "1.0",
                            "message_type": "chatting.auxiliary_ingress.v1",
                            "event_id": "aux:1",
                            "received_at": "2026-04-01T12:00:00Z",
                            "body": {"hello": "world", "nested": [1, 2]},
                        },
                    )
                ]
                self.acked: list[tuple[str, str]] = []

            def pickup_json(
                self, queue_name: str, timeout_seconds: int, wait_seconds: int
            ):
                del timeout_seconds, wait_seconds
                if queue_name != "chatting.auxiliary-ingress.v1" or not self.messages:
                    return None
                guid, payload = self.messages.pop(0)
                return type("Picked", (), {"guid": guid, "payload": payload})()

            def ack(self, queue_name: str, guid: str) -> None:
                self.acked.append((queue_name, guid))

        broker = _FakeBroker()
        connector = AuxiliaryIngressConnector(
            broker=broker,  # type: ignore[arg-type]
            queue_name="chatting.auxiliary-ingress.v1",
            context_refs=["repo:/workspace/chatting"],
        )

        envelopes = connector.poll()

        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertEqual(envelope.source, "webhook")
        self.assertEqual(envelope.reply_channel.type, "webhook")
        self.assertEqual(envelope.reply_channel.target, "generic_post")
        self.assertEqual(envelope.context_refs, ["repo:/workspace/chatting"])
        self.assertEqual(
            envelope.content,
            '{\n  "hello": "world",\n  "nested": [\n    1,\n    2\n  ]\n}',
        )
        connector.ack_envelope(envelope.id)
        self.assertEqual(broker.acked, [("chatting.auxiliary-ingress.v1", "guid-1")])


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

    def fetch(self, uid: str, _payload: str):
        raw_message = self._raw_messages_by_uid[uid.encode("ascii")]
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
