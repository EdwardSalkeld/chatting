import tempfile
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from app.handler.github_ingress import (
    AssignmentCheckpoint,
    GitHubAssignmentCheckpointStore,
    GitHubIssueAssignmentEvent,
    GitHubPullRequestReviewEvent,
    checkpoint_scope_key,
    default_graphql_runner,
    expand_repository_patterns,
    fetch_assignment_events_for_repository,
    fetch_authenticated_viewer_login,
    fetch_pull_request_review_events_for_repository,
    list_owner_repositories,
    parse_repo_slug,
    publish_assignment_events,
    select_events_after_checkpoint,
)
from app.state import SQLiteStateStore
@dataclass
class _RecordingBroker:
    published: list[tuple[str, dict[str, object]]] = field(default_factory=list)

    def publish_json(self, queue_name: str, payload: dict[str, object]) -> None:
        self.published.append((queue_name, payload))
class GitHubIngressRuntimeTests(unittest.TestCase):
    def test_fetch_assignment_events_for_repository_filters_by_assignee_login(self) -> None:
        payload = {
            "data": {
                "repository": {
                    "id": "R_123",
                    "nameWithOwner": "brokensbone/chatting",
                    "issues": {
                        "nodes": [
                            {
                                "id": "I_1",
                                "number": 12,
                                "title": "Plan milestone 5",
                                "body": "Body text",
                                "url": "https://github.com/brokensbone/chatting/issues/12",
                                "labels": {"nodes": [{"name": "enhancement"}, {"name": "ai"}]},
                                "timelineItems": {
                                    "nodes": [
                                        {
                                            "id": "AE_1",
                                            "createdAt": "2026-03-07T10:47:35Z",
                                            "actor": {"login": "BillyAcachofa"},
                                            "assignee": {"__typename": "User", "login": "BillyAcachofa"},
                                        },
                                        {
                                            "id": "AE_2",
                                            "createdAt": "2026-03-07T10:49:35Z",
                                            "actor": {"login": "BillyAcachofa"},
                                            "assignee": {"__typename": "User", "login": "someoneelse"},
                                        },
                                    ]
                                },
                            }
                        ]
                    },
                }
            }
        }

        events = fetch_assignment_events_for_repository(
            repo_owner="brokensbone",
            repo_name="chatting",
            assignee_login="billyacachofa",
            issue_limit=20,
            timeline_limit=10,
            graphql_runner=lambda _query, _variables: payload,
        )

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.event_id, "AE_1")
        self.assertEqual(event.repository_name_with_owner, "brokensbone/chatting")
        self.assertEqual(event.labels, ["enhancement", "ai"])

    def test_fetch_pull_request_review_events_for_repository_filters_by_author_login(self) -> None:
        payload = {
            "data": {
                "repository": {
                    "id": "R_123",
                    "nameWithOwner": "brokensbone/chatting",
                    "pullRequests": {
                        "nodes": [
                            {
                                "id": "PR_1",
                                "number": 60,
                                "title": "Add ingress from GitHub reviews",
                                "body": "Implements the review ingress flow.",
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
                                            "body": "Please wire this into ingress.",
                                            "url": "https://github.com/brokensbone/chatting/pull/60#pullrequestreview-1",
                                            "author": {"login": "brokensbone"},
                                            "comments": {"totalCount": 2},
                                        },
                                        {
                                            "id": "PRR_2",
                                            "submittedAt": "2026-03-07T12:05:00Z",
                                            "state": "COMMENTED",
                                            "body": "Self review should be ignored.",
                                            "url": "https://github.com/brokensbone/chatting/pull/60#pullrequestreview-2",
                                            "author": {"login": "BillyAcachofa"},
                                            "comments": {"totalCount": 0},
                                        },
                                    ]
                                },
                            },
                            {
                                "id": "PR_2",
                                "number": 61,
                                "title": "Other author's PR",
                                "body": "",
                                "url": "https://github.com/brokensbone/chatting/pull/61",
                                "author": {"login": "someoneelse"},
                                "closingIssuesReferences": {"nodes": []},
                                "reviews": {
                                    "nodes": [
                                        {
                                            "id": "PRR_3",
                                            "submittedAt": "2026-03-07T12:10:00Z",
                                            "state": "COMMENTED",
                                            "body": "Should not be picked up.",
                                            "url": "https://github.com/brokensbone/chatting/pull/61#pullrequestreview-3",
                                            "author": {"login": "brokensbone"},
                                            "comments": {"totalCount": 1},
                                        }
                                    ]
                                },
                            },
                        ]
                    },
                }
            }
        }

        events = fetch_pull_request_review_events_for_repository(
            repo_owner="brokensbone",
            repo_name="chatting",
            author_login="billyacachofa",
            pull_request_limit=20,
            review_limit=10,
            graphql_runner=lambda _query, _variables: payload,
        )

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.event_id, "PRR_1")
        self.assertEqual(event.repository_name_with_owner, "brokensbone/chatting")
        self.assertEqual(event.pull_request_number, 60)
        self.assertEqual(event.closing_issue_refs, ["#60 Add ingress from GitHub reviews"])

    def test_select_events_after_checkpoint_filters_boundary(self) -> None:
        older = GitHubIssueAssignmentEvent(
            event_id="AE_1",
            event_created_at=datetime(2026, 3, 7, 10, 47, 35, tzinfo=timezone.utc),
            repository_id="R_1",
            repository_name_with_owner="brokensbone/chatting",
            issue_id="I_1",
            issue_number=12,
            issue_title="Title",
            issue_body="Body",
            issue_url="https://example.com/1",
            assignee_login="BillyAcachofa",
            actor_login="edward",
            labels=[],
        )
        same_time_newer_id = GitHubIssueAssignmentEvent(
            event_id="AE_2",
            event_created_at=datetime(2026, 3, 7, 10, 47, 35, tzinfo=timezone.utc),
            repository_id="R_1",
            repository_name_with_owner="brokensbone/chatting",
            issue_id="I_2",
            issue_number=13,
            issue_title="Title",
            issue_body="Body",
            issue_url="https://example.com/2",
            assignee_login="BillyAcachofa",
            actor_login="edward",
            labels=[],
        )

        selected = select_events_after_checkpoint(
            [same_time_newer_id, older],
            checkpoint=AssignmentCheckpoint(
                event_created_at=older.event_created_at,
                event_id=older.event_id,
            ),
        )

        self.assertEqual([event.event_id for event in selected], ["AE_2"])

    def test_select_events_after_checkpoint_supports_pull_request_review_events(self) -> None:
        older = GitHubPullRequestReviewEvent(
            event_id="PRR_1",
            event_created_at=datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc),
            repository_id="R_1",
            repository_name_with_owner="brokensbone/chatting",
            pull_request_id="PR_1",
            pull_request_number=60,
            pull_request_title="Add ingress from GitHub reviews",
            pull_request_body="PR body",
            pull_request_url="https://example.com/pr/60",
            pull_request_author_login="BillyAcachofa",
            review_author_login="brokensbone",
            review_state="CHANGES_REQUESTED",
            review_body="Please fix this",
            review_url="https://example.com/pr/60#review-1",
            review_comment_count=1,
            closing_issue_refs=["#60 Add ingress from GitHub reviews"],
        )
        newer = GitHubPullRequestReviewEvent(
            event_id="PRR_2",
            event_created_at=datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc),
            repository_id="R_1",
            repository_name_with_owner="brokensbone/chatting",
            pull_request_id="PR_1",
            pull_request_number=60,
            pull_request_title="Add ingress from GitHub reviews",
            pull_request_body="PR body",
            pull_request_url="https://example.com/pr/60",
            pull_request_author_login="BillyAcachofa",
            review_author_login="brokensbone",
            review_state="COMMENTED",
            review_body="Another note",
            review_url="https://example.com/pr/60#review-2",
            review_comment_count=0,
            closing_issue_refs=["#60 Add ingress from GitHub reviews"],
        )

        selected = select_events_after_checkpoint(
            [newer, older],
            checkpoint=AssignmentCheckpoint(
                event_created_at=older.event_created_at,
                event_id=older.event_id,
            ),
        )

        self.assertEqual([event.event_id for event in selected], ["PRR_2"])

    def test_publish_assignment_events_emits_task_messages_and_dedupes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            store = SQLiteStateStore(db_path)
            broker = _RecordingBroker()
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

            first = publish_assignment_events(
                events=[event],
                store=store,
                broker=broker,  # type: ignore[arg-type]
                context_refs=["repo:/home/edward/chatting"],
            )
            second = publish_assignment_events(
                events=[event],
                store=store,
                broker=broker,  # type: ignore[arg-type]
                context_refs=["repo:/home/edward/chatting"],
            )

            self.assertEqual(first, 1)
            self.assertEqual(second, 0)
            self.assertEqual(len(broker.published), 1)
            queue_name, payload = broker.published[0]
            self.assertEqual(queue_name, "chatting.tasks.v1")
            self.assertEqual(payload["task_id"], "task:github-assignment:brokensbone/chatting:12:AE_1")
            self.assertEqual(
                payload["envelope"]["reply_channel"],
                {
                    "type": "github",
                    "target": "https://github.com/brokensbone/chatting/issues/12",
                },
            )

    def test_checkpoint_store_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")
            checkpoint_store = GitHubAssignmentCheckpointStore(db_path)
            checkpoint = AssignmentCheckpoint(
                event_created_at=datetime(2026, 3, 7, 10, 47, 35, tzinfo=timezone.utc),
                event_id="AE_1",
            )
            scope = checkpoint_scope_key(
                repositories=["brokensbone/chatting", "brokensbone/bbmb"],
                assignee_login="BillyAcachofa",
            )

            checkpoint_store.set_checkpoint(scope, checkpoint)
            loaded = checkpoint_store.get_checkpoint(scope)

            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.event_id, "AE_1")
            self.assertEqual(
                loaded.event_created_at,
                datetime(2026, 3, 7, 10, 47, 35, tzinfo=timezone.utc),
            )

    def test_parse_repo_slug_requires_owner_repo(self) -> None:
        with self.assertRaisesRegex(ValueError, "owner/repo"):
            parse_repo_slug("brokensbone")

    def test_fetch_authenticated_viewer_login_parses_login(self) -> None:
        login = fetch_authenticated_viewer_login(
            graphql_runner=lambda _query, _variables: {
                "data": {
                    "viewer": {
                        "login": "BillyAcachofa",
                    }
                }
            }
        )
        self.assertEqual(login, "BillyAcachofa")

    def test_expand_repository_patterns_supports_owner_wildcard(self) -> None:
        calls: list[dict[str, object]] = []

        def _graphql_runner(_query: str, variables: dict[str, object]) -> dict[str, object]:
            calls.append(dict(variables))
            if variables.get("after") is None:
                return {
                    "data": {
                        "organization": {
                            "repositories": {
                                "nodes": [{"nameWithOwner": "brokensbone/chatting"}],
                                "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                            }
                        },
                        "user": None,
                    }
                }
            return {
                "data": {
                    "organization": {
                        "repositories": {
                            "nodes": [{"nameWithOwner": "brokensbone/bbmb"}],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    },
                    "user": None,
                }
            }

        repositories = expand_repository_patterns(
            repository_patterns=["brokensbone/*", "brokensbone/chatting"],
            graphql_runner=_graphql_runner,
        )

        self.assertEqual(repositories, ["brokensbone/chatting", "brokensbone/bbmb"])
        self.assertEqual(
            calls,
            [
                {"owner": "brokensbone"},
                {"owner": "brokensbone", "after": "cursor-1"},
            ],
        )

    def test_expand_repository_patterns_rejects_partial_wildcard(self) -> None:
        with self.assertRaisesRegex(ValueError, "owner/repo or owner/\\*"):
            expand_repository_patterns(
                repository_patterns=["brokensbone/chat*"],
                graphql_runner=lambda _query, _variables: {"data": {}},
            )

    def test_list_owner_repositories_handles_partial_not_found_error(self) -> None:
        repositories = list_owner_repositories(
            owner="brokensbone",
            graphql_runner=lambda _query, _variables: {
                "data": {
                    "organization": None,
                    "user": {
                        "repositories": {
                            "nodes": [{"nameWithOwner": "brokensbone/chatting"}],
                            "pageInfo": {"hasNextPage": False, "endCursor": None},
                        }
                    },
                },
                "errors": [
                    {
                        "type": "NOT_FOUND",
                        "path": ["organization"],
                        "message": "Could not resolve to an Organization",
                    }
                ],
            },
        )
        self.assertEqual(repositories, ["brokensbone/chatting"])

    def test_default_graphql_runner_parses_json_when_gh_exits_non_zero(self) -> None:
        with patch(
            "subprocess.run",
            return_value=CompletedProcess(
                args=["gh", "api", "graphql"],
                returncode=1,
                stdout='{"data":{"viewer":{"login":"BillyAcachofa"}},"errors":[{"path":["organization"]}]}',
                stderr="gh: Could not resolve to an Organization",
            ),
        ):
            payload = default_graphql_runner("query { viewer { login } }", {})
        self.assertIn("data", payload)
if __name__ == "__main__":
    unittest.main()
