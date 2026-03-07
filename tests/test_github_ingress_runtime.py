import tempfile
import unittest
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from app.github_ingress_runtime import (
    AssignmentCheckpoint,
    GitHubAssignmentCheckpointStore,
    GitHubIssueAssignmentEvent,
    checkpoint_scope_key,
    fetch_assignment_events_for_repository,
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
                reply_channel_type="telegram",
                reply_channel_target="8605042448",
                context_refs=["repo:/home/edward/chatting"],
                policy_profile="default",
            )
            second = publish_assignment_events(
                events=[event],
                store=store,
                broker=broker,  # type: ignore[arg-type]
                reply_channel_type="telegram",
                reply_channel_target="8605042448",
                context_refs=["repo:/home/edward/chatting"],
                policy_profile="default",
            )

            self.assertEqual(first, 1)
            self.assertEqual(second, 0)
            self.assertEqual(len(broker.published), 1)
            queue_name, payload = broker.published[0]
            self.assertEqual(queue_name, "chatting.tasks.v1")
            self.assertEqual(payload["task_id"], "task:github-assignment:brokensbone/chatting:12:AE_1")

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


if __name__ == "__main__":
    unittest.main()
