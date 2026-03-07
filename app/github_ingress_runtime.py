"""Runtime helpers for GitHub issue-assignment polling ingress."""

from __future__ import annotations

import json
import sqlite3
import subprocess
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping

from app.broker import TASK_QUEUE_NAME, BBMBQueueAdapter, TaskQueueMessage
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore

ASSIGNED_EVENTS_QUERY = """
query (
  $repoOwner: String!
  $repoName: String!
  $issueLimit: Int!
  $timelineLimit: Int!
) {
  repository(owner: $repoOwner, name: $repoName) {
    id
    nameWithOwner
    issues(first: $issueLimit, orderBy: { field: UPDATED_AT, direction: DESC }) {
      nodes {
        id
        number
        title
        body
        url
        labels(first: 20) {
          nodes {
            name
          }
        }
        timelineItems(last: $timelineLimit, itemTypes: [ASSIGNED_EVENT]) {
          nodes {
            ... on AssignedEvent {
              id
              createdAt
              actor {
                login
              }
              assignee {
                __typename
                ... on User {
                  login
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


@dataclass(frozen=True)
class AssignmentCheckpoint:
    """Checkpoint tuple used to avoid re-emitting old assignment events."""

    event_created_at: datetime
    event_id: str

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("event_id is required")
        if self.event_created_at.tzinfo is None:
            raise ValueError("event_created_at must be timezone-aware")


@dataclass(frozen=True)
class GitHubIssueAssignmentEvent:
    """Normalized GitHub AssignedEvent payload."""

    event_id: str
    event_created_at: datetime
    repository_id: str
    repository_name_with_owner: str
    issue_id: str
    issue_number: int
    issue_title: str
    issue_body: str
    issue_url: str
    assignee_login: str
    actor_login: str | None
    labels: list[str]

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("event_id is required")
        if self.event_created_at.tzinfo is None:
            raise ValueError("event_created_at must be timezone-aware")
        if not self.repository_id:
            raise ValueError("repository_id is required")
        if not self.repository_name_with_owner:
            raise ValueError("repository_name_with_owner is required")
        if not self.issue_id:
            raise ValueError("issue_id is required")
        if self.issue_number <= 0:
            raise ValueError("issue_number must be positive")
        if not self.issue_title:
            raise ValueError("issue_title is required")
        if not self.issue_url:
            raise ValueError("issue_url is required")
        if not self.assignee_login:
            raise ValueError("assignee_login is required")
        if any(not label for label in self.labels):
            raise ValueError("labels must be non-empty strings")

    def dedupe_key(self) -> str:
        return f"github:{self.repository_id}:{self.issue_id}:{self.event_id}"

    def envelope_id(self) -> str:
        return f"github-assignment:{self.repository_name_with_owner}:{self.issue_number}:{self.event_id}"

    def to_task_envelope(
        self,
        *,
        reply_channel_type: str,
        reply_channel_target: str,
        context_refs: list[str],
        policy_profile: str,
    ) -> TaskEnvelope:
        labels_summary = ", ".join(self.labels) if self.labels else "(none)"
        actor = self.actor_login if self.actor_login else "unknown"
        content = (
            "GitHub issue assignment detected.\n\n"
            f"Repository: {self.repository_name_with_owner}\n"
            f"Issue: #{self.issue_number} {self.issue_title}\n"
            f"URL: {self.issue_url}\n"
            f"Assigned to: {self.assignee_login}\n"
            f"Assigned by: {actor}\n"
            f"Labels: {labels_summary}\n"
            f"Assigned event id: {self.event_id}\n"
            f"Assigned at: {_serialize_utc_datetime(self.event_created_at)}\n\n"
            "Issue body:\n"
            f"{self.issue_body.strip() if self.issue_body.strip() else '(empty)'}"
        )
        return TaskEnvelope(
            id=self.envelope_id(),
            source="webhook",
            received_at=self.event_created_at,
            actor=self.actor_login,
            content=content,
            attachments=[],
            context_refs=list(context_refs),
            policy_profile=policy_profile,
            reply_channel=ReplyChannel(type=reply_channel_type, target=reply_channel_target),
            dedupe_key=self.dedupe_key(),
        )


class GitHubAssignmentCheckpointStore:
    """SQLite-backed checkpoint storage for assignment polling progress."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS github_assignment_checkpoints (
                    scope_key TEXT PRIMARY KEY,
                    event_created_at TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.commit()

    def get_checkpoint(self, scope_key: str) -> AssignmentCheckpoint | None:
        if not scope_key:
            raise ValueError("scope_key is required")
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT event_created_at, event_id
                FROM github_assignment_checkpoints
                WHERE scope_key = ?
                """,
                (scope_key,),
            ).fetchone()
        if row is None:
            return None
        return AssignmentCheckpoint(
            event_created_at=_parse_rfc3339_utc(row["event_created_at"]),
            event_id=row["event_id"],
        )

    def set_checkpoint(self, scope_key: str, checkpoint: AssignmentCheckpoint) -> None:
        if not scope_key:
            raise ValueError("scope_key is required")
        now = _serialize_utc_datetime(datetime.now(timezone.utc))
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO github_assignment_checkpoints (
                    scope_key,
                    event_created_at,
                    event_id,
                    updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(scope_key) DO UPDATE SET
                    event_created_at = excluded.event_created_at,
                    event_id = excluded.event_id,
                    updated_at = excluded.updated_at
                """,
                (
                    scope_key,
                    _serialize_utc_datetime(checkpoint.event_created_at),
                    checkpoint.event_id,
                    now,
                ),
            )
            connection.commit()


def default_graphql_runner(query: str, variables: Mapping[str, object]) -> dict[str, object]:
    """Invoke `gh api graphql` and parse JSON response."""
    command: list[str] = ["gh", "api", "graphql", "-f", f"query={query}"]
    for key, value in sorted(variables.items()):
        if isinstance(value, bool):
            command.extend(["-F", f"{key}={'true' if value else 'false'}"])
            continue
        if isinstance(value, int):
            command.extend(["-F", f"{key}={value}"])
            continue
        command.extend(["-f", f"{key}={value}"])

    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"github_graphql_failed:{stderr}")
    try:
        parsed = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("github_graphql_invalid_json") from error
    if not isinstance(parsed, dict):
        raise RuntimeError("github_graphql_invalid_shape")
    return parsed


def fetch_assignment_events_for_repository(
    *,
    repo_owner: str,
    repo_name: str,
    assignee_login: str,
    issue_limit: int,
    timeline_limit: int,
    graphql_runner: Callable[[str, Mapping[str, object]], dict[str, object]],
) -> list[GitHubIssueAssignmentEvent]:
    """Fetch and normalize AssignedEvent items for one repository."""
    if issue_limit <= 0:
        raise ValueError("issue_limit must be positive")
    if timeline_limit <= 0:
        raise ValueError("timeline_limit must be positive")

    payload = graphql_runner(
        ASSIGNED_EVENTS_QUERY,
        {
            "repoOwner": repo_owner,
            "repoName": repo_name,
            "issueLimit": issue_limit,
            "timelineLimit": timeline_limit,
        },
    )
    if payload.get("errors"):
        raise RuntimeError(f"github_graphql_errors:{payload['errors']}")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("github_graphql_missing_data")
    repository = data.get("repository")
    if repository is None:
        return []
    if not isinstance(repository, dict):
        raise RuntimeError("github_graphql_invalid_repository")

    repository_id = _require_non_empty_string(repository.get("id"), field_name="repository.id")
    repo_name_with_owner = _require_non_empty_string(
        repository.get("nameWithOwner"),
        field_name="repository.nameWithOwner",
    )
    issues = repository.get("issues")
    if not isinstance(issues, dict):
        raise RuntimeError("github_graphql_invalid_issues")
    issue_nodes = issues.get("nodes")
    if not isinstance(issue_nodes, list):
        raise RuntimeError("github_graphql_invalid_issue_nodes")

    expected_assignee_login = assignee_login.casefold()
    events: list[GitHubIssueAssignmentEvent] = []
    for issue_node in issue_nodes:
        if not isinstance(issue_node, dict):
            continue
        try:
            issue_id = _require_non_empty_string(issue_node.get("id"), field_name="issue.id")
            issue_number = _require_positive_int(issue_node.get("number"), field_name="issue.number")
            issue_title = _require_non_empty_string(issue_node.get("title"), field_name="issue.title")
            issue_url = _require_non_empty_string(issue_node.get("url"), field_name="issue.url")
        except RuntimeError:
            continue
        issue_body_raw = issue_node.get("body")
        issue_body = issue_body_raw if isinstance(issue_body_raw, str) else ""
        labels = _parse_labels(issue_node.get("labels"))

        timeline_items = issue_node.get("timelineItems")
        if not isinstance(timeline_items, dict):
            continue
        timeline_nodes = timeline_items.get("nodes")
        if not isinstance(timeline_nodes, list):
            continue
        for timeline_node in timeline_nodes:
            if not isinstance(timeline_node, dict):
                continue
            assignee = timeline_node.get("assignee")
            if not isinstance(assignee, dict):
                continue
            if assignee.get("__typename") != "User":
                continue
            assignee_node_login = assignee.get("login")
            if not isinstance(assignee_node_login, str):
                continue
            if assignee_node_login.casefold() != expected_assignee_login:
                continue

            actor_login = _parse_actor_login(timeline_node.get("actor"))
            try:
                event_id = _require_non_empty_string(timeline_node.get("id"), field_name="assigned_event.id")
                created_at = _parse_rfc3339_utc(
                    _require_non_empty_string(
                        timeline_node.get("createdAt"),
                        field_name="assigned_event.createdAt",
                    )
                )
            except RuntimeError:
                continue
            events.append(
                GitHubIssueAssignmentEvent(
                    event_id=event_id,
                    event_created_at=created_at,
                    repository_id=repository_id,
                    repository_name_with_owner=repo_name_with_owner,
                    issue_id=issue_id,
                    issue_number=issue_number,
                    issue_title=issue_title,
                    issue_body=issue_body,
                    issue_url=issue_url,
                    assignee_login=assignee_node_login,
                    actor_login=actor_login,
                    labels=labels,
                )
            )
    return events


def checkpoint_scope_key(*, repositories: list[str], assignee_login: str) -> str:
    """Build deterministic checkpoint scope for repo set + assignee."""
    if not repositories:
        raise ValueError("repositories are required")
    normalized_repos = sorted(repo.strip() for repo in repositories)
    return f"{assignee_login.casefold()}:{','.join(normalized_repos)}"


def select_events_after_checkpoint(
    events: list[GitHubIssueAssignmentEvent],
    *,
    checkpoint: AssignmentCheckpoint | None,
) -> list[GitHubIssueAssignmentEvent]:
    """Sort events and filter to entries after checkpoint boundary."""
    unique_by_id: dict[str, GitHubIssueAssignmentEvent] = {}
    for event in events:
        unique_by_id[event.event_id] = event

    ordered_events = sorted(
        unique_by_id.values(),
        key=lambda event: (_serialize_utc_datetime(event.event_created_at), event.event_id),
    )
    if checkpoint is None:
        return ordered_events
    boundary = (_serialize_utc_datetime(checkpoint.event_created_at), checkpoint.event_id)
    return [
        event
        for event in ordered_events
        if (_serialize_utc_datetime(event.event_created_at), event.event_id) > boundary
    ]


def publish_assignment_events(
    *,
    events: list[GitHubIssueAssignmentEvent],
    store: SQLiteStateStore,
    broker: BBMBQueueAdapter,
    reply_channel_type: str,
    reply_channel_target: str,
    context_refs: list[str],
    policy_profile: str,
) -> int:
    """Publish assignment events as TaskQueueMessage payloads."""
    published_count = 0
    for event in events:
        envelope = event.to_task_envelope(
            reply_channel_type=reply_channel_type,
            reply_channel_target=reply_channel_target,
            context_refs=context_refs,
            policy_profile=policy_profile,
        )
        if store.seen(envelope.source, envelope.dedupe_key):
            continue
        task_message = TaskQueueMessage.from_envelope(
            envelope,
            trace_id=f"trace:{envelope.id}",
        )
        broker.publish_json(TASK_QUEUE_NAME, task_message.to_dict())
        store.mark_seen(envelope.source, envelope.dedupe_key)
        published_count += 1
    return published_count


def parse_repo_slug(value: str) -> tuple[str, str]:
    """Parse `owner/repo` slug string."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("repository slug must be non-empty")
    parts = value.strip().split("/", maxsplit=1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("repository slug must be owner/repo")
    return parts[0], parts[1]


def _parse_labels(raw_labels: object) -> list[str]:
    if not isinstance(raw_labels, dict):
        return []
    label_nodes = raw_labels.get("nodes")
    if not isinstance(label_nodes, list):
        return []
    labels: list[str] = []
    for node in label_nodes:
        if not isinstance(node, dict):
            continue
        name = node.get("name")
        if isinstance(name, str) and name.strip():
            labels.append(name.strip())
    return labels


def _parse_actor_login(raw_actor: object) -> str | None:
    if not isinstance(raw_actor, dict):
        return None
    login = raw_actor.get("login")
    if not isinstance(login, str) or not login.strip():
        return None
    return login


def _require_non_empty_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"{field_name} must be a non-empty string")
    return value


def _require_positive_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise RuntimeError(f"{field_name} must be a positive integer")
    return value


def _parse_rfc3339_utc(raw_value: str) -> datetime:
    parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise RuntimeError("datetime must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _serialize_utc_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "ASSIGNED_EVENTS_QUERY",
    "AssignmentCheckpoint",
    "GitHubAssignmentCheckpointStore",
    "GitHubIssueAssignmentEvent",
    "checkpoint_scope_key",
    "default_graphql_runner",
    "fetch_assignment_events_for_repository",
    "parse_repo_slug",
    "publish_assignment_events",
    "select_events_after_checkpoint",
]
