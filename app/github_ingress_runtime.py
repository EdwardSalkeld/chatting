"""Runtime helpers for GitHub issue-assignment polling ingress."""

from __future__ import annotations

import json
import sqlite3
import subprocess
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Protocol, TypeVar

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

PULL_REQUEST_REVIEWS_QUERY = """
query (
  $repoOwner: String!
  $repoName: String!
  $pullRequestLimit: Int!
  $reviewLimit: Int!
) {
  repository(owner: $repoOwner, name: $repoName) {
    id
    nameWithOwner
    pullRequests(
      first: $pullRequestLimit
      states: OPEN
      orderBy: { field: UPDATED_AT, direction: DESC }
    ) {
      nodes {
        id
        number
        title
        body
        url
        author {
          login
        }
        closingIssuesReferences(first: 10) {
          nodes {
            number
            title
            url
          }
        }
        reviews(last: $reviewLimit) {
          nodes {
            id
            submittedAt
            state
            body
            url
            author {
              login
            }
            comments {
              totalCount
            }
          }
        }
      }
    }
  }
}
"""

VIEWER_LOGIN_QUERY = """
query {
  viewer {
    login
  }
}
"""

OWNER_REPOSITORIES_QUERY = """
query (
  $owner: String!
  $after: String
) {
  organization(login: $owner) {
    repositories(first: 100, after: $after, orderBy: { field: UPDATED_AT, direction: DESC }) {
      nodes {
        nameWithOwner
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
  user(login: $owner) {
    repositories(
      first: 100
      after: $after
      ownerAffiliations: OWNER
      orderBy: { field: UPDATED_AT, direction: DESC }
    ) {
      nodes {
        nameWithOwner
      }
      pageInfo {
        hasNextPage
        endCursor
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
            reply_channel=ReplyChannel(type="github", target=self.issue_url),
            dedupe_key=self.dedupe_key(),
        )


@dataclass(frozen=True)
class GitHubPullRequestReviewEvent:
    """Normalized GitHub pull request review payload."""

    event_id: str
    event_created_at: datetime
    repository_id: str
    repository_name_with_owner: str
    pull_request_id: str
    pull_request_number: int
    pull_request_title: str
    pull_request_body: str
    pull_request_url: str
    pull_request_author_login: str
    review_author_login: str
    review_state: str
    review_body: str
    review_url: str
    review_comment_count: int
    closing_issue_refs: list[str]

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("event_id is required")
        if self.event_created_at.tzinfo is None:
            raise ValueError("event_created_at must be timezone-aware")
        if not self.repository_id:
            raise ValueError("repository_id is required")
        if not self.repository_name_with_owner:
            raise ValueError("repository_name_with_owner is required")
        if not self.pull_request_id:
            raise ValueError("pull_request_id is required")
        if self.pull_request_number <= 0:
            raise ValueError("pull_request_number must be positive")
        if not self.pull_request_title:
            raise ValueError("pull_request_title is required")
        if not self.pull_request_url:
            raise ValueError("pull_request_url is required")
        if not self.pull_request_author_login:
            raise ValueError("pull_request_author_login is required")
        if not self.review_author_login:
            raise ValueError("review_author_login is required")
        if not self.review_state:
            raise ValueError("review_state is required")
        if not self.review_url:
            raise ValueError("review_url is required")
        if self.review_comment_count < 0:
            raise ValueError("review_comment_count must be non-negative")
        if any(not ref for ref in self.closing_issue_refs):
            raise ValueError("closing_issue_refs must be non-empty strings")

    def dedupe_key(self) -> str:
        return f"github-review:{self.repository_id}:{self.pull_request_id}:{self.event_id}"

    def envelope_id(self) -> str:
        return f"github-review:{self.repository_name_with_owner}:{self.pull_request_number}:{self.event_id}"

    def to_task_envelope(
        self,
        *,
        context_refs: list[str],
        policy_profile: str,
    ) -> TaskEnvelope:
        closing_issues_summary = ", ".join(self.closing_issue_refs) if self.closing_issue_refs else "(none linked)"
        review_body = self.review_body.strip() if self.review_body.strip() else "(empty)"
        pull_request_body = self.pull_request_body.strip() if self.pull_request_body.strip() else "(empty)"
        content = (
            "GitHub pull request review detected.\n\n"
            f"Repository: {self.repository_name_with_owner}\n"
            f"Pull request: #{self.pull_request_number} {self.pull_request_title}\n"
            f"Pull request URL: {self.pull_request_url}\n"
            f"Pull request author: {self.pull_request_author_login}\n"
            f"Review state: {self.review_state}\n"
            f"Review author: {self.review_author_login}\n"
            f"Review URL: {self.review_url}\n"
            f"Inline review comments: {self.review_comment_count}\n"
            f"Linked issues: {closing_issues_summary}\n"
            f"Review id: {self.event_id}\n"
            f"Reviewed at: {_serialize_utc_datetime(self.event_created_at)}\n\n"
            "Read the review and any inline comments on this pull request, then make changes or respond on GitHub as needed.\n\n"
            "Review body:\n"
            f"{review_body}\n\n"
            "Pull request body:\n"
            f"{pull_request_body}"
        )
        return TaskEnvelope(
            id=self.envelope_id(),
            source="webhook",
            received_at=self.event_created_at,
            actor=self.review_author_login,
            content=content,
            attachments=[],
            context_refs=list(context_refs),
            policy_profile=policy_profile,
            reply_channel=ReplyChannel(type="github", target=self.pull_request_url),
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

    parsed: dict[str, object] | None = None
    stdout = completed.stdout.strip()
    if stdout:
        try:
            candidate = json.loads(stdout)
        except json.JSONDecodeError:
            candidate = None
        if isinstance(candidate, dict):
            parsed = candidate

    if completed.returncode != 0 and parsed is None:
        stderr = completed.stderr.strip() or stdout
        raise RuntimeError(f"github_graphql_failed:{stderr}")
    if parsed is None:
        raise RuntimeError("github_graphql_invalid_json")
    return parsed


def fetch_authenticated_viewer_login(
    *,
    graphql_runner: Callable[[str, Mapping[str, object]], dict[str, object]],
) -> str:
    """Fetch authenticated `gh` viewer login."""
    payload = graphql_runner(VIEWER_LOGIN_QUERY, {})
    if payload.get("errors"):
        raise RuntimeError(f"github_graphql_errors:{payload['errors']}")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("github_graphql_missing_data")
    viewer = data.get("viewer")
    if not isinstance(viewer, dict):
        raise RuntimeError("github_graphql_invalid_viewer")
    return _require_non_empty_string(viewer.get("login"), field_name="viewer.login")


def list_owner_repositories(
    *,
    owner: str,
    graphql_runner: Callable[[str, Mapping[str, object]], dict[str, object]],
) -> list[str]:
    """List repositories for owner login from GitHub GraphQL."""
    if not isinstance(owner, str) or not owner.strip():
        raise ValueError("owner must be a non-empty string")

    repositories: list[str] = []
    after: str | None = None
    while True:
        variables: dict[str, object] = {"owner": owner}
        if after is not None:
            variables["after"] = after
        payload = graphql_runner(OWNER_REPOSITORIES_QUERY, variables)

        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("github_graphql_missing_data")

        owner_node = data.get("organization")
        if not isinstance(owner_node, dict):
            owner_node = data.get("user")
        if owner_node is None:
            if payload.get("errors"):
                raise RuntimeError(f"github_graphql_errors:{payload['errors']}")
            return []
        if not isinstance(owner_node, dict):
            raise RuntimeError("github_graphql_invalid_owner")

        repositories_conn = owner_node.get("repositories")
        if not isinstance(repositories_conn, dict):
            raise RuntimeError("github_graphql_invalid_owner_repositories")
        nodes = repositories_conn.get("nodes")
        if not isinstance(nodes, list):
            raise RuntimeError("github_graphql_invalid_owner_repository_nodes")
        for node in nodes:
            if not isinstance(node, dict):
                continue
            repo_slug = node.get("nameWithOwner")
            if isinstance(repo_slug, str) and repo_slug.strip():
                repositories.append(repo_slug.strip())

        page_info = repositories_conn.get("pageInfo")
        if not isinstance(page_info, dict):
            raise RuntimeError("github_graphql_invalid_owner_repositories_page_info")
        has_next_page = page_info.get("hasNextPage")
        if not isinstance(has_next_page, bool):
            raise RuntimeError("github_graphql_invalid_owner_repositories_has_next_page")
        if not has_next_page:
            break
        end_cursor = page_info.get("endCursor")
        if not isinstance(end_cursor, str) or not end_cursor.strip():
            raise RuntimeError("github_graphql_invalid_owner_repositories_end_cursor")
        after = end_cursor

    return repositories


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


def fetch_pull_request_review_events_for_repository(
    *,
    repo_owner: str,
    repo_name: str,
    author_login: str,
    pull_request_limit: int,
    review_limit: int,
    graphql_runner: Callable[[str, Mapping[str, object]], dict[str, object]],
) -> list[GitHubPullRequestReviewEvent]:
    """Fetch submitted review events for open pull requests authored by one login."""
    if pull_request_limit <= 0:
        raise ValueError("pull_request_limit must be positive")
    if review_limit <= 0:
        raise ValueError("review_limit must be positive")

    payload = graphql_runner(
        PULL_REQUEST_REVIEWS_QUERY,
        {
            "repoOwner": repo_owner,
            "repoName": repo_name,
            "pullRequestLimit": pull_request_limit,
            "reviewLimit": review_limit,
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
    pull_requests = repository.get("pullRequests")
    if not isinstance(pull_requests, dict):
        raise RuntimeError("github_graphql_invalid_pull_requests")
    pull_request_nodes = pull_requests.get("nodes")
    if not isinstance(pull_request_nodes, list):
        raise RuntimeError("github_graphql_invalid_pull_request_nodes")

    expected_author_login = author_login.casefold()
    events: list[GitHubPullRequestReviewEvent] = []
    for pull_request_node in pull_request_nodes:
        if not isinstance(pull_request_node, dict):
            continue
        author = pull_request_node.get("author")
        if not isinstance(author, dict):
            continue
        pull_request_author_login = _parse_actor_login(author)
        if pull_request_author_login is None or pull_request_author_login.casefold() != expected_author_login:
            continue

        try:
            pull_request_id = _require_non_empty_string(
                pull_request_node.get("id"),
                field_name="pull_request.id",
            )
            pull_request_number = _require_positive_int(
                pull_request_node.get("number"),
                field_name="pull_request.number",
            )
            pull_request_title = _require_non_empty_string(
                pull_request_node.get("title"),
                field_name="pull_request.title",
            )
            pull_request_url = _require_non_empty_string(
                pull_request_node.get("url"),
                field_name="pull_request.url",
            )
        except RuntimeError:
            continue
        pull_request_body_raw = pull_request_node.get("body")
        pull_request_body = pull_request_body_raw if isinstance(pull_request_body_raw, str) else ""
        closing_issue_refs = _parse_issue_reference_summaries(
            pull_request_node.get("closingIssuesReferences"),
        )

        reviews = pull_request_node.get("reviews")
        if not isinstance(reviews, dict):
            continue
        review_nodes = reviews.get("nodes")
        if not isinstance(review_nodes, list):
            continue
        for review_node in review_nodes:
            if not isinstance(review_node, dict):
                continue
            review_author_login = _parse_actor_login(review_node.get("author"))
            if review_author_login is None or review_author_login.casefold() == expected_author_login:
                continue
            submitted_at = review_node.get("submittedAt")
            if not isinstance(submitted_at, str) or not submitted_at.strip():
                continue
            try:
                event_id = _require_non_empty_string(review_node.get("id"), field_name="review.id")
                review_state = _require_non_empty_string(review_node.get("state"), field_name="review.state")
                review_url = _require_non_empty_string(review_node.get("url"), field_name="review.url")
                review_created_at = _parse_rfc3339_utc(submitted_at)
                review_comment_count = _parse_total_count(
                    review_node.get("comments"),
                    field_name="review.comments.totalCount",
                )
            except RuntimeError:
                continue
            review_body_raw = review_node.get("body")
            review_body = review_body_raw if isinstance(review_body_raw, str) else ""
            events.append(
                GitHubPullRequestReviewEvent(
                    event_id=event_id,
                    event_created_at=review_created_at,
                    repository_id=repository_id,
                    repository_name_with_owner=repo_name_with_owner,
                    pull_request_id=pull_request_id,
                    pull_request_number=pull_request_number,
                    pull_request_title=pull_request_title,
                    pull_request_body=pull_request_body,
                    pull_request_url=pull_request_url,
                    pull_request_author_login=pull_request_author_login,
                    review_author_login=review_author_login,
                    review_state=review_state,
                    review_body=review_body,
                    review_url=review_url,
                    review_comment_count=review_comment_count,
                    closing_issue_refs=closing_issue_refs,
                )
            )
    return events


def checkpoint_scope_key(
    *,
    repositories: list[str],
    assignee_login: str,
    stream_name: str = "assignments",
) -> str:
    """Build deterministic checkpoint scope for repo set + assignee."""
    if not repositories:
        raise ValueError("repositories are required")
    if not stream_name.strip():
        raise ValueError("stream_name is required")
    normalized_repos = sorted(repo.strip() for repo in repositories)
    return f"{stream_name.strip().casefold()}:{assignee_login.casefold()}:{','.join(normalized_repos)}"


def expand_repository_patterns(
    *,
    repository_patterns: list[str],
    graphql_runner: Callable[[str, Mapping[str, object]], dict[str, object]],
) -> list[str]:
    """Expand `owner/*` patterns to concrete `owner/repo` repository slugs."""
    if not repository_patterns:
        raise ValueError("repository_patterns are required")

    expanded: list[str] = []
    seen: set[str] = set()
    for pattern in repository_patterns:
        owner, repo = _parse_repository_pattern(pattern)
        if repo == "*":
            owner_repositories = list_owner_repositories(
                owner=owner,
                graphql_runner=graphql_runner,
            )
            for repository in owner_repositories:
                normalized_owner, normalized_name = parse_repo_slug(repository)
                normalized = f"{normalized_owner}/{normalized_name}"
                if normalized in seen:
                    continue
                seen.add(normalized)
                expanded.append(normalized)
            continue

        normalized = f"{owner}/{repo}"
        if normalized in seen:
            continue
        seen.add(normalized)
        expanded.append(normalized)
    return expanded


class _CheckpointedEvent(Protocol):
    event_id: str
    event_created_at: datetime


EventT = TypeVar("EventT", bound=_CheckpointedEvent)


def select_events_after_checkpoint(
    events: list[EventT],
    *,
    checkpoint: AssignmentCheckpoint | None,
) -> list[EventT]:
    """Sort events and filter to entries after checkpoint boundary."""
    unique_by_id: dict[str, EventT] = {}
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
    context_refs: list[str],
    policy_profile: str,
) -> int:
    """Publish assignment events as TaskQueueMessage payloads."""
    published_count = 0
    for event in events:
        envelope = event.to_task_envelope(
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


def _parse_repository_pattern(value: str) -> tuple[str, str]:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("repository selector must be non-empty")
    parts = value.strip().split("/", maxsplit=1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("repository selector must be owner/repo or owner/*")
    owner, repo = parts
    if repo != "*" and "*" in repo:
        raise ValueError("repository selector must be owner/repo or owner/*")
    return owner, repo


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


def _parse_issue_reference_summaries(raw_references: object) -> list[str]:
    if not isinstance(raw_references, dict):
        return []
    nodes = raw_references.get("nodes")
    if not isinstance(nodes, list):
        return []
    references: list[str] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        number = node.get("number")
        if not isinstance(number, int) or number <= 0:
            continue
        title = node.get("title")
        if isinstance(title, str) and title.strip():
            references.append(f"#{number} {title.strip()}")
        else:
            references.append(f"#{number}")
    return references


def _parse_total_count(raw_connection: object, *, field_name: str) -> int:
    if not isinstance(raw_connection, dict):
        raise RuntimeError(f"{field_name} must be an object")
    total_count = raw_connection.get("totalCount")
    if not isinstance(total_count, int) or isinstance(total_count, bool) or total_count < 0:
        raise RuntimeError(f"{field_name} must be a non-negative integer")
    return total_count


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
    "OWNER_REPOSITORIES_QUERY",
    "PULL_REQUEST_REVIEWS_QUERY",
    "AssignmentCheckpoint",
    "GitHubAssignmentCheckpointStore",
    "GitHubIssueAssignmentEvent",
    "GitHubPullRequestReviewEvent",
    "VIEWER_LOGIN_QUERY",
    "checkpoint_scope_key",
    "default_graphql_runner",
    "expand_repository_patterns",
    "fetch_assignment_events_for_repository",
    "fetch_authenticated_viewer_login",
    "fetch_pull_request_review_events_for_repository",
    "list_owner_repositories",
    "parse_repo_slug",
    "publish_assignment_events",
    "select_events_after_checkpoint",
]
