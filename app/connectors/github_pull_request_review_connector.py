"""GitHub pull request review polling connector."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Mapping

from app.github_ingress_runtime import (
    AssignmentCheckpoint,
    GitHubAssignmentCheckpointStore,
    GitHubPullRequestReviewEvent,
    checkpoint_scope_key,
    default_graphql_runner,
    expand_repository_patterns,
    fetch_pull_request_review_events_for_repository,
    parse_repo_slug,
    select_events_after_checkpoint,
)
from app.models import TaskEnvelope

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class GitHubPullRequestReviewConnector:
    """Poll GitHub pull request reviews and normalize them into task envelopes."""

    repository_patterns: list[str]
    author_login: str
    context_refs: list[str]
    checkpoint_store: GitHubAssignmentCheckpointStore
    policy_profile: str = "default"
    max_pull_requests: int = 25
    max_reviews: int = 10
    graphql_runner: Callable[[str, Mapping[str, object]], dict[str, object]] = default_graphql_runner

    def __post_init__(self) -> None:
        if not self.repository_patterns:
            raise ValueError("repository_patterns are required")
        if not self.author_login:
            raise ValueError("author_login is required")
        if self.max_pull_requests <= 0:
            raise ValueError("max_pull_requests must be positive")
        if self.max_reviews <= 0:
            raise ValueError("max_reviews must be positive")
        object.__setattr__(self, "_pending_checkpoint", None)
        object.__setattr__(self, "_last_poll_scanned_events", 0)
        object.__setattr__(self, "_last_poll_new_events", 0)
        object.__setattr__(self, "_last_poll_checkpoint_id", "disabled")

    @property
    def last_poll_scanned_events(self) -> int:
        return self._last_poll_scanned_events

    @property
    def last_poll_new_events(self) -> int:
        return self._last_poll_new_events

    @property
    def last_poll_checkpoint_id(self) -> str:
        return self._last_poll_checkpoint_id

    def poll(self) -> list[TaskEnvelope]:
        self._flush_pending_checkpoint()

        scope_key = checkpoint_scope_key(
            repositories=self.repository_patterns,
            assignee_login=self.author_login,
            stream_name="pull-request-reviews",
        )
        checkpoint = self.checkpoint_store.get_checkpoint(scope_key)
        repositories = expand_repository_patterns(
            repository_patterns=self.repository_patterns,
            graphql_runner=self.graphql_runner,
        )

        events: list[GitHubPullRequestReviewEvent] = []
        scanned_event_count = 0
        for repository in repositories:
            owner, name = parse_repo_slug(repository)
            try:
                repository_events = fetch_pull_request_review_events_for_repository(
                    repo_owner=owner,
                    repo_name=name,
                    author_login=self.author_login,
                    pull_request_limit=self.max_pull_requests,
                    review_limit=self.max_reviews,
                    graphql_runner=self.graphql_runner,
                )
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "github_pull_request_review_poll_failed repository=%s author=%s",
                    repository,
                    self.author_login,
                )
                continue
            scanned_event_count += len(repository_events)
            events.extend(repository_events)

        new_events = select_events_after_checkpoint(events, checkpoint=checkpoint)
        object.__setattr__(self, "_last_poll_scanned_events", scanned_event_count)
        object.__setattr__(self, "_last_poll_new_events", len(new_events))

        checkpoint_id = checkpoint.event_id if checkpoint else "none"
        if new_events:
            latest = new_events[-1]
            object.__setattr__(
                self,
                "_pending_checkpoint",
                AssignmentCheckpoint(
                    event_created_at=latest.event_created_at,
                    event_id=latest.event_id,
                ),
            )
            checkpoint_id = latest.event_id
        object.__setattr__(self, "_last_poll_checkpoint_id", checkpoint_id)

        return [
            event.to_task_envelope(
                context_refs=self.context_refs,
                policy_profile=self.policy_profile,
            )
            for event in new_events
        ]

    def _flush_pending_checkpoint(self) -> None:
        pending_checkpoint = self._pending_checkpoint
        if pending_checkpoint is None:
            return
        self.checkpoint_store.set_checkpoint(
            checkpoint_scope_key(
                repositories=self.repository_patterns,
                assignee_login=self.author_login,
                stream_name="pull-request-reviews",
            ),
            checkpoint=pending_checkpoint,
        )
        object.__setattr__(self, "_pending_checkpoint", None)


__all__ = ["GitHubPullRequestReviewConnector"]
