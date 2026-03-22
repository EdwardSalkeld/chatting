"""Compatibility package for handler connectors."""

from pathlib import Path

__path__.append(str(Path(__file__).resolve().parent.parent / "handler" / "connectors"))

from app.connectors.base import Connector
from app.connectors.github_issue_assignment_connector import GitHubIssueAssignmentConnector
from app.connectors.github_pull_request_review_connector import GitHubPullRequestReviewConnector
from app.connectors.imap_email_connector import ImapEmailConnector
from app.connectors.internal_heartbeat_connector import InternalHeartbeatConnector
from app.connectors.interval_schedule_connector import (
    IntervalScheduleConnector,
    IntervalScheduleJob,
)
from app.connectors.slack_connector import SlackConnector
from app.connectors.telegram_connector import TelegramConnector
from app.connectors.webhook_connector import WebhookConnector, WebhookEvent

__all__ = [
    "Connector",
    "GitHubIssueAssignmentConnector",
    "GitHubPullRequestReviewConnector",
    "ImapEmailConnector",
    "InternalHeartbeatConnector",
    "IntervalScheduleConnector",
    "IntervalScheduleJob",
    "SlackConnector",
    "TelegramConnector",
    "WebhookConnector",
    "WebhookEvent",
]
