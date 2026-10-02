"""Worker-side compatibility routing for tasks without handler lane IDs."""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from dataclasses import dataclass

from app.broker import TaskQueueMessage

_PR_URL = re.compile(r"https://github\.com/([\w.-]+/[\w.-]+)/pull/(\d+)(?:\b|/)", re.I)


@dataclass(frozen=True)
class WorkAssignment:
    work_item_id: str
    workspace_id: str
    reason: str


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS work_items (
            work_item_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL UNIQUE,
            origin_conversation_id TEXT NOT NULL,
            preferred_reply_json TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS work_item_events (
            task_id TEXT PRIMARY KEY,
            work_item_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            route_reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS work_item_routes (
            route_kind TEXT NOT NULL,
            route_key TEXT NOT NULL,
            work_item_id TEXT NOT NULL,
            PRIMARY KEY (route_kind, route_key)
        );
        CREATE TABLE IF NOT EXISTS work_item_artifacts (
            kind TEXT NOT NULL,
            artifact_key TEXT NOT NULL,
            work_item_id TEXT NOT NULL,
            PRIMARY KEY (kind, artifact_key)
        );
        CREATE INDEX IF NOT EXISTS work_item_events_conversation
            ON work_item_events (conversation_id, work_item_id);
        """
    )


def register_artifact(
    connection: sqlite3.Connection, *, work_item_id: str, kind: str, key: str
) -> None:
    if (
        connection.execute(
            "SELECT 1 FROM work_items WHERE work_item_id = ?", (work_item_id,)
        ).fetchone()
        is None
    ):
        raise KeyError(work_item_id)
    if kind != "github_pr":
        raise ValueError("unsupported artifact kind")
    key = normalize_pr(key)
    if not key:
        raise ValueError("artifact key is required")
    owner = connection.execute(
        "SELECT work_item_id FROM work_item_artifacts WHERE kind = ? AND artifact_key = ?",
        (kind, key),
    ).fetchone()
    if owner and owner[0] != work_item_id:
        raise ValueError("artifact already belongs to another work item")
    connection.execute(
        "INSERT OR IGNORE INTO work_item_artifacts VALUES (?, ?, ?)",
        (kind, key, work_item_id),
    )


def normalize_pr(value: str) -> str:
    match = _PR_URL.search(value)
    if not match:
        raise ValueError("invalid GitHub PR URL")
    return f"{match.group(1).lower()}#{int(match.group(2))}"


def assign(
    connection: sqlite3.Connection,
    *,
    task: TaskQueueMessage,
    conversation_id: str,
    created_at: str,
) -> WorkAssignment:
    return WorkItemRouter(connection).assign(
        task=task, conversation_id=conversation_id, created_at=created_at
    )


class WorkItemRouter:
    """Mirror the handler policy only for older task messages without lane IDs."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def assign(
        self, *, task: TaskQueueMessage, conversation_id: str, created_at: str
    ) -> WorkAssignment:
        row = self.connection.execute(
            "SELECT work_item_id, route_reason FROM work_item_events WHERE task_id = ?",
            (task.task_id,),
        ).fetchone()
        if row:
            return _result(self.connection, str(row[0]), str(row[1]))

        envelope = task.envelope
        reply = envelope.reply_channel
        if task.work_item_id and task.workspace_id:
            self.connection.execute(
                """INSERT OR IGNORE INTO work_items
                   VALUES (?, ?, ?, ?, 'open', ?)""",
                (
                    task.work_item_id,
                    task.workspace_id,
                    conversation_id,
                    json.dumps(
                        {
                            "type": reply.type,
                            "target": reply.target,
                            "metadata": reply.metadata,
                        }
                    ),
                    created_at,
                ),
            )
            self._record_event(
                task.task_id,
                task.work_item_id,
                conversation_id,
                "handler_assignment",
                created_at,
            )
            return _result(self.connection, task.work_item_id, "handler_assignment")
        # Direct Telegram ingress always belongs to its chat/topic lane.
        if envelope.source == "im" and reply.type == "telegram":
            route_kind = "telegram"
            route_key = json.dumps(
                {
                    "chat": reply.target,
                    "topic": reply.metadata.get("message_thread_id"),
                },
                sort_keys=True,
            )
            reason = "telegram_channel"
        else:
            route_kind, route_key, reason = "general", "default", "general_lane"
            owner = self._artifact_match(task)
            if owner is not None:
                self._record_event(
                    task.task_id, owner, conversation_id, "github_pr", created_at
                )
                return _result(self.connection, owner, "github_pr")

        row = self.connection.execute(
            "SELECT work_item_id FROM work_item_routes WHERE route_kind = ? AND route_key = ?",
            (route_kind, route_key),
        ).fetchone()
        if row:
            item_id = str(row[0])
        else:
            item_id = f"item_{uuid.uuid4().hex}"
            self.connection.execute(
                "INSERT INTO work_items VALUES (?, ?, ?, ?, 'open', ?)",
                (
                    item_id,
                    f"ws_{uuid.uuid4().hex}",
                    conversation_id,
                    json.dumps(
                        {
                            "type": reply.type,
                            "target": reply.target,
                            "metadata": reply.metadata,
                        }
                    ),
                    created_at,
                ),
            )
            self.connection.execute(
                "INSERT INTO work_item_routes VALUES (?, ?, ?)",
                (route_kind, route_key, item_id),
            )
        self._record_event(task.task_id, item_id, conversation_id, reason, created_at)
        return _result(self.connection, item_id, reason)

    def _record_event(
        self,
        task_id: str,
        item_id: str,
        conversation_id: str,
        reason: str,
        created_at: str,
    ) -> None:
        self.connection.execute(
            """INSERT INTO work_item_events
               (task_id, work_item_id, conversation_id, route_reason, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (task_id, item_id, conversation_id, reason, created_at),
        )

    def _artifact_match(self, task: TaskQueueMessage) -> str | None:
        envelope = task.envelope
        is_github = (
            envelope.source == "email"
            and (envelope.actor or "").lower()
            in {"notifications@github.com", "noreply@github.com"}
        ) or envelope.reply_channel.type == "github"
        if not is_github:
            return None
        urls = {
            normalize_pr(match.group(0)) for match in _PR_URL.finditer(envelope.content)
        }
        if envelope.reply_channel.type == "github":
            try:
                urls.add(normalize_pr(envelope.reply_channel.target))
            except ValueError:
                pass
        owners = {_artifact_owner(self.connection, "github_pr", url) for url in urls}
        owners.discard(None)
        # Conflicting evidence is safer in the general lane.
        return owners.pop() if len(owners) == 1 else None


def _artifact_owner(connection: sqlite3.Connection, kind: str, key: str) -> str | None:
    row = connection.execute(
        "SELECT work_item_id FROM work_item_artifacts WHERE kind = ? AND artifact_key = ?",
        (kind, key),
    ).fetchone()
    return str(row[0]) if row else None


def _result(
    connection: sqlite3.Connection,
    item_id: str,
    reason: str,
) -> WorkAssignment:
    row = connection.execute(
        "SELECT workspace_id FROM work_items WHERE work_item_id = ?", (item_id,)
    ).fetchone()
    return WorkAssignment(item_id, str(row[0]), reason)
