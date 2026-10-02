"""Prototype correlation of ingress events to continuing work items.

This module assigns identity only. Executors still use the existing single
working directory until workspace allocation and leases are implemented.
"""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from dataclasses import dataclass

from app.broker import TaskQueueMessage

_MESSAGE_ID = re.compile(r"<[^<>\s]+>")
_PR_URL = re.compile(r"https://github\.com/([\w.-]+/[\w.-]+)/pull/(\d+)(?:\b|/)", re.I)


@dataclass(frozen=True)
class WorkAssignment:
    work_item_id: str | None
    workspace_id: str | None
    reason: str
    candidates: tuple[str, ...] = ()


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
            work_item_id TEXT,
            conversation_id TEXT NOT NULL,
            route_reason TEXT NOT NULL,
            candidate_ids_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL
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
    if kind not in {"github_pr", "email_message_id"}:
        raise ValueError("unsupported artifact kind")
    if kind == "github_pr":
        key = normalize_pr(key)
    elif kind == "email_message_id":
        ids = _MESSAGE_ID.findall(key)
        if len(ids) != 1:
            raise ValueError("invalid email message ID")
        key = ids[0].lower()
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
    existing = connection.execute(
        "SELECT work_item_id, route_reason, candidate_ids_json FROM work_item_events WHERE task_id = ?",
        (task.task_id,),
    ).fetchone()
    if existing:
        return _result(connection, existing[0], existing[1], json.loads(existing[2]))

    envelope = task.envelope
    metadata = envelope.reply_channel.metadata
    candidate: str | None = None
    reason = "new_request"

    if envelope.reply_channel.type == "telegram":
        parent_id = metadata.get("reply_to_message_id")
        if isinstance(parent_id, int) and parent_id > 0:
            parent = connection.execute(
                """SELECT e.work_item_id FROM worker_telegram_history h
                   JOIN work_item_events e ON e.task_id = h.task_id
                   WHERE h.target = ? AND h.message_id = ?""",
                (envelope.reply_channel.target, parent_id),
            ).fetchone()
            if parent and parent[0]:
                candidate, reason = str(parent[0]), "telegram_reply"
    elif envelope.source == "email":
        references = " ".join(
            str(metadata.get(key, "")) for key in ("in_reply_to", "references")
        )
        for message_id in reversed(_MESSAGE_ID.findall(references)):
            owner = _artifact_owner(connection, "email_message_id", message_id.lower())
            if owner:
                candidate, reason = owner, "email_thread"
                break

    # A notification carries a provider artifact, never a workspace ID. Only
    # GitHub-origin mail/webhooks may use PR URLs as routing evidence.
    is_github = (
        envelope.source == "email"
        and (envelope.actor or "").lower()
        in {"notifications@github.com", "noreply@github.com"}
    ) or envelope.reply_channel.type == "github"
    if candidate is None and is_github:
        urls = {
            normalize_pr(match.group(0)) for match in _PR_URL.finditer(envelope.content)
        }
        if envelope.reply_channel.type == "github":
            try:
                urls.add(normalize_pr(envelope.reply_channel.target))
            except ValueError:
                pass
        owners = {_artifact_owner(connection, "github_pr", url) for url in urls}
        owners.discard(None)
        if len(owners) == 1:
            candidate, reason = owners.pop(), "github_pr"

    if candidate is None and envelope.reply_channel.type == "telegram":
        rows = connection.execute(
            """SELECT DISTINCT w.work_item_id FROM work_items w
               JOIN work_item_events e ON e.work_item_id = w.work_item_id
               WHERE e.conversation_id = ? AND w.state = 'open'""",
            (conversation_id,),
        ).fetchall()
        candidates = tuple(sorted(str(row[0]) for row in rows))
        if len(candidates) == 1:
            candidate, reason = candidates[0], "single_open_item_in_conversation"
        elif len(candidates) > 1:
            reason = "ambiguous_conversation"
            connection.execute(
                "INSERT INTO work_item_events VALUES (?, NULL, ?, ?, ?, ?)",
                (
                    task.task_id,
                    conversation_id,
                    reason,
                    json.dumps(candidates),
                    created_at,
                ),
            )
            return WorkAssignment(None, None, reason, candidates)

    if candidate is None:
        candidate = f"item_{uuid.uuid4().hex}"
        connection.execute(
            "INSERT INTO work_items VALUES (?, ?, ?, ?, 'open', ?)",
            (
                candidate,
                f"ws_{uuid.uuid4().hex}",
                conversation_id,
                json.dumps(
                    {
                        "type": envelope.reply_channel.type,
                        "target": envelope.reply_channel.target,
                        "metadata": envelope.reply_channel.metadata,
                    }
                ),
                created_at,
            ),
        )
    connection.execute(
        "INSERT INTO work_item_events VALUES (?, ?, ?, ?, '[]', ?)",
        (task.task_id, candidate, conversation_id, reason, created_at),
    )
    if envelope.source == "email":
        message_ids = _MESSAGE_ID.findall(str(metadata.get("message_id", "")))
        if len(message_ids) == 1:
            register_artifact(
                connection,
                work_item_id=candidate,
                kind="email_message_id",
                key=message_ids[0],
            )
    return _result(connection, candidate, reason, [])


def _artifact_owner(connection: sqlite3.Connection, kind: str, key: str) -> str | None:
    row = connection.execute(
        "SELECT work_item_id FROM work_item_artifacts WHERE kind = ? AND artifact_key = ?",
        (kind, key),
    ).fetchone()
    return str(row[0]) if row else None


def _result(
    connection: sqlite3.Connection,
    item_id: str | None,
    reason: str,
    candidates: list[str],
) -> WorkAssignment:
    if item_id is None:
        return WorkAssignment(None, None, reason, tuple(candidates))
    row = connection.execute(
        "SELECT workspace_id FROM work_items WHERE work_item_id = ?", (item_id,)
    ).fetchone()
    return WorkAssignment(item_id, str(row[0]), reason, tuple(candidates))
