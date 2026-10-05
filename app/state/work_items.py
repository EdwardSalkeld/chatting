"""Persist handler lane assignments in the worker database."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from app.broker import TaskQueueMessage


@dataclass(frozen=True)
class WorkAssignment:
    work_item_id: str
    reason: str


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS work_items (
            work_item_id TEXT PRIMARY KEY,
            origin_conversation_id TEXT NOT NULL,
            preferred_reply_json TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'open',
            model_tier TEXT NOT NULL DEFAULT 'auto',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS work_item_events (
            task_id TEXT PRIMARY KEY,
            work_item_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            route_reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )
    columns = {row[1] for row in connection.execute("PRAGMA table_info(work_items)")}
    if "model_tier" not in columns:
        connection.execute(
            "ALTER TABLE work_items ADD COLUMN model_tier TEXT NOT NULL DEFAULT 'auto'"
        )


def assign(
    connection: sqlite3.Connection,
    *,
    task: TaskQueueMessage,
    conversation_id: str,
    created_at: str,
) -> WorkAssignment:
    """Mirror the handler's assignment, or use one lane for legacy messages."""
    row = connection.execute(
        "SELECT work_item_id, route_reason FROM work_item_events WHERE task_id = ?",
        (task.task_id,),
    ).fetchone()
    if row:
        return _result(connection, str(row[0]), str(row[1]))

    item_id = task.work_item_id or "item_legacy_general"
    reason = "handler_assignment" if task.work_item_id else "legacy_general"
    reply = task.envelope.reply_channel
    connection.execute(
        """INSERT OR IGNORE INTO work_items
           (work_item_id, origin_conversation_id, preferred_reply_json, state, model_tier, created_at)
           VALUES (?, ?, ?, 'open', 'auto', ?)""",
        (
            item_id,
            conversation_id,
            json.dumps(
                {"type": reply.type, "target": reply.target, "metadata": reply.metadata}
            ),
            created_at,
        ),
    )
    connection.execute(
        """INSERT INTO work_item_events
           (task_id, work_item_id, conversation_id, route_reason, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (task.task_id, item_id, conversation_id, reason, created_at),
    )
    return _result(connection, item_id, reason)


def _result(
    connection: sqlite3.Connection, item_id: str, reason: str
) -> WorkAssignment:
    return WorkAssignment(item_id, reason)
