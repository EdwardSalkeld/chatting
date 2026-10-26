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
            explicit_finish INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS work_item_events (
            task_id TEXT PRIMARY KEY,
            work_item_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            route_reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS work_item_session_aliases (
            work_item_id TEXT NOT NULL,
            legacy_id TEXT NOT NULL,
            PRIMARY KEY (work_item_id, legacy_id)
        );
        CREATE TABLE IF NOT EXISTS worker_schema_migrations (
            name TEXT PRIMARY KEY
        );
        """
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
    # Mirror the recorded origin as context, without changing the event channel.
    # The legacy SQL column name is retained for existing worker databases.
    reply = next(iter(task.envelope.conversation_channels), task.envelope.reply_channel)
    created = connection.execute(
        """INSERT OR IGNORE INTO work_items
           (work_item_id, origin_conversation_id, preferred_reply_json, state, model_tier, explicit_finish, created_at)
           VALUES (?, ?, ?, 'open', 'auto', 1, ?)""",
        (
            item_id,
            conversation_id,
            json.dumps(
                {"type": reply.type, "target": reply.target, "metadata": reply.metadata}
            ),
            created_at,
        ),
    )
    # Set the default only at creation; existing items without a setting keep
    # using exec until explicitly switched. Legacy unassigned messages also
    # retain their existing routing and backend.
    if created.rowcount and task.work_item_id:
        connection.execute(
            "INSERT OR IGNORE INTO conversation_executors VALUES (?, 'app-server')",
            (item_id,),
        )
    # Telegram titles can change, and older items predate the title metadata.
    # Refresh the saved label on each new message without changing its model.
    if reply.metadata.get("chat_title"):
        connection.execute(
            "UPDATE work_items SET preferred_reply_json = ? WHERE work_item_id = ?",
            (
                json.dumps(
                    {
                        "type": reply.type,
                        "target": reply.target,
                        "metadata": reply.metadata,
                    }
                ),
                item_id,
            ),
        )
    connection.execute(
        """INSERT INTO work_item_events
           (task_id, work_item_id, conversation_id, route_reason, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (task.task_id, item_id, conversation_id, reason, created_at),
    )
    return _result(connection, item_id, reason)


def migrate_identity(connection: sqlite3.Connection) -> None:
    """Keep the old origin as a session migration hint; canonicalize inbox IDs.

    Legacy broker messages without a handler assignment retain their old route.
    Executor settings follow the originating conversation, never the transport
    of a later webhook/email. Reopening the database is idempotent.
    """
    migration = "work_item_identity_v1"
    if connection.execute(
        "SELECT 1 FROM worker_schema_migrations WHERE name = ?", (migration,)
    ).fetchone():
        return
    connection.execute(
        """INSERT OR IGNORE INTO work_item_session_aliases
           SELECT work_item_id, conversation_id FROM work_item_events
           WHERE work_item_id != 'item_legacy_general' AND conversation_id LIKE 'conv_%'
           UNION SELECT work_item_id, conversation_id FROM worker_inbox
           WHERE work_item_id IS NOT NULL AND work_item_id != 'item_legacy_general'
             AND conversation_id LIKE 'conv_%'"""
    )
    connection.execute(
        """INSERT OR IGNORE INTO conversation_executors (conversation_id, backend)
           SELECT item.work_item_id, mode.backend FROM work_items AS item
           JOIN conversation_executors AS mode
             ON mode.conversation_id = item.origin_conversation_id
           WHERE item.work_item_id != 'item_legacy_general'"""
    )
    connection.execute(
        """INSERT OR IGNORE INTO conversation_executors (conversation_id, backend)
           SELECT DISTINCT alias.work_item_id, mode.backend FROM work_item_session_aliases AS alias
           JOIN conversation_executors AS mode ON mode.conversation_id = alias.legacy_id
           WHERE mode.backend = 'app-server'"""
    )
    connection.execute(
        """UPDATE worker_inbox SET conversation_id = work_item_id
           WHERE work_item_id IS NOT NULL AND work_item_id != 'item_legacy_general'
             AND conversation_id != work_item_id"""
    )
    connection.execute(
        """UPDATE work_item_events SET conversation_id = work_item_id
           WHERE work_item_id != 'item_legacy_general'
             AND conversation_id != work_item_id"""
    )
    connection.execute("INSERT INTO worker_schema_migrations VALUES (?)", (migration,))


def _result(
    connection: sqlite3.Connection, item_id: str, reason: str
) -> WorkAssignment:
    return WorkAssignment(item_id, reason)
