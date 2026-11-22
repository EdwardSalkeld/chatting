"""Web conversation history owned by the worker, alongside Telegram history."""

from __future__ import annotations

import base64
import json
import sqlite3
from datetime import datetime
from typing import Any

from app.broker import TaskQueueMessage
from app.timefmt import format_utc


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS worker_web_messages (
            id TEXT PRIMARY KEY,
            conversation_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            task_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS worker_web_messages_conversation
        ON worker_web_messages(conversation_id, occurred_at, id);
    """)


def record_inbound(connection: sqlite3.Connection, task: TaskQueueMessage) -> None:
    channel = task.envelope.reply_channel
    if channel.type != "web":
        return
    connection.execute(
        "INSERT OR IGNORE INTO worker_web_messages VALUES (?, ?, 'user', ?, ?, ?)",
        (
            "user:" + task.task_id,
            channel.target,
            task.envelope.content,
            task.task_id,
            format_utc(task.envelope.received_at),
        ),
    )


def record_reply(
    connection: sqlite3.Connection,
    *,
    target: str,
    content: str,
    task_id: str,
    event_id: str,
    occurred_at: datetime,
) -> None:
    row = connection.execute(
        "SELECT task_payload_json FROM worker_inbox WHERE task_id=?", (task_id,)
    ).fetchone()
    if row is None:
        raise ValueError("web reply requires a known worker task")
    task = TaskQueueMessage.from_dict(json.loads(row[0]))
    if (
        task.envelope.reply_channel.type != "web"
        or task.envelope.reply_channel.target != target
    ):
        raise ValueError("web reply destination does not match the task")
    connection.execute(
        "INSERT OR IGNORE INTO worker_web_messages VALUES (?, ?, 'assistant', ?, ?, ?)",
        (
            "reply:" + task_id + ":" + event_id,
            target,
            content,
            task_id,
            format_utc(occurred_at),
        ),
    )


def scope(conversation_id: str) -> tuple[str, int | None]:
    try:
        data = json.loads(
            base64.urlsafe_b64decode(
                conversation_id + "=" * (-len(conversation_id) % 4)
            )
        )
        target, topic = data["chat"], data["topic"]
        if (
            not isinstance(target, str)
            or not target
            or (topic is not None and (type(topic) is not int or topic <= 0))
        ):
            raise ValueError
        return target, topic
    except (ValueError, KeyError, TypeError) as error:
        raise ValueError("invalid conversation ID") from error


def snapshot(
    connection: sqlite3.Connection,
    *,
    conversation_id: str,
    before: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    target, topic = scope(conversation_id)
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    cutoff, anchor = "9999", ""
    if before:
        try:
            cutoff, anchor = json.loads(
                base64.urlsafe_b64decode(before + "=" * (-len(before) % 4))
            )
            if not isinstance(cutoff, str) or not isinstance(anchor, str):
                raise ValueError
        except (ValueError, TypeError) as error:
            raise ValueError("invalid history cursor") from error
    rows = connection.execute(
        """
        WITH messages AS (
          SELECT 'telegram:' || history_id AS id, role, content, sender,
                 attachments_json, task_id, occurred_at, 'telegram' AS transport
          FROM worker_telegram_history WHERE target=? AND topic_id IS ?
          UNION ALL
          SELECT id, role, content, CASE WHEN role='user' THEN 'You' ELSE 'Billy' END,
                 '[]', task_id, occurred_at, 'web'
          FROM worker_web_messages WHERE conversation_id=?
        )
        SELECT m.*, i.state AS task_state, i.reply_delivered_at,
          EXISTS(SELECT 1 FROM worker_web_messages reply
                 WHERE reply.task_id=m.task_id AND reply.role='assistant') AS answered
        FROM messages m LEFT JOIN worker_inbox i ON i.task_id=m.task_id
        WHERE m.occurred_at < ? OR (m.occurred_at = ? AND m.id < ?)
        ORDER BY m.occurred_at DESC, m.id DESC LIMIT ?
        """,
        (target, topic, conversation_id, cutoff, cutoff, anchor, limit + 1),
    ).fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    result = []
    for row in reversed(rows):
        status = "delivered"
        if row["transport"] == "web" and row["role"] == "user":
            status = (
                "answered"
                if row["answered"]
                else "queued"
                if row["task_state"] in ("pending", "attached")
                else "working"
                if row["task_state"] in ("active", "closing")
                else "failed"
            )
        result.append(
            {
                "id": row["id"],
                "role": row["role"],
                "content": row["content"],
                "sender": row["sender"],
                "attachments": json.loads(row["attachments_json"]),
                "task_id": row["task_id"],
                "occurred_at": row["occurred_at"],
                "transport": row["transport"],
                "status": status,
            }
        )
    cursor = None
    if more and rows:
        last = rows[-1]
        cursor = (
            base64.urlsafe_b64encode(
                json.dumps([last["occurred_at"], last["id"]]).encode()
            )
            .decode()
            .rstrip("=")
        )
    return {"messages": result, "next_cursor": cursor}
