"""Project Codex usage notifications into an idempotent, unpriced ledger."""

from __future__ import annotations

import hashlib
import json
import sqlite3


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS model_token_usage (
            usage_key TEXT PRIMARY KEY,
            activity_id INTEGER NOT NULL,
            occurred_at TEXT NOT NULL,
            task_id TEXT,
            work_item_id TEXT,
            run_id TEXT,
            attempt INTEGER,
            thread_id TEXT NOT NULL,
            turn_id TEXT NOT NULL,
            model TEXT,
            model_source TEXT,
            model_context_window INTEGER,
            input_tokens INTEGER NOT NULL,
            cached_input_tokens INTEGER,
            cache_write_input_tokens INTEGER,
            output_tokens INTEGER NOT NULL,
            reasoning_output_tokens INTEGER,
            total_tokens INTEGER,
            cumulative_json TEXT NOT NULL,
            usage_source TEXT NOT NULL DEFAULT 'codex_app_server'
        );
        CREATE INDEX IF NOT EXISTS model_token_usage_time
            ON model_token_usage(occurred_at);
        CREATE INDEX IF NOT EXISTS model_token_usage_task
            ON model_token_usage(task_id);
        CREATE TABLE IF NOT EXISTS model_usage_context (
            task_id TEXT PRIMARY KEY,
            attempt INTEGER,
            model TEXT,
            model_source TEXT
        );
        CREATE TABLE IF NOT EXISTS personal_model_calls (
            call_id TEXT PRIMARY KEY,
            occurred_at TEXT NOT NULL,
            task_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            model TEXT,
            status TEXT NOT NULL,
            metered_turns INTEGER NOT NULL
        );
    """)


def _object(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _count(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def project(connection: sqlite3.Connection, event: sqlite3.Row) -> None:
    """Called in the activity insert transaction, and by the bounded backfill."""
    task_id = event["task_id"]
    detail = _object(json.loads(event["detail_json"]))
    phase = event["phase"]
    if phase == "personal_model_usage":
        call_id = detail["call_id"]
        usages = detail["usage"]
        connection.execute(
            "INSERT OR IGNORE INTO personal_model_calls VALUES (?,?,?,?,?,?,?)",
            (
                call_id,
                event["occurred_at"],
                task_id,
                detail["kind"],
                detail["model"],
                detail["status"],
                len(usages),
            ),
        )
        for index, usage in enumerate(usages):
            connection.execute(
                """INSERT OR IGNORE INTO model_token_usage
                (usage_key, activity_id, occurred_at, task_id, thread_id, turn_id,
                 model, model_source, input_tokens, cached_input_tokens,
                 output_tokens, reasoning_output_tokens, cumulative_json, usage_source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    f"personal:{call_id}:{index}",
                    event["activity_id"],
                    event["occurred_at"],
                    task_id,
                    call_id,
                    str(index),
                    detail["model"],
                    "cli_request",
                    usage["input_tokens"],
                    usage.get("cached_input_tokens"),
                    usage["output_tokens"],
                    usage.get("reasoning_output_tokens"),
                    "{}",
                    "codex_exec_personal",
                ),
            )
        return
    if phase == "executor_started":
        connection.execute(
            "INSERT OR REPLACE INTO model_usage_context(task_id, attempt) VALUES (?, ?)",
            (task_id, _count(detail.get("attempt"))),
        )
        return
    if phase == "task_finished":
        connection.execute(
            "UPDATE model_token_usage SET run_id = ? WHERE task_id = ?",
            (event["run_id"], task_id),
        )
        return
    if phase != "executor_stdout":
        return
    content = detail.get("content")
    if not isinstance(content, str):
        return
    for line in content.splitlines():
        try:
            message = _object(json.loads(line))
        except ValueError, TypeError:
            continue
        method = message.get("method")
        params = _object(message.get("params"))
        model = None
        model_source = None
        if method == "chatting/modelSelected":
            model, model_source = params.get("model"), "turn_request"
        elif method == "model/rerouted":
            model, model_source = params.get("toModel"), "rerouted"
        elif method == "item/started":
            # Historical captures include the submitted envelope as a user item.
            item = _object(params.get("item"))
            if item.get("type") == "userMessage":
                blocks = item.get("content", [])
                for block in blocks if isinstance(blocks, list) else []:
                    text = _object(block).get("text")
                    try:
                        payload = json.loads(text) if isinstance(text, str) else {}
                        if isinstance(payload, str):
                            payload = json.loads(payload)
                    except ValueError:
                        continue
                    task = _object(_object(payload).get("task"))
                    selection = _object(task.get("model_selection"))
                    if task.get("task_id") == task_id:
                        model, model_source = selection.get("model"), "task_envelope"
        if isinstance(model, str) and model:
            connection.execute(
                """INSERT INTO model_usage_context(task_id, model, model_source)
                   VALUES (?, ?, ?) ON CONFLICT(task_id) DO UPDATE
                   SET model = excluded.model, model_source = excluded.model_source
                   WHERE excluded.model_source != 'task_envelope'
                      OR model_usage_context.model_source IS NULL""",
                (task_id, model, model_source),
            )
        if method != "thread/tokenUsage/updated":
            continue
        usage = _object(params.get("tokenUsage"))
        last, total = _object(usage.get("last")), _object(usage.get("total"))
        thread_id, turn_id = params.get("threadId"), params.get("turnId")
        if not all(isinstance(v, str) and v for v in (thread_id, turn_id)):
            continue
        if any(_count(last.get(k)) is None for k in ("inputTokens", "outputTokens")):
            continue
        if not total or any(_count(v) is None for v in total.values()):
            continue
        cumulative = json.dumps(total, sort_keys=True)
        # turnId covers many model responses. Cumulative counters distinguish
        # equal-sized responses, while repeated snapshots are counted once.
        key = hashlib.sha256(
            json.dumps([thread_id, turn_id, total], sort_keys=True).encode()
        ).hexdigest()
        context = connection.execute(
            "SELECT attempt, model, model_source FROM model_usage_context WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        connection.execute(
            """INSERT OR IGNORE INTO model_token_usage VALUES
               (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                key,
                event["activity_id"],
                event["occurred_at"],
                task_id,
                event["work_item_id"],
                event["run_id"],
                context[0] if context else None,
                thread_id,
                turn_id,
                context[1] if context else None,
                context[2] if context else None,
                _count(usage.get("modelContextWindow")),
                *(
                    _count(last.get(k))
                    for k in (
                        "inputTokens",
                        "cachedInputTokens",
                        "cacheWriteInputTokens",
                        "outputTokens",
                        "reasoningOutputTokens",
                        "totalTokens",
                    )
                ),
                cumulative,
                "codex_app_server",
            ),
        )
