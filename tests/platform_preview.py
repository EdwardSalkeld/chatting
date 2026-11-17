"""Disposable worker activity fixture for platform browser QA.

Run with PYTHONPATH=. uv run python tests/platform_preview.py --state-dir /tmp/…
No executor, broker or outbound delivery is started. Only activity is simulated;
HTTP responses come from the real worker monitor and SQLite store. --rich adds
two weeks of varied runs, failures, dead letters and live executors.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event
from typing import Literal

from app.broker import EgressQueueMessage, TaskQueueMessage
from app import typesafe
from app.model_command import MODELS
from app.models import (
    AuditEvent,
    OutboundMessage,
    ReplyChannel,
    RunRecord,
    TaskEnvelope,
)
from app.state import SQLiteStateStore
from app.timefmt import format_utc
from app.worker.activity import WorkerActivityMonitor, start_worker_activity_server


def task(index: int, title: str, content: str, at: datetime) -> TaskQueueMessage:
    envelope = TaskEnvelope(
        id=f"platform-fixture:{index}",
        source="im",
        received_at=at,
        actor="preview",
        content=content,
        attachments=[],
        context_refs=[],
        reply_channel=ReplyChannel(
            type="telegram", target="fixture-chat", metadata={"chat_title": title}
        ),
        dedupe_key=f"platform-fixture:{index}",
    )
    return replace(
        TaskQueueMessage.from_envelope(envelope, trace_id=f"preview:{index}"),
        work_item_id=f"item_fixture_{index}",
    )


def finish(store, monitor, message, status, created_at):
    run = RunRecord(
        run_id=f"run:fixture:{message.task_id}",
        envelope_id=message.envelope.id,
        source="im",
        workflow="default",
        latency_ms=42300,
        result_status=status,
        created_at=created_at,
        work_item_id=message.work_item_id,
    )
    store.append_run(run)
    store.append_audit_event(
        AuditEvent(
            run_id=run.run_id,
            envelope_id=run.envelope_id,
            source=run.source,
            workflow=run.workflow,
            result_status=status,
            detail={"task_id": message.task_id, "attempt_count": 1, "reason_codes": []},
            created_at=created_at,
        )
    )
    monitor.record_executor_finished(
        task_message=message,
        run_id=run.run_id,
        result_status=status,
        attempt_count=1,
        reason_codes=[],
        latency_ms=42300,
    )
    with sqlite3.connect(store.db_path) as db:
        db.execute(
            "UPDATE worker_inbox SET state='completed' WHERE task_id=?",
            (message.task_id,),
        )


class Clock:
    """Monitor clock that can be pinned while seeding historical activity."""

    def __init__(self) -> None:
        self.at: datetime | None = None

    def __call__(self) -> datetime:
        return self.at or datetime.now(timezone.utc)


Status = Literal["success", "execution_error", "dead_letter"]

# Job names and reminder ids match the handler fixture's --rich seed, so cron
# and reminder fires can be joined to their definitions.
CRON_JOBS = [
    (
        "Morning briefing",
        8,
        0,
        10,
        "Bring together today’s priorities, open pull requests, and anything that needs attention.",
    ),
    (
        "Evening inbox digest",
        18,
        30,
        14,
        "Summarise today’s newsletters and share the highlights worth reading.",
    ),
    (
        "Sentry triage",
        11,
        30,
        20,
        "Triage new Sentry issues since the last run and flag regressions.",
    ),
    (
        "Backup verification",
        3,
        15,
        14,
        "Verify last night’s SQLite backups restore cleanly and report sizes.",
    ),
    (
        "CI flake report",
        6,
        0,
        20,
        "List CI jobs that failed then passed on retry in the last six hours.",
    ),
    (
        "Dependency audit",
        6,
        0,
        2,
        "Check Go, Python and npm dependencies for security advisories.",
    ),
    (
        "Weekly repository review",
        16,
        0,
        2,
        "Review the week’s changes and prepare a short summary of progress and outstanding work.",
    ),
]
REMINDER_FIRES = [
    ("rem_rich12", 1, "Chase the Hetzner invoice query"),
    ("rem_rich13", 2, "Renew the staging TLS certificate"),
    ("rem_rich14", 1, "Check whether the reminder backfill finished"),
    ("rem_rich15", 1, "Send Sam the Friday review notes"),
    ("rem_rich16", 3, "Rotate the Telegram bot token"),
    ("rem_rich17", 1, "Look at the egress drop metrics again"),
    ("rem_rich18", 1, "Book the dentist"),
    ("rem_rich19", 2, "Review the open dependabot PRs"),
    ("rem_rich20", 1, "Confirm the worker disk cleanup ran"),
    ("rem_rich21", 1, "Prepare the quarterly cost summary"),
    ("rem_rich22", 1, "Follow up on the JMAP rate limit ticket"),
    ("rem_rich23", 1, "Archive last month’s inbox digests"),
]
IM_REQUESTS = [
    (
        "Ops",
        "Can you check why the nightly deploy failed and open a fix PR if it’s obvious?",
    ),
    ("Chatting dev", "Summarise the open PRs on chatting and flag anything stale."),
    ("Chatting dev", "Bump the Go toolchain to 1.25.4 and run the handler tests."),
    ("Ops", "What’s in today’s Sentry digest that I should care about?"),
    ("Personal", "Draft a reply to Sam about moving the Friday review to 3pm."),
    ("Chatting dev", "Rebase platform-ui-iteration on main and fix any conflicts."),
    ("Chatting dev", "TestReminderLate is flaky again. Find out why and fix it."),
    ("Ops", "Compare worker queue latency this week against last week."),
    (
        "Chatting dev",
        "Find where egress is dropped for disallowed channels and add a metric.",
    ),
    ("Chatting dev", "Write a migration that backfills work_item_id on audit_events."),
    (
        "Ops",
        "Check disk usage on the worker box and clean attachments older than 30 days.",
    ),
    (
        "Chatting dev",
        "Why are app-server sessions being rerouted to a different model?",
    ),
    ("Ops", "Pull the last 50 dead letters and group them by reason."),
    ("Chatting dev", "Review PR #391 and leave comments on anything risky."),
    ("Personal", "Find the cheapest train to Bristol on Thursday morning."),
    ("Chatting dev", "Update the README with the new platform URL and screenshots."),
    ("Ops", "Why did the morning briefing post twice yesterday?"),
    (
        "Chatting dev",
        "Add a regression test for the cron next-run calculation across DST.",
    ),
    ("Personal", "Summarise the three long newsletters I starred this week."),
    (
        "Ops",
        "Is the JMAP connector still hitting rate limits? Check the last 24h of logs.",
    ),
    ("Chatting dev", "Port the reminder UI table to the new platform design."),
    ("Ops", "Restart the worker if the queue has been stuck for more than 10 minutes."),
]
IM_TOPICS = [
    "Nightly deploy",
    "PR sweep",
    "Go toolchain bump",
    "Sentry digest",
    "Friday review reply",
    "Rebase platform UI",
    "Flaky reminder test",
    "Queue latency",
    "Egress drop metric",
    "Audit backfill",
    "Worker disk cleanup",
    "Model reroutes",
    "Dead letter review",
    "PR #391 review",
    "Bristol train",
    "README refresh",
    "Duplicate briefing",
    "Cron DST test",
    "Newsletter summaries",
    "JMAP rate limits",
    "Reminder table port",
    "Stuck queue watch",
]
EMAIL_REQUESTS = [
    (
        "Invoice INV-2291 from Hetzner",
        "Please review the attached invoice and confirm the server line items match last month.",
    ),
    (
        "Your GitHub Actions usage report",
        "You have used 82% of included Actions minutes for this billing cycle.",
    ),
    (
        "Re: Platform review on Friday",
        "Works for me. Can you send the agenda and the latest screenshots beforehand?",
    ),
    (
        "Security advisory: golang.org/x/net",
        "A moderate severity advisory affects versions before 0.38.0.",
    ),
    (
        "Order 114-2287 dispatched",
        "Your order has been dispatched and will arrive on Wednesday.",
    ),
]
INTERNAL_REQUESTS = [
    "Supervised recovery: the previous attempt produced no visible reply. Send one.",
]
COMMANDS: list[tuple[str, str, int]] = [
    (
        "git status --short",
        " M app/worker/activity.py\n?? platform/src/pages/Runs.tsx",
        0,
    ),
    (
        'rg -n "list_runs" app',
        "app/state/sqlite_store.py:940:    def list_runs(self) -> list[RunRecord]:\napp/worker/activity.py:284:    def list_runs(",
        0,
    ),
    (
        "uv run pytest tests/test_worker_activity.py -q",
        "........................ [100%]\n24 passed in 3.12s",
        0,
    ),
    (
        "go test ./internal/reminders/...",
        "ok  \tgithub.com/EdwardSalkeld/chatting/go/handler/internal/reminders\t0.412s",
        0,
    ),
    (
        "gh pr list --state open --limit 20",
        "398\tAdd unified private platform interface\tplatform-ui\tOPEN\n391\tReminder revisions\treminder-revisions\tOPEN",
        0,
    ),
    (
        "npm --prefix platform run check",
        "> platform@0.0.0 check\n> tsc -b && eslint .",
        0,
    ),
    (
        "uv run pytest tests/test_reminders.py -q",
        "F.......\nFAILED tests/test_reminders.py::test_late_fire - AssertionError: 3 != 2\n1 failed, 7 passed in 1.84s",
        1,
    ),
    (
        "go vet ./...",
        "# github.com/EdwardSalkeld/chatting/go/handler/internal/platform\ninternal/platform/platform.go:212:2: unreachable code",
        1,
    ),
    (
        "curl -sf http://127.0.0.1:9465/activity.json | jq .queue",
        '{\n  "queued": 2,\n  "running": 1\n}',
        0,
    ),
    (
        "df -h /var/lib/chatting",
        "Filesystem  Size  Used Avail Use% Mounted on\n/dev/vdb     40G   31G  9.0G  78% /var/lib/chatting",
        0,
    ),
    (
        "gh run view 18234 --log-failed",
        "build\tRun go test ./...\t--- FAIL: TestScheduleNextRun (0.00s)\n    schedule_test.go:88: next run = 09:00, want 08:00",
        1,
    ),
    (
        "git push origin HEAD",
        "To github.com:EdwardSalkeld/chatting.git\n   4f2c1ab..9e01d77  HEAD -> platform-ui-iteration",
        0,
    ),
    ("ls /workspace/attachments | wc -l", "1284", 0),
    ("pnpm install", "bash: line 1: pnpm: command not found", 127),
    ("sqlite3 /var/lib/chatting/worker.db 'select count(*) from dead_letters'", "7", 0),
]
REPLIES = [
    "Done. The deploy failed on a missing migration; PR #402 adds it and CI is green.",
    "Three PRs are open. #391 has been idle for nine days and needs a rebase; the other two are ready for review.",
    "Nothing urgent: two new issues, both from the JMAP connector timing out, already rate-limited upstream.",
    "I’ve drafted the reply and left it in your drafts folder for a final read.",
    "The test is order-dependent: it shares a clock with TestReminderDue. I’ve isolated it and pushed a fix.",
    "Median queue latency is 41s this week against 38s last week; p95 went from 3m10s to 4m02s.",
    "All backups restored cleanly. handler.db 18.2 MB, worker.db 412 MB.",
    "Cleaned 1,102 attachments (6.4 GB). Disk is now at 61%.",
    "Two flaky jobs: go-handler-race (3 retries) and platform-e2e (1 retry).",
]
STDERR = [
    "2026-11-12T08:14:03.112Z ERROR codex_core::exec: exec error: Sandbox(Timeout)",
    "Error: stream error: unexpected status 502 Bad Gateway; retrying 1/5 in 213ms…",
    "thread 'tokio-runtime-worker' panicked at codex-rs/core/src/client.rs:417:22:\ncalled `Result::unwrap()` on an `Err` value: Os { code: 32, kind: BrokenPipe }",
    "WARN codex_core::config: ignoring unknown key `approval_mode` in config.toml",
]
FAILED_ATTEMPTS = [
    "TimeoutError: executor exceeded 1200s deadline",
    "RuntimeError: turn/start: {'code': -32603, 'message': 'model overloaded'}",
    "BrokenPipeError: [Errno 32] Broken pipe",
    "CalledProcessError: Command '['codex', 'exec', '--json']' returned non-zero exit status 137.",
]


def reminder_fire_time(now: datetime, index: int) -> datetime:
    """Matches reminderFireTime in the handler fixture."""
    return (now - timedelta(days=12 - index)).replace(
        hour=9 + index % 8, minute=0, second=0, microsecond=0
    )


def rich_task(
    envelope_id: str,
    source: Literal["cron", "email", "im", "internal", "reminder"],
    content: str,
    at: datetime,
    reply: ReplyChannel,
    work_item_id: str | None,
) -> TaskQueueMessage:
    envelope = TaskEnvelope(
        id=envelope_id,
        source=source,
        received_at=at,
        actor="ed" if source == "im" else None,
        content=content,
        attachments=[],
        context_refs=["repo:/workspace/chatting"],
        reply_channel=reply,
        dedupe_key=envelope_id,
    )
    message = replace(
        TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:{envelope_id}"),
        emitted_at=at,
    )
    return replace(message, work_item_id=work_item_id) if work_item_id else message


def line(payload: dict[str, object]) -> str:
    return json.dumps(payload)


def app_server_turn(
    rng: random.Random,
    *,
    model: str,
    thread: str,
    turn: str,
    commands: list[tuple[str, str, int]],
    reply: str | None,
    totals: dict[str, int],
    rerouted_to: str | None = None,
    error: str | None = None,
) -> list[str]:
    lines = [
        line({"method": "chatting/modelSelected", "params": {"model": model}}),
        line(
            {
                "method": "turn/started",
                "params": {
                    "threadId": thread,
                    "turn": {"id": turn, "status": "inProgress", "items": []},
                },
            }
        ),
    ]
    if rerouted_to:
        lines.append(
            line(
                {
                    "method": "model/rerouted",
                    "params": {
                        "threadId": thread,
                        "turnId": turn,
                        "fromModel": model,
                        "toModel": rerouted_to,
                        "reason": "highRiskCyberActivity",
                    },
                }
            )
        )

    def usage() -> str:
        last = {
            "inputTokens": rng.randint(9_000, 64_000),
            "outputTokens": rng.randint(150, 2_800),
            "reasoningOutputTokens": rng.randint(0, 1_900),
        }
        last["cachedInputTokens"] = int(last["inputTokens"] * rng.uniform(0.55, 0.92))
        last["totalTokens"] = last["inputTokens"] + last["outputTokens"]
        for key, value in last.items():
            totals[key] = totals.get(key, 0) + value
        return line(
            {
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": thread,
                    "turnId": turn,
                    "tokenUsage": {
                        "last": last,
                        "total": dict(totals),
                        "modelContextWindow": 272_000,
                    },
                },
            }
        )

    for index, (command, output, exit_code) in enumerate(commands):
        item_id = f"call_{turn}_{index}"
        lines.append(
            line(
                {
                    "method": "item/started",
                    "params": {
                        "threadId": thread,
                        "turnId": turn,
                        "item": {
                            "type": "commandExecution",
                            "id": item_id,
                            "command": command,
                            "cwd": "/workspace/chatting",
                            "status": "inProgress",
                        },
                    },
                }
            )
        )
        lines.append(
            line(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": thread,
                        "turnId": turn,
                        "item": {
                            "type": "commandExecution",
                            "id": item_id,
                            "command": command,
                            "cwd": "/workspace/chatting",
                            "status": "completed" if exit_code == 0 else "failed",
                            "exitCode": exit_code,
                            "durationMs": rng.randint(40, 48_000),
                            "aggregatedOutput": output,
                        },
                    },
                }
            )
        )
        lines.append(usage())
    if error:
        lines.append(
            line(
                {
                    "method": "error",
                    "params": {
                        "threadId": thread,
                        "turnId": turn,
                        "willRetry": False,
                        "error": {
                            "message": error,
                            "codexErrorInfo": "responseStreamDisconnected",
                        },
                    },
                }
            )
        )
    if reply:
        lines.append(
            line(
                {
                    "method": "item/completed",
                    "params": {
                        "threadId": thread,
                        "turnId": turn,
                        "item": {
                            "type": "agentMessage",
                            "id": f"msg_{turn}",
                            "text": reply,
                        },
                    },
                }
            )
        )
        lines.append(usage())
    lines.append(
        line(
            {
                "method": "turn/completed",
                "params": {
                    "threadId": thread,
                    "turn": {
                        "id": turn,
                        "status": "failed" if error else "completed",
                        "items": [],
                    },
                },
            }
        )
    )
    return lines


def exec_turn(
    rng: random.Random,
    *,
    thread: str,
    commands: list[tuple[str, str, int]],
    reply: str | None,
    error: str | None = None,
) -> list[str]:
    lines = [
        line({"type": "thread.started", "thread_id": thread}),
        line({"type": "turn.started"}),
    ]
    for index, (command, output, exit_code) in enumerate(commands):
        item = {
            "id": f"item_{index}",
            "type": "command_execution",
            "command": f"bash -lc '{command}'",
        }
        lines.append(
            line(
                {
                    "type": "item.started",
                    "item": {
                        **item,
                        "aggregated_output": "",
                        "exit_code": None,
                        "status": "in_progress",
                    },
                }
            )
        )
        lines.append(
            line(
                {
                    "type": "item.completed",
                    "item": {
                        **item,
                        "aggregated_output": output,
                        "exit_code": exit_code,
                        "status": "completed" if exit_code == 0 else "failed",
                    },
                }
            )
        )
    if rng.random() < 0.4:
        lines.append(
            line(
                {
                    "type": "item.completed",
                    "item": {
                        "id": f"item_{len(commands)}",
                        "type": "file_change",
                        "changes": [
                            {
                                "path": "/workspace/chatting/app/worker/activity.py",
                                "kind": "update",
                            },
                            {
                                "path": "/workspace/chatting/tests/test_worker_activity.py",
                                "kind": "update",
                            },
                        ],
                        "status": "completed",
                    },
                }
            )
        )
    if error:
        lines.append(line({"type": "error", "message": error}))
        lines.append(line({"type": "turn.failed", "error": {"message": error}}))
        return lines
    if reply:
        lines.append(
            line(
                {
                    "type": "item.completed",
                    "item": {
                        "id": f"item_{len(commands) + 1}",
                        "type": "agent_message",
                        "text": reply,
                    },
                }
            )
        )
    input_tokens = rng.randint(12_000, 90_000)
    lines.append(
        line(
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": input_tokens,
                    "cached_input_tokens": int(input_tokens * 0.8),
                    "output_tokens": rng.randint(200, 3_000),
                },
            }
        )
    )
    return lines


def emit_stdout(
    monitor: WorkerActivityMonitor,
    clock: Clock,
    message: TaskQueueMessage,
    lines: list[str],
    start: datetime,
    end: datetime,
    chunks: int,
) -> None:
    # Whole lines per chunk: token usage is projected from each event alone.
    cuts = sorted(
        {0, len(lines), *(len(lines) * i // chunks for i in range(1, chunks))}
    )
    span = end - start
    for index, (left, right) in enumerate(zip(cuts, cuts[1:])):
        clock.at = start + span * (index + 1) / len(cuts)
        monitor.record_executor_output(
            task_message=message,
            stream="stdout",
            content="".join(f"{entry}\n" for entry in lines[left:right]),
        )


def simulate_run(
    store: SQLiteStateStore,
    monitor: WorkerActivityMonitor,
    clock: Clock,
    rng: random.Random,
    message: TaskQueueMessage,
    *,
    status: Status,
    finished: datetime,
    latency_ms: int,
    backend: str,
    model_tier: str,
) -> str:
    envelope = message.envelope
    started = finished - timedelta(milliseconds=latency_ms)
    run_id = f"run:{message.task_id}:{int(finished.timestamp()) * 10**9 + finished.microsecond * 1000}"
    clock.at = started
    store.stage_inbox_task(message)
    monitor.record_task_received(task_message=message)
    decision = None
    tier = "high" if model_tier == "high" else "low" if model_tier == "low" else "high"
    if model_tier == "auto" or envelope.source == "cron":
        confidence = round(rng.uniform(0.55, 0.99), 3)
        tier = "low" if confidence >= 0.8 and rng.random() < 0.6 else "high"
        decision = {
            "tier": tier,
            "reason": "jev_confident_low" if tier == "low" else "jev_high_or_uncertain",
            "confidence": confidence,
            "probabilities": {
                "low": confidence if tier == "low" else round(1 - confidence, 3),
                "high": round(1 - confidence, 3) if tier == "low" else confidence,
            },
            "provider_model": typesafe.ROUTING_MODEL,
        }
    escalation = None
    if tier == "low" and status == "success" and rng.random() < 0.15:
        escalation = "Needs a repository-wide refactor across handler and worker."
        tier = "high"
    reply = rng.choice(REPLIES)
    commands = rng.sample(COMMANDS, rng.randint(1, 4))
    attempts = 3 if status == "dead_letter" else 2 if rng.random() < 0.06 else 1
    last_error = None
    errors: list[str] = []
    reason_codes: list[str] = []
    window = latency_ms / attempts
    totals: dict[str, int] = {}
    for attempt in range(1, attempts + 1):
        attempt_start = started + timedelta(milliseconds=window * (attempt - 1) + 400)
        attempt_end = attempt_start + timedelta(milliseconds=window * 0.9)
        clock.at = attempt_start
        monitor.record_executor_started(task_message=message, attempt=attempt)
        failed_attempt = attempt < attempts or status == "dead_letter"
        is_error = status == "execution_error" and attempt == attempts
        error_text = (
            "stream disconnected before completion: error decoding response body"
            if is_error
            else None
        )
        if is_error:
            commands = [*commands[:2], next(c for c in COMMANDS if c[2] != 0)]
        thread = f"thr_{message.task_id[-6:]}_{attempt}".replace(":", "")
        model = MODELS[tier]
        if backend == "app-server":
            lines = app_server_turn(
                rng,
                model=model,
                thread=thread,
                turn=f"turn_{attempt}",
                commands=commands,
                reply=None if failed_attempt or is_error else reply,
                totals=totals,
                rerouted_to=MODELS["low"]
                if tier == "high" and rng.random() < 0.08
                else None,
                error=error_text,
            )
        else:
            lines = exec_turn(
                rng,
                thread=thread,
                commands=commands,
                reply=None if failed_attempt or is_error else reply,
                error=error_text,
            )
        if failed_attempt:
            lines = lines[: max(2, len(lines) // 2)]
        emit_stdout(
            monitor,
            clock,
            message,
            lines,
            attempt_start,
            attempt_end,
            rng.randint(1, 3),
        )
        if failed_attempt or is_error:
            clock.at = attempt_end
            monitor.record_executor_output(
                task_message=message, stream="stderr", content=rng.choice(STDERR) + "\n"
            )
        if failed_attempt:
            last_error = rng.choice(FAILED_ATTEMPTS)
            clock.at = attempt_end + timedelta(milliseconds=200)
            monitor.record_executor_failure(
                task_message=message, attempt=attempt, error=last_error
            )
    if status == "dead_letter":
        reason_codes = ["retry_exhausted"]
    elif status == "execution_error":
        errors = [
            rng.choice(
                [
                    "executor_exit_nonzero:1",
                    "executor_exit_nonzero:1",
                    "executor_exit_nonzero:137",
                    "app_server_error:turn failed: responseStreamDisconnected",
                ]
            )
        ]
        reason_codes = ["executor_reported_errors"]
    clock.at = finished - timedelta(milliseconds=300)
    body = (
        reply
        if status == "success"
        else "Sorry, I couldn’t finish that. The executor failed; details are in the run log."
    )
    monitor.record_egress(
        egress_message=EgressQueueMessage(
            task_id=message.task_id,
            envelope_id=envelope.id,
            trace_id=message.trace_id,
            message=OutboundMessage(
                channel=envelope.reply_channel.type,
                target=envelope.reply_channel.target,
                body=body,
            ),
            emitted_at=clock.at,
            event_id=f"egress:{message.task_id}:0",
            sequence=0,
        ),
        publish_source="worker",
    )
    store.append_run(
        RunRecord(
            run_id=run_id,
            envelope_id=envelope.id,
            source=envelope.source,
            workflow="default",
            latency_ms=latency_ms,
            result_status=status,
            created_at=finished,
            work_item_id=assignment.work_item_id
            if (assignment := store.get_work_assignment(task_id=message.task_id))
            else None,
        )
    )
    store.append_audit_event(
        AuditEvent(
            run_id=run_id,
            envelope_id=envelope.id,
            source=envelope.source,
            workflow="default",
            result_status=status,
            detail={
                "trace_id": message.trace_id,
                "task_id": message.task_id,
                "reason_codes": reason_codes,
                "attempt_count": attempts,
                "executor_launch_count": attempts,
                "executor_backend": backend,
                "max_attempts": 3,
                "last_error": last_error,
                "last_error_stage": "executor" if last_error else None,
                "supervised_recovery_used": envelope.source == "im"
                and rng.random() < 0.05,
                "execution_result": None
                if status == "dead_letter"
                else {
                    "schema_version": "1.0",
                    "errors": errors,
                    "escalation_reason": escalation,
                },
                "model_tier": model_tier,
                "executed_model_tier": tier,
                "auto_model_decision": decision,
                "escalation_reason": escalation,
                "incremental_reply_send_published_count": 0,
                "egress_message_count": 1,
                "conversation_id": message.work_item_id,
                "coalesced_followup_count": 0,
            },
            created_at=finished,
        )
    )
    if status == "dead_letter":
        store.append_dead_letter(
            run_id=run_id,
            envelope=envelope,
            reason_codes=reason_codes,
            last_error=last_error,
            attempt_count=attempts,
        )
    clock.at = finished
    monitor.record_executor_finished(
        task_message=message,
        run_id=run_id,
        result_status=status,
        attempt_count=attempts,
        reason_codes=reason_codes,
        latency_ms=latency_ms,
    )
    with sqlite3.connect(store.db_path) as db:
        db.execute(
            "UPDATE worker_inbox SET state='completed' WHERE task_id=?",
            (message.task_id,),
        )
        db.execute(
            "UPDATE dead_letters SET created_at=? WHERE run_id=?",
            (format_utc(finished), run_id),
        )
    return run_id


def telegram(title: str, index: int) -> ReplyChannel:
    return ReplyChannel(
        type="telegram",
        target="-1002233445566",
        metadata={"chat_title": title, "message_thread_id": 100 + index},
    )


def seed_rich(
    store: SQLiteStateStore,
    monitor: WorkerActivityMonitor,
    clock: Clock,
    now: datetime,
) -> list[TaskQueueMessage]:
    """Seed ~40 items and ~200 runs; return the extra live executors' tasks."""
    rng = random.Random(398)
    plans: list[tuple[TaskQueueMessage, str, str]] = []
    window = timedelta(days=14)

    def when(day_offset: float) -> datetime:
        return now - window + timedelta(days=day_offset)

    for index, (title, request) in enumerate(IM_REQUESTS):
        item = f"item_rich_im_{index:02d}"
        backend = "exec" if index % 4 == 3 else "app-server"
        tier = ("auto", "high", "low", "auto")[index % 4]
        runs = 2 + index % 4
        for run in range(runs):
            at = when(rng.uniform(0, 13.9))
            content = (
                f"Earlier conversation omitted.\n\nCurrent message from Ed:\n{request}"
                if run == 0
                else f"Current message from Ed:\nFollow-up {run}: {request.lower()}"
            )
            plans.append(
                (
                    rich_task(
                        f"telegram:{item}:{run}",
                        "im",
                        content,
                        at,
                        telegram(f"{title} · {IM_TOPICS[index]}", index),
                        item,
                    ),
                    backend,
                    tier,
                )
            )
    for index, (name, hour, minute, count, content) in enumerate(CRON_JOBS):
        item = f"item_rich_cron_{index:02d}"
        base = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if base > now - timedelta(minutes=30):
            base -= timedelta(days=1)
        if count == 20:
            fires = [base - timedelta(hours=6 * n) for n in range(count)]
        elif count == 2:
            fires = [
                base - timedelta(days=7 * n + (base.weekday() - 4) % 7)
                for n in range(count)
            ]
        elif count == 10:
            fires = [
                day
                for n in range(14)
                if (day := base - timedelta(days=n)).weekday() < 5
            ]
        else:
            fires = [base - timedelta(days=n) for n in range(count)]
        for day in fires:
            envelope_id = f"cron:{name}:{day.isoformat()}"
            plans.append(
                (
                    rich_task(
                        envelope_id,
                        "cron",
                        content,
                        day,
                        ReplyChannel(
                            type="telegram",
                            target="-1002233445566",
                            metadata={"chat_title": name},
                        ),
                        item,
                    ),
                    "exec",
                    "auto",
                )
            )
    for index, (reminder_id, revision, prompt) in enumerate(REMINDER_FIRES):
        item = f"item_rich_reminder_{index % 4:02d}"
        at = reminder_fire_time(now, index)
        plans.append(
            (
                rich_task(
                    f"reminder:{reminder_id}:{revision}",
                    "reminder",
                    prompt,
                    at,
                    telegram("Reminders", 90 + index % 4),
                    item,
                ),
                "app-server",
                "high",
            )
        )
    for index, (subject, body) in enumerate(EMAIL_REQUESTS):
        item = f"item_rich_email_{index:02d}"
        reply = ReplyChannel(
            type="email", target="ed@example.com", metadata={"subject": subject}
        )
        for run in range(3):
            at = when(rng.uniform(0, 13.9))
            plans.append(
                (
                    rich_task(
                        f"email:<{index}.{run}@mail.example.com>",
                        "email",
                        f"Subject: {subject}\n\n{body}",
                        at,
                        reply,
                        item,
                    ),
                    "exec",
                    "auto",
                )
            )
    for run in range(6):
        at = when(rng.uniform(0, 13.9))
        plans.append(
            (
                rich_task(
                    f"internal:recovery:{run}",
                    "internal",
                    INTERNAL_REQUESTS[0],
                    at,
                    ReplyChannel(type="internal", target="worker"),
                    "item_rich_internal_00",
                ),
                "app-server",
                "high",
            )
        )
    for run in range(5):
        at = when(rng.uniform(0, 13.9))
        plans.append(
            (
                rich_task(
                    f"telegram:legacy:{run}",
                    "im",
                    f"Current user message:\nOld-style message {run}: what’s on my calendar today?",
                    at,
                    ReplyChannel(type="telegram", target="12345678"),
                    None,
                ),
                "exec",
                "high",
            )
        )

    plans.sort(key=lambda plan: plan[0].envelope.received_at)
    statuses: list[Status] = ["success"] * len(plans)
    picks = rng.sample(range(len(plans) - 8), 30)
    for index in picks[:8]:
        statuses[index] = "dead_letter"
    for index in picks[8:]:
        statuses[index] = "execution_error"
    run_ids: dict[str, list[str]] = {
        "success": [],
        "execution_error": [],
        "dead_letter": [],
    }
    item_tiers: dict[str, str] = {}
    failed: dict[str, str] = {}
    for (message, backend, tier), status in zip(plans, statuses):
        if status == "dead_letter":
            latency = rng.randint(600_000, 1_200_000)
        elif rng.random() < 0.08:
            latency = rng.randint(240_000, 1_200_000)
        else:
            latency = int(min(1_200_000, max(5_000, rng.lognormvariate(10.8, 0.8))))
        finished = min(
            message.envelope.received_at + timedelta(milliseconds=latency + 1500),
            now - timedelta(minutes=3),
        )
        run_id = simulate_run(
            store,
            monitor,
            clock,
            rng,
            message,
            status=status,
            finished=finished,
            latency_ms=latency,
            backend=backend,
            model_tier=tier,
        )
        run_ids[status].append(run_id)
        if status == "execution_error" and message.envelope.source != "internal":
            failed[backend] = run_id
        if message.work_item_id:
            item_tiers[message.work_item_id] = tier
            store.set_conversation_executor(
                conversation_id=message.work_item_id, backend=backend
            )
    for item, tier in item_tiers.items():
        store.set_work_model_tier(work_item_id=item, tier=tier)

    long_task = seed_long_task(store, monitor, clock, rng, now)

    live: list[TaskQueueMessage] = []
    for index, (title, request, minutes) in enumerate(
        [
            (
                "Chatting dev · Race suite bisect",
                "Run the full Go race suite and bisect any new failures.",
                14,
            ),
            (
                "Ops · Deploy watch",
                "Watch the deploy and tell me when all health checks pass.",
                3,
            ),
        ]
    ):
        item = f"item_rich_live_{index:02d}"
        message = rich_task(
            f"telegram:{item}:0",
            "im",
            f"Current message from Ed:\n{request}",
            now - timedelta(minutes=minutes),
            telegram(title, 50 + index),
            item,
        )
        clock.at = message.envelope.received_at
        store.stage_inbox_task(message)
        monitor.record_task_received(task_message=message)
        clock.at += timedelta(seconds=2)
        monitor.record_executor_started(task_message=message, attempt=1)
        lines = app_server_turn(
            rng,
            model=MODELS["high"],
            thread=f"thr_live_{index}",
            turn="turn_1",
            commands=rng.sample(COMMANDS, 3),
            reply=None,
            totals={},
        )[:-1]
        emit_stdout(
            monitor, clock, message, lines, clock.at, now - timedelta(seconds=20), 3
        )
        with sqlite3.connect(store.db_path) as db:
            db.execute(
                "UPDATE worker_inbox SET state='active' WHERE task_id=?",
                (message.task_id,),
            )
        live.append(message)

    for index, request in enumerate(
        [
            "Draft the release notes for v0.14 once the deploy finishes.",
            "Then post the release notes in #chatting-dev.",
            "Check the reminder table for duplicates.",
        ]
    ):
        item = "item_rich_live_01" if index < 2 else "item_rich_im_02"
        clock.at = now - timedelta(minutes=2 - index * 0.5)
        message = rich_task(
            f"telegram:queued:{index}",
            "im",
            f"Current message from Ed:\n{request}",
            clock.at,
            telegram("Ops · Deploy watch", 51),
            item,
        )
        store.stage_inbox_task(message)

    for index, run_id in enumerate(run_ids["success"][-3:]):
        task_id = run_id.split(":", 1)[1].rsplit(":", 1)[0]
        store.queue_egress_outbox_event(
            EgressQueueMessage(
                task_id=task_id,
                envelope_id=task_id.removeprefix("task:"),
                trace_id=f"trace:{task_id}",
                message=OutboundMessage(
                    channel="telegram",
                    target="-1002233445566",
                    body="Queued reply waiting for the broker.",
                ),
                emitted_at=now - timedelta(minutes=9 - index),
                event_id=f"outbox:{task_id}:1",
                sequence=1,
            )
        )
    with sqlite3.connect(store.db_path) as db:
        db.execute(
            "UPDATE egress_outbox SET created_at=?, updated_at=? WHERE publish_state='pending_publish'",
            (format_utc(now - timedelta(minutes=9)),) * 2,
        )
        db.execute(
            "UPDATE worker_inbox SET lease_owner = 'supervisor-' || rowid"
            " WHERE state = 'active'"
        )
        db.execute(
            """UPDATE work_items SET created_at = COALESCE(
                   (SELECT MIN(created_at) FROM run_records AS run
                    WHERE run.work_item_id = work_items.work_item_id),
                   created_at)"""
        )
    clock.at = None
    print(
        "Rich fixture: "
        + ", ".join(f"{len(ids)} {status}" for status, ids in run_ids.items())
        + "".join(f"; failed {key} run {value}" for key, value in failed.items())
        + f"; dead letter {run_ids['dead_letter'][-1]}; long task {long_task}",
        flush=True,
    )
    return live


def seed_long_task(
    store: SQLiteStateStore,
    monitor: WorkerActivityMonitor,
    clock: Clock,
    rng: random.Random,
    now: datetime,
) -> str:
    """One finished task with >300 events, every event shape and two attempts."""
    item = "item_rich_long"
    started = now - timedelta(hours=5, minutes=12)
    message = rich_task(
        "telegram:item_rich_long:0",
        "im",
        "Earlier conversation omitted.\n\nCurrent message from Ed:\nMigrate the worker activity store to the new schema, backfill token usage and make the whole test suite pass.",
        started,
        telegram("Chatting dev · Activity store migration", 77),
        item,
    )
    clock.at = started
    store.stage_inbox_task(message)
    store.set_conversation_executor(conversation_id=item, backend="app-server")
    monitor.record_task_received(task_message=message)
    clock.at = started + timedelta(seconds=3)
    monitor.record_executor_started(task_message=message, attempt=1)
    first = app_server_turn(
        rng,
        model=MODELS["high"],
        thread="thr_long",
        turn="turn_1",
        commands=COMMANDS[:4],
        reply=None,
        totals={},
        error="stream disconnected before completion: Transport error: error decoding response body",
    )
    emit_stdout(
        monitor, clock, message, first, clock.at, started + timedelta(minutes=6), 4
    )
    monitor.record_executor_output(
        task_message=message,
        stream="stderr",
        content=STDERR[1] + "\n" + STDERR[2] + "\n",
    )
    monitor.record_executor_failure(
        task_message=message, attempt=1, error=FAILED_ATTEMPTS[1]
    )
    clock.at = started + timedelta(minutes=7)
    monitor.record_executor_started(task_message=message, attempt=2)
    totals: dict[str, int] = {}
    elapsed = timedelta(minutes=7)
    for turn in range(1, 41):
        commands = [
            COMMANDS[(turn * 3 + offset) % len(COMMANDS)] for offset in range(3)
        ]
        lines = app_server_turn(
            rng,
            model=MODELS["high"],
            thread="thr_long",
            turn=f"turn_{turn + 1}",
            commands=commands,
            reply=f"Step {turn}/40: migrated batch {turn} of the activity table."
            if turn < 40
            else "Migration finished: 41,882 activity rows rewritten, token usage backfilled for 1,204 tasks and the full suite passes.",
            totals=totals,
            rerouted_to=MODELS["low"] if turn == 12 else None,
        )
        start = started + elapsed
        elapsed += timedelta(seconds=rng.randint(45, 420))
        emit_stdout(monitor, clock, message, lines, start, started + elapsed, 8)
        if turn % 9 == 0:
            monitor.record_executor_output(
                task_message=message, stream="stderr", content=STDERR[3] + "\n"
            )
    finished = started + elapsed + timedelta(seconds=5)
    run_id = f"run:{message.task_id}:{int(finished.timestamp()) * 10**9 + finished.microsecond * 1000}"
    clock.at = finished - timedelta(seconds=1)
    monitor.record_egress(
        egress_message=EgressQueueMessage(
            task_id=message.task_id,
            envelope_id=message.envelope.id,
            trace_id=message.trace_id,
            message=OutboundMessage(
                channel="telegram",
                target="-1002233445566",
                body="Migration finished; PR #405 is ready for review.",
            ),
            emitted_at=clock.at,
            event_id=f"egress:{message.task_id}:0",
            sequence=0,
        ),
        publish_source="worker",
    )
    latency = int((finished - started).total_seconds() * 1000)
    store.append_run(
        RunRecord(
            run_id=run_id,
            envelope_id=message.envelope.id,
            source="im",
            workflow="default",
            latency_ms=latency,
            result_status="success",
            created_at=finished,
            work_item_id=item,
        )
    )
    store.append_audit_event(
        AuditEvent(
            run_id=run_id,
            envelope_id=message.envelope.id,
            source="im",
            workflow="default",
            result_status="success",
            detail={
                "trace_id": message.trace_id,
                "task_id": message.task_id,
                "reason_codes": [],
                "attempt_count": 2,
                "executor_launch_count": 2,
                "executor_backend": "app-server",
                "max_attempts": 3,
                "last_error": FAILED_ATTEMPTS[1],
                "last_error_stage": "executor",
                "supervised_recovery_used": False,
                "execution_result": {
                    "schema_version": "1.0",
                    "errors": [],
                    "escalation_reason": None,
                },
                "model_tier": "auto",
                "executed_model_tier": "high",
                "auto_model_decision": {
                    "tier": "high",
                    "reason": "jev_high_or_uncertain",
                    "confidence": 0.93,
                    "probabilities": {"high": 0.93, "low": 0.07},
                    "provider_model": typesafe.ROUTING_MODEL,
                },
                "escalation_reason": None,
                "incremental_reply_send_published_count": 0,
                "egress_message_count": 1,
                "conversation_id": item,
                "coalesced_followup_count": 0,
            },
            created_at=finished,
        )
    )
    clock.at = finished
    monitor.record_executor_finished(
        task_message=message,
        run_id=run_id,
        result_status="success",
        attempt_count=2,
        reason_codes=[],
        latency_ms=latency,
    )
    with sqlite3.connect(store.db_path) as db:
        db.execute(
            "UPDATE worker_inbox SET state='completed' WHERE task_id=?",
            (message.task_id,),
        )
    return message.task_id


LIVE_TOTALS: dict[int, dict[str, int]] = {}


def live_tick(message: TaskQueueMessage, index: int) -> str:
    stamp = datetime.now(timezone.utc)
    tick = int(stamp.timestamp()) // 5
    command, output, exit_code = COMMANDS[(tick + index) % len(COMMANDS)]
    item = {
        "type": "commandExecution",
        "id": f"call_live_{tick}",
        "command": command,
        "cwd": "/workspace/chatting",
    }
    usage = {
        "inputTokens": 20_000 + tick % 7 * 1_000,
        "cachedInputTokens": 16_000,
        "outputTokens": 400,
        "reasoningOutputTokens": 120,
        "totalTokens": 20_400 + tick % 7 * 1_000,
    }
    total = LIVE_TOTALS.setdefault(index, {})
    for key, value in usage.items():
        total[key] = total.get(key, 0) + value
    return (
        "\n".join(
            [
                line(
                    {
                        "method": "item/completed",
                        "params": {
                            "item": {
                                **item,
                                "status": "completed" if exit_code == 0 else "failed",
                                "exitCode": exit_code,
                                "aggregatedOutput": output,
                            }
                        },
                    }
                ),
                line(
                    {
                        "method": "thread/tokenUsage/updated",
                        "params": {
                            "threadId": f"thr_live_{index}",
                            "turnId": "turn_1",
                            "tokenUsage": {
                                "last": usage,
                                "total": total,
                                "modelContextWindow": 272_000,
                            },
                        },
                    }
                ),
                line(
                    {
                        "method": "item/completed",
                        "params": {
                            "item": {
                                "type": "agentMessage",
                                "id": f"msg_live_{tick}",
                                "text": f"Still working on {message.envelope.content.splitlines()[-1][:60].lower()}",
                            }
                        },
                    }
                ),
            ]
        )
        + "\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--port", type=int, default=19465)
    parser.add_argument("--empty", action="store_true")
    parser.add_argument(
        "--rich",
        action="store_true",
        help="Also seed two weeks of varied runs, failures and live executors",
    )
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    path = args.state_dir / "worker.db"
    if path.exists():
        parser.error("use a fresh state directory for each preview")
    store = SQLiteStateStore(str(path))
    clock = Clock()
    monitor = WorkerActivityMonitor(store=store, now_fn=clock)
    now = datetime.now(timezone.utc)
    live = None
    if not args.empty:
        specs = [
            (
                "Platform interface",
                "Review the new platform interface and check the layout on mobile.",
                "live",
            ),
            (
                "Release notes",
                "Prepare release notes for the latest changes.",
                "success",
            ),
            (
                "Repository maintenance",
                "Check the repository for dependency updates.",
                "execution_error",
            ),
            (
                "Friday planning",
                "Bring together a short plan for Friday’s review.",
                "queued",
            ),
        ]
        for index, (title, content, status) in enumerate(specs):
            at = now - timedelta(minutes=index * 35 + 2)
            message = task(index, title, content, at)
            store.stage_inbox_task(message)
            if status == "queued":
                continue
            monitor.record_task_received(task_message=message)
            monitor.record_executor_started(task_message=message, attempt=1)
            native = [
                {
                    "method": "item/started",
                    "params": {
                        "item": {
                            "type": "commandExecution",
                            "command": "git status --short",
                        }
                    },
                },
                {
                    "method": "item/completed",
                    "params": {
                        "item": {
                            "type": "commandExecution",
                            "command": "git status --short",
                            "exitCode": 0,
                            "aggregatedOutput": "Working tree is clean.",
                        }
                    },
                },
                {
                    "method": "item/completed",
                    "params": {
                        "item": {
                            "type": "agentMessage",
                            "text": "I’ve checked the current state. The layout and core flows are ready for a closer look.",
                        }
                    },
                },
            ]
            output = "\n".join(json.dumps(row) for row in native) + "\n"
            monitor.record_executor_output(
                task_message=message, stream="stdout", content=output[:73]
            )
            monitor.record_executor_output(
                task_message=message, stream="stdout", content=output[73:]
            )
            monitor.record_egress(
                egress_message=EgressQueueMessage(
                    task_id=message.task_id,
                    envelope_id=message.envelope.id,
                    trace_id=message.trace_id,
                    message=OutboundMessage(
                        channel="telegram",
                        target="fixture-chat",
                        body="Fixture delivery: the review is ready.",
                    ),
                    emitted_at=now,
                    event_id=f"fixture-egress:{index}",
                    sequence=0,
                ),
                publish_source="preview",
            )
            if status == "live":
                live = message
                with sqlite3.connect(store.db_path) as db:
                    db.execute(
                        "UPDATE worker_inbox SET state='active' WHERE task_id=?",
                        (message.task_id,),
                    )
            else:
                finish(store, monitor, message, status, at)
    rich_live: list[TaskQueueMessage] = []
    if args.rich and not args.empty:
        rich_live = seed_rich(store, monitor, clock, now)
    server = start_worker_activity_server(
        host="127.0.0.1", port=args.port, monitor=monitor
    )
    stopped = Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    print(f"Fixture worker: http://127.0.0.1:{args.port}", flush=True)
    try:
        while not stopped.wait(5):
            if live and (args.state_dir / "finish-live").exists():
                finish(store, monitor, live, "success", datetime.now(timezone.utc))
                live = None
            elif live:
                monitor.record_executor_output(
                    task_message=live,
                    stream="stdout",
                    content=json.dumps(
                        {
                            "method": "item/completed",
                            "params": {
                                "item": {
                                    "type": "agentMessage",
                                    "text": f"Preview activity tick · {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC",
                                }
                            },
                        }
                    )
                    + "\n",
                )
            for index, message in enumerate(rich_live):
                monitor.record_executor_output(
                    task_message=message,
                    stream="stdout",
                    content=live_tick(message, index),
                )
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
