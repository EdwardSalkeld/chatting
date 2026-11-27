import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from app.models import ExecutionResult, ReplyChannel
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor
from app.worker.main import _process_claimed_task
from app.worker.metrics import render_sqlite_metrics
from tests.test_worker_inbox import _telegram_task
from tests.test_worker_runtime import StepExecutor


def quota_transcript(with_tool=False):
    events = []
    if with_tool:
        events.append(
            {"method": "item/started", "params": {"item": {"type": "commandExecution"}}}
        )
    events.append(
        {
            "method": "turn/completed",
            "params": {
                "turn": {
                    "status": "failed",
                    "error": {"codexErrorInfo": "usageLimitExceeded"},
                }
            },
        }
    )
    return "\n".join(map(json.dumps, events))


class QuotaQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = SQLiteStateStore(str(Path(self.tmp.name) / "worker.db"))

    def test_block_stages_work_then_releases_in_order_after_fresh_read(self):
        self.store.record_codex_quota(
            {"ordinary_usage_allowed": False, "windows": []}, 10
        )
        first, second = _telegram_task(1), _telegram_task(2)
        self.assertTrue(self.store.stage_inbox_task(first))
        self.assertTrue(self.store.stage_inbox_task(second))
        self.assertIsNone(self.store.claim_next_inbox_task())
        self.store.record_codex_quota(
            {"ordinary_usage_allowed": True, "windows": []}, 9
        )
        self.assertIsNone(self.store.claim_next_inbox_task())
        self.store.record_codex_quota(
            {"ordinary_usage_allowed": True, "windows": []}, 11
        )
        self.assertEqual(
            self.store.claim_next_inbox_task().task_message.task_id, first.task_id
        )

    def test_quota_failure_before_tools_is_queued_without_egress(self):
        task = _telegram_task(1)
        self.store.stage_inbox_task(task)
        claim = self.store.claim_next_inbox_task()
        executor = StepExecutor(
            lambda *_: ExecutionResult(
                errors=["app_server_error: usageLimitExceeded"],
                stdout=quota_transcript(),
            )
        )
        _process_claimed_task(
            store=self.store,
            inbox_task=claim,
            executor=executor,
            max_attempts=1,
            activity_monitor=WorkerActivityMonitor(store=self.store),
            handler_egress_url="http://invalid/egress",
        )
        self.assertEqual(
            self.store.get_inbox_task(task_id=task.task_id).state, "pending"
        )
        self.assertEqual(self.store.codex_quota_retry_count(task.task_id), 1)
        self.assertEqual(self.store.list_replayable_egress_outbox_events(), [])
        self.assertIsNone(self.store.claim_next_inbox_task())
        self.assertIn(
            "chatting_codex_quota_blocked 1",
            render_sqlite_metrics(self.store.db_path).decode(),
        )
        self.assertEqual(self.store.interrupted_quota_tasks(), [])

    def test_partial_attempt_waits_for_explicit_review(self):
        task = _telegram_task(1)
        self.store.stage_inbox_task(task)
        self.store.stage_inbox_task(_telegram_task(2))
        claim = self.store.claim_next_inbox_task()
        executor = StepExecutor(
            lambda *_: ExecutionResult(
                errors=["app_server_error: usageLimitExceeded"],
                stdout=quota_transcript(True),
            )
        )
        _process_claimed_task(
            store=self.store,
            inbox_task=claim,
            executor=executor,
            max_attempts=1,
            activity_monitor=WorkerActivityMonitor(store=self.store),
            handler_egress_url="http://invalid/egress",
        )
        self.assertEqual(self.store.interrupted_quota_tasks(), [task.task_id])
        self.assertIn(
            "chatting_codex_quota_interrupted_tasks 1",
            render_sqlite_metrics(self.store.db_path).decode(),
        )
        self.store.recover_inbox_tasks()
        self.assertEqual(self.store.interrupted_quota_tasks(), [task.task_id])
        self.store.record_codex_quota(
            {"ordinary_usage_allowed": True, "windows": []}, 10**12
        )
        self.assertIsNone(self.store.claim_next_inbox_task())
        self.assertTrue(self.store.resume_interrupted_quota_task(task.task_id))
        self.assertEqual(
            self.store.claim_next_inbox_task().task_message.task_id, task.task_id
        )

    def test_heartbeat_remains_runnable_during_quota_block(self):
        self.store.record_codex_quota(
            {"ordinary_usage_allowed": False, "windows": []}, 10
        )
        work = _telegram_task(1)
        self.store.stage_inbox_task(work)
        heartbeat = _telegram_task(2, target="heartbeat")
        heartbeat = replace(
            heartbeat,
            envelope=replace(
                heartbeat.envelope,
                source="internal",
                reply_channel=ReplyChannel(type="internal", target="heartbeat"),
            ),
        )
        self.store.stage_inbox_task(heartbeat)
        self.assertEqual(
            self.store.claim_next_inbox_task().task_message.task_id, heartbeat.task_id
        )
