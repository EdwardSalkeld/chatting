import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from app.main_token_usage import backfill
from app.state import SQLiteStateStore, token_usage


class TokenUsageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "worker.db")
        self.store = SQLiteStateStore(self.path)

    def append(self, phase, detail, **kwargs):
        self.store.append_worker_activity(
            occurred_at=datetime.now(timezone.utc),
            phase=phase,
            summary=phase,
            detail=detail,
            task_id="task1",
            **kwargs,
        )

    def stdout(self, method, params):
        self.append(
            "executor_stdout",
            {"content": json.dumps({"method": method, "params": params}) + "\n"},
        )

    def usage(self, cumulative=100, **overrides):
        params = {
            "threadId": "thread1",
            "turnId": "turn1",
            "tokenUsage": {
                "total": {"inputTokens": cumulative, "outputTokens": 10},
                "last": {
                    "inputTokens": 100,
                    "cachedInputTokens": 80,
                    "outputTokens": 10,
                    "reasoningOutputTokens": 4,
                },
                "modelContextWindow": 475000,
            },
        }
        params.update(overrides)
        self.stdout("thread/tokenUsage/updated", params)

    def rows(self):
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM model_token_usage ORDER BY activity_id"
                )
            ]

    def test_duplicate_snapshots_equal_responses_reroute_and_run(self):
        self.append("executor_started", {"attempt": 2})
        self.stdout("chatting/modelSelected", {"model": "selected"})
        self.usage()
        self.usage()  # repeated snapshot
        self.stdout("model/rerouted", {"toModel": "actual"})
        self.usage(200)  # same last usage, different response
        self.append("task_finished", {}, run_id="run1")
        rows = self.rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual([r["model"] for r in rows], ["selected", "actual"])
        self.assertEqual(sum(r["input_tokens"] for r in rows), 200)
        self.assertEqual(sum(r["output_tokens"] for r in rows), 20)
        self.assertTrue(all(r["attempt"] == 2 and r["run_id"] == "run1" for r in rows))
        self.assertIsNone(rows[0]["cache_write_input_tokens"])

    def test_unknown_model_invalid_events_and_turn_identity(self):
        self.append("executor_stdout", {"content": "not json\n[]\n"})
        self.usage(tokenUsage={"last": {"inputTokens": -1}})
        self.usage()
        self.usage(turnId="turn2")
        self.assertEqual(len(self.rows()), 2)
        self.assertIsNone(self.rows()[0]["model"])

    def test_legacy_model_backfill_is_bounded_and_idempotent(self):
        self.append("executor_started", {"attempt": 1})
        payload = {"task": {"task_id": "task1", "model_selection": {"model": "legacy"}}}
        self.stdout(
            "item/started",
            {
                "item": {
                    "type": "userMessage",
                    "content": [{"text": json.dumps(payload)}],
                }
            },
        )
        self.usage()
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("DELETE FROM model_token_usage")
            connection.execute("DELETE FROM model_usage_context")
            connection.commit()
            self.assertEqual(backfill(connection, since="9999"), 0)
            self.assertEqual(backfill(connection, since="2000"), 1)
            self.assertEqual(backfill(connection, since="2000"), 0)
            token_usage.initialize(connection)  # upgrade/restart is safe
        self.assertEqual(self.rows()[0]["model"], "legacy")

    def test_retry_keeps_failed_usage_and_backfill_preserves_live_context(self):
        self.append("executor_started", {"attempt": 1})
        self.stdout("chatting/modelSelected", {"model": "first"})
        self.usage()
        self.append("executor_failed_attempt", {"attempt": 1})
        self.append("executor_started", {"attempt": 2})
        self.stdout("chatting/modelSelected", {"model": "second"})
        self.usage(200)
        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("UPDATE model_usage_context SET model = 'live'")
            connection.commit()
            backfill(connection, since="2000")
            self.assertEqual(
                connection.execute(
                    "SELECT model FROM model_usage_context WHERE task_id='task1'"
                ).fetchone()[0],
                "live",
            )
        self.assertEqual([r["attempt"] for r in self.rows()], [1, 2])
        self.assertEqual([r["model"] for r in self.rows()], ["first", "second"])

    def test_backfill_and_live_writer_can_share_wal_database(self):
        self.usage()
        done = threading.Event()
        errors = []

        def write_live_activity():
            try:
                self.append("executor_stderr", {"content": "live output"})
            except Exception as error:
                errors.append(error)
            finally:
                done.set()

        writer = threading.Thread(target=write_live_activity)
        started = False

        def overlap_write(statement):
            nonlocal started
            if not started and "SELECT * FROM worker_activity_events" in statement:
                started = True
                writer.start()
                # With a deferred transaction the other writer commits here,
                # invalidating the backfill's snapshot before its first insert.
                # With a reserved writer it waits until this batch commits.
                done.wait(0.2)

        with sqlite3.connect(self.path) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("DELETE FROM model_token_usage")
            connection.commit()
            connection.set_trace_callback(overlap_write)
            self.assertEqual(backfill(connection, since="2000"), 1)
        writer.join(timeout=5)
        self.assertTrue(done.is_set())
        self.assertEqual(errors, [])
