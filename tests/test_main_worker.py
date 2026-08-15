import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import EgressQueueMessage
from app.models import OutboundMessage, RunRecord
from app.state import SQLiteStateStore
from app.worker.main import (
    WORKER_CONFIG_PATH_ENV_VAR,
    _build_executor_env,
    _log_worker_processed,
    _load_config,
    _publish_egress_with_outbox,
    _resolve_non_negative_int,
)
from app.worker.runtime import WorkerProcessResult


class _RecordingMonitor:
    def __init__(self) -> None:
        self.egress_calls: list[str] = []

    def record_egress(self, *, egress_message, publish_source: str) -> None:
        self.egress_calls.append(publish_source)


def _completion_egress(event_id: str = "evt:task:1:0:completion") -> EgressQueueMessage:
    return EgressQueueMessage(
        task_id="task:1",
        envelope_id="email:1",
        trace_id="trace:1",
        event_index=0,
        event_count=1,
        message=OutboundMessage(channel="log", target="task", body="done"),
        emitted_at=datetime(2026, 6, 7, 22, 30, tzinfo=timezone.utc),
        event_id=event_id,
        sequence=0,
        event_kind="completion",
        message_type="chatting.egress.v2",
    )


class PublishEgressWithOutboxTests(unittest.TestCase):
    def _run(self, submit_return):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = SQLiteStateStore(str(Path(tmp.name) / "worker.db"))
        monitor = _RecordingMonitor()
        with patch("app.worker.main.submit_egress", return_value=submit_return):
            _publish_egress_with_outbox(
                store=store,
                handler_egress_url="http://handler.test/egress",
                egress_message=_completion_egress(),
                activity_monitor=monitor,
            )
        return store, monitor

    def test_delivered_2xx_acks_outbox_and_records_activity(self) -> None:
        store, monitor = self._run((200, {"status": "completed"}))
        # Acked -> not replayable.
        self.assertEqual(store.list_replayable_egress_outbox_events(), [])
        self.assertEqual(monitor.egress_calls, ["worker"])

    def test_transient_failure_leaves_event_pending_for_replay(self) -> None:
        store, monitor = self._run((0, {"reason": "unreachable"}))
        replayable = store.list_replayable_egress_outbox_events()
        self.assertEqual(len(replayable), 1)
        self.assertEqual(monitor.egress_calls, [])

    def test_permanent_drop_422_is_loud_and_acked(self) -> None:
        with self.assertLogs("app.worker.main", level="ERROR") as captured:
            store, monitor = self._run(
                (422, {"status": "dropped", "reason": "disallowed_channel"})
            )
        # Acked so it does not replay forever, but not counted as delivered.
        self.assertEqual(store.list_replayable_egress_outbox_events(), [])
        self.assertEqual(monitor.egress_calls, [])
        self.assertTrue(
            any("worker_egress_dropped_by_handler" in line for line in captured.output)
        )


class MainWorkerTests(unittest.TestCase):
    def test_load_config_accepts_activity_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"bbmb_address": "127.0.0.1:9876", "activity_port": 0}),
                encoding="utf-8",
            )

            payload = _load_config(str(config_path))

        self.assertEqual(payload["activity_port"], 0)

    def test_resolve_non_negative_int_accepts_zero(self) -> None:
        resolved = _resolve_non_negative_int(
            None,
            0,
            default_value=9465,
            setting_name="activity_port",
        )

        self.assertEqual(resolved, 0)

    def test_resolve_non_negative_int_rejects_negative_values(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "config activity_port must be a non-negative integer"
        ):
            _resolve_non_negative_int(
                None,
                -1,
                default_value=9465,
                setting_name="activity_port",
            )

    def test_build_executor_env_propagates_cli_config_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text("{}", encoding="utf-8")

            env = _build_executor_env(str(config_path), {"PATH": "/usr/bin"})

        self.assertIsNotNone(env)
        assert env is not None
        self.assertEqual(env["PATH"], "/usr/bin")
        self.assertEqual(
            env[WORKER_CONFIG_PATH_ENV_VAR],
            str(config_path.resolve()),
        )

    def test_build_executor_env_skips_override_without_cli_config(self) -> None:
        self.assertIsNone(
            _build_executor_env(None, {WORKER_CONFIG_PATH_ENV_VAR: "/tmp/worker.json"})
        )

    def test_log_worker_processed_keeps_success_terse(self) -> None:
        result = self._build_result(
            result_status="success",
            reason_codes=[],
            attempt_count=1,
            error_summary=None,
        )

        with self.assertLogs("app.worker.main", level="INFO") as captured:
            _log_worker_processed(task_id="task:1", result=result)

        self.assertEqual(len(captured.output), 1)
        log_line = captured.output[0]
        self.assertIn("INFO:app.worker.main:worker_processed", log_line)
        self.assertIn("result_status=success", log_line)
        self.assertNotIn("reason_codes=", log_line)
        self.assertNotIn("attempt_count=", log_line)
        self.assertNotIn("error_summary=", log_line)

    def test_log_worker_processed_includes_failure_context_for_execution_error(
        self,
    ) -> None:
        result = self._build_result(
            result_status="execution_error",
            reason_codes=["executor_reported_errors"],
            attempt_count=1,
            error_summary="executor_exit_nonzero:1:model unsupported",
        )

        with self.assertLogs("app.worker.main", level="WARNING") as captured:
            _log_worker_processed(task_id="task:1", result=result)

        self.assertEqual(len(captured.output), 1)
        log_line = captured.output[0]
        self.assertIn("WARNING:app.worker.main:worker_processed", log_line)
        self.assertIn("result_status=execution_error", log_line)
        self.assertIn("reason_codes=executor_reported_errors", log_line)
        self.assertIn("attempt_count=1", log_line)
        self.assertIn(
            "error_summary=executor_exit_nonzero:1:model unsupported",
            log_line,
        )

    def test_log_worker_processed_includes_failure_context_for_dead_letter(
        self,
    ) -> None:
        result = self._build_result(
            result_status="dead_letter",
            reason_codes=["retry_exhausted"],
            attempt_count=2,
            error_summary="RuntimeError: executor down",
        )

        with self.assertLogs("app.worker.main", level="WARNING") as captured:
            _log_worker_processed(task_id="task:1", result=result)

        log_line = captured.output[0]
        self.assertIn("result_status=dead_letter", log_line)
        self.assertIn("reason_codes=retry_exhausted", log_line)
        self.assertIn("attempt_count=2", log_line)
        self.assertIn("error_summary=RuntimeError: executor down", log_line)

    def _build_result(
        self,
        *,
        result_status: str,
        reason_codes: list[str],
        attempt_count: int,
        error_summary: str | None,
    ) -> WorkerProcessResult:
        return WorkerProcessResult(
            run_record=RunRecord(
                run_id="run:task:1:123",
                envelope_id="email:1",
                source="email",
                workflow="default",
                latency_ms=12,
                result_status=result_status,
                created_at=datetime(2026, 6, 7, 22, 30, tzinfo=timezone.utc),
            ),
            egress_messages=[
                EgressQueueMessage(
                    task_id="task:1",
                    envelope_id="email:1",
                    trace_id="trace:1",
                    event_index=0,
                    event_count=1,
                    message=OutboundMessage(
                        channel="internal",
                        target="task",
                        body="done",
                    ),
                    emitted_at=datetime(2026, 6, 7, 22, 30, tzinfo=timezone.utc),
                    event_id="evt:task:1:0:completion",
                    sequence=0,
                    event_kind="completion",
                    message_type="chatting.egress.v2",
                )
            ],
            dead_lettered=result_status == "dead_letter",
            attempt_count=attempt_count,
            reason_codes=reason_codes,
            error_summary=error_summary,
        )


if __name__ == "__main__":
    unittest.main()
