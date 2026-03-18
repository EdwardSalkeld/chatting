import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.cli import main as cli_main
from app.main import _load_schedule_jobs, main
from app.models import RunRecord
from app.state import SQLiteStateStore
class MainCliSplitOnlyTests(unittest.TestCase):
    def test_main_requires_split_mode_entrypoints_for_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            with patch("sys.argv", ["app.main", "--db-path", str(db_path)]):
                with self.assertRaisesRegex(
                    ValueError,
                    "non-split runtime has been removed",
                ):
                    main()

    def test_main_list_runs_outputs_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            store = SQLiteStateStore(str(db_path))
            store.append_run(
                RunRecord(
                    run_id="run:1",
                    envelope_id="env:1",
                    source="im",
                    workflow="test",
                    latency_ms=1,
                    result_status="success",
                    created_at=datetime(2026, 3, 7, 0, 0, tzinfo=timezone.utc),
                )
            )

            stdout = io.StringIO()
            with (
                patch("sys.stdout", stdout),
                patch("sys.argv", ["app.main", "--db-path", str(db_path), "--list-runs"]),
            ):
                result = main()

            self.assertEqual(result, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(len(payload), 1)
            self.assertEqual(payload[0]["run_id"], "run:1")

    def test_cli_module_list_metrics_outputs_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            store = SQLiteStateStore(str(db_path))
            store.append_run(
                RunRecord(
                    run_id="run:metrics:1",
                    envelope_id="env:metrics:1",
                    source="im",
                    workflow="test",
                    latency_ms=25,
                    result_status="success",
                    created_at=datetime(2026, 3, 7, 0, 0, tzinfo=timezone.utc),
                )
            )

            stdout = io.StringIO()
            with (
                patch("sys.stdout", stdout),
                patch("sys.argv", ["app.cli", "--db-path", str(db_path), "--list-metrics"]),
            ):
                result = cli_main()

            self.assertEqual(result, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["total_runs"], 1)
            self.assertEqual(payload["by_status"], {"success": 1})

    def test_load_schedule_jobs_accepts_cron_timezone_and_interval_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                            "timezone": "Europe/London",
                            "interval_seconds": 86400,
                        },
                        {
                            "job_name": "interval-job",
                            "content": "Run interval job",
                            "interval_seconds": 300,
                            "start_at": "2026-03-07T00:00:00Z",
                        },
                    ]
                ),
                encoding="utf-8",
            )

            jobs = _load_schedule_jobs(str(schedule_path))

            self.assertEqual(len(jobs), 2)
            self.assertEqual(jobs[0].cron, "0 8 * * *")
            self.assertEqual(jobs[0].timezone_name, "Europe/London")
            self.assertEqual(jobs[0].interval_seconds, 86400)
            self.assertEqual(jobs[1].interval_seconds, 300)
            self.assertEqual(jobs[1].cron, None)

    def test_load_schedule_jobs_defaults_cron_timezone_to_utc(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            jobs = _load_schedule_jobs(str(schedule_path))

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].timezone_name, "UTC")

    def test_load_schedule_jobs_ignores_interval_anchors_when_cron_is_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                            "interval_seconds": "legacy-value",
                            "start_at": 123,
                        }
                    ]
                ),
                encoding="utf-8",
            )

            jobs = _load_schedule_jobs(str(schedule_path))

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].cron, "0 8 * * *")
            self.assertEqual(jobs[0].interval_seconds, None)
            self.assertEqual(jobs[0].start_at, None)

    def test_load_schedule_jobs_rejects_invalid_cron_timezone(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                            "timezone": "Not/AZone",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "invalid timezone"):
                _load_schedule_jobs(str(schedule_path))

    def test_load_schedule_jobs_rejects_timezone_without_cron(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "interval-job",
                            "content": "Run interval job",
                            "interval_seconds": 60,
                            "timezone": "UTC",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "timezone is only valid with cron"):
                _load_schedule_jobs(str(schedule_path))

    def test_load_schedule_jobs_defaults_timezone_for_cron(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            schedule_path = Path(tmpdir) / "schedule.json"
            schedule_path.write_text(
                json.dumps(
                    [
                        {
                            "job_name": "cron-job",
                            "content": "Run cron job",
                            "cron": "0 8 * * *",
                        }
                    ]
                ),
                encoding="utf-8",
            )

            jobs = _load_schedule_jobs(str(schedule_path))

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].timezone_name, "UTC")
if __name__ == "__main__":
    unittest.main()
