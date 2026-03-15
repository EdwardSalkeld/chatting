import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.cli import main as cli_main
from app.main import main
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
                    policy_profile="default",
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

    def test_main_replay_dead_letters_supports_stub_executor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            stdout = io.StringIO()
            with (
                patch("sys.stdout", stdout),
                patch(
                    "sys.argv",
                    [
                        "app.main",
                        "--db-path",
                        str(db_path),
                        "--replay-dead-letters",
                        "--use-stub-executor",
                    ],
                ),
            ):
                result = main()

            self.assertEqual(result, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload, [])

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
                    policy_profile="default",
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


if __name__ == "__main__":
    unittest.main()
