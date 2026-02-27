import tempfile
import unittest
from pathlib import Path

from app.main import run_bootstrap
from app.state import SQLiteStateStore


class MainBootstrapFlowTests(unittest.TestCase):
    def test_run_bootstrap_processes_unique_events_and_records_blocked_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "state.db")

            runs = run_bootstrap(db_path)

            self.assertEqual([run.source for run in runs], ["cron", "email", "email"])
            self.assertEqual(
                [run.envelope_id for run in runs],
                ["cron:daily-summary:2026-02-27T09:00:00+00:00", "email:ok-1", "email:blocked-1"],
            )
            self.assertEqual(
                [run.result_status for run in runs],
                ["success", "success", "blocked_action"],
            )

            audit_events = SQLiteStateStore(db_path).list_audit_events()
            self.assertEqual(len(audit_events), len(runs))
            self.assertEqual(
                [event.run_id for event in audit_events],
                [run.run_id for run in runs],
            )
            self.assertEqual(
                [event.result_status for event in audit_events],
                [run.result_status for run in runs],
            )


if __name__ == "__main__":
    unittest.main()
