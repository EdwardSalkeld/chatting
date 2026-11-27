import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.state import SQLiteStateStore
from app.worker.codex_quota import QuotaPoller, normalize
from app.worker.metrics import render_sqlite_metrics


class CodexQuotaTests(unittest.TestCase):
    def test_poll_exposes_windows_and_does_not_treat_empty_credits_as_blocked(self):
        response = {
            "accountId": "private",
            "ordinaryUsageAllowed": True,
            "rateLimits": {
                "credits": {"hasCredits": False},
                "primary": {
                    "usedPercent": 20,
                    "windowDurationMins": 300,
                    "resetsAt": 123,
                },
                "secondary": {
                    "usedPercent": 70,
                    "windowDurationMins": 10080,
                    "resetsAt": 456,
                },
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteStateStore(str(Path(tmp) / "worker.db"))
            server = MagicMock()
            server.rpc.side_effect = [{}, response]
            with patch("app.worker.codex_quota.AppServer", return_value=server):
                QuotaPoller(store, "codex").poll()
            metrics = render_sqlite_metrics(store.db_path).decode()
            self.assertIn("chatting_codex_quota_blocked 0", metrics)
            self.assertIn(
                'chatting_codex_quota_remaining_percent{window_minutes="300"} 80',
                metrics,
            )
            self.assertIn(
                'chatting_codex_quota_remaining_percent{window_minutes="10080"} 30',
                metrics,
            )
            self.assertNotIn("private", metrics)
            server.close.assert_called_once()

    def test_unknown_permission_is_not_reported_as_unblocked(self):
        result = normalize(
            {"rateLimits": {"primary": {"usedPercent": 100, "windowDurationMins": 300}}}
        )
        self.assertIsNone(result["ordinary_usage_allowed"])

    def test_failed_poll_preserves_prior_snapshot_and_closes_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteStateStore(str(Path(tmp) / "worker.db"))
            store.record_codex_quota(
                {"ordinary_usage_allowed": False, "windows": []}, 123
            )
            server = MagicMock()
            server.rpc.side_effect = RuntimeError("failure")
            with (
                patch("app.worker.codex_quota.AppServer", return_value=server),
                self.assertRaises(RuntimeError),
            ):
                QuotaPoller(store, "codex").poll()
            metrics = render_sqlite_metrics(store.db_path).decode()
            self.assertIn("chatting_codex_quota_blocked 1", metrics)
            self.assertIn(
                "chatting_codex_quota_snapshot_timestamp_seconds 123", metrics
            )
            server.close.assert_called_once()
