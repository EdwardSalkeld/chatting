import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.broker.bbmb_client import PickedMessage
from app.personal_inbox import CodexQuotaExhausted, codex_structured
from app.state import SQLiteStateStore
from app.worker.personal_inbox import process_one
from tests.test_personal_inbox import digest_task, jev_answer, mail_task


class PersonalQuotaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = SQLiteStateStore(str(Path(self.tmp.name) / "worker.db"))
        self.cache = sqlite3.connect(":memory:")
        self.addCleanup(self.cache.close)
        self.cache.execute(
            "CREATE TABLE classifications (id TEXT PRIMARY KEY, fingerprint TEXT, result TEXT)"
        )
        self.broker = MagicMock()

    def test_blocked_gate_leaves_email_and_digest_in_broker(self):
        self.store.block_codex_quota()
        self.assertFalse(
            process_one(
                self.broker,
                self.cache,
                jev_key="j",
                summary_model="m",
                store=self.store,
            )
        )
        self.broker.pickup_json.assert_not_called()
        self.broker.ack.assert_not_called()

    def test_codex_quota_error_does_not_become_cached_fallback(self):
        task = mail_task()
        self.broker.pickup_json.return_value = PickedMessage("message-guid", task)
        with (
            patch("app.personal_inbox.post", return_value=jev_answer()),
            patch(
                "app.personal_inbox.summarize",
                side_effect=CodexQuotaExhausted("blocked"),
            ),
        ):
            with self.assertRaises(CodexQuotaExhausted):
                process_one(
                    self.broker,
                    self.cache,
                    jev_key="j",
                    summary_model="m",
                    store=self.store,
                )
        self.assertTrue(self.store.codex_quota_blocked())
        self.broker.ack.assert_not_called()
        self.broker.publish_json.assert_not_called()
        self.assertEqual(
            self.cache.execute("SELECT COUNT(*) FROM classifications").fetchone()[0], 0
        )
        # After a fresh successful usage reading the same leased message can
        # be picked up and processed normally on redelivery.
        self.store.record_codex_quota(
            {"ordinary_usage_allowed": True, "windows": []}, 10**12
        )
        with (
            patch("app.personal_inbox.post", return_value=jev_answer()),
            patch(
                "app.personal_inbox.summarize",
                return_value={"summary": "done", "action": "", "deadline": ""},
            ),
        ):
            self.assertTrue(
                process_one(
                    self.broker,
                    self.cache,
                    jev_key="j",
                    summary_model="m",
                    store=self.store,
                )
            )
        self.broker.ack.assert_called_once()
        self.broker.publish_json.assert_called_once()

    def test_gate_closing_during_classification_prevents_publishing_fallback(self):
        self.broker.pickup_json.return_value = PickedMessage(
            "message-guid", mail_task()
        )

        def completed_after_block(*_args, **_kwargs):
            self.store.block_codex_quota()
            return {"kind": "personal_inbox.result.v1"}

        with patch(
            "app.worker.personal_inbox.classify", side_effect=completed_after_block
        ):
            with self.assertRaises(CodexQuotaExhausted):
                process_one(
                    self.broker,
                    self.cache,
                    jev_key="j",
                    summary_model="m",
                    store=self.store,
                )
        self.broker.ack.assert_not_called()
        self.broker.publish_json.assert_not_called()

    def test_digest_quota_failure_is_unacked(self):
        self.broker.pickup_json.return_value = PickedMessage(
            "digest-guid", digest_task()
        )
        with patch(
            "app.personal_inbox.codex_structured",
            side_effect=CodexQuotaExhausted("blocked"),
        ):
            with self.assertRaises(CodexQuotaExhausted):
                process_one(
                    self.broker,
                    self.cache,
                    jev_key="j",
                    summary_model="m",
                    store=self.store,
                )
        self.assertTrue(self.store.codex_quota_blocked())
        self.broker.ack.assert_not_called()
        self.broker.publish_json.assert_not_called()

    def test_cli_quota_error_is_identified_without_exposing_diagnostics(self):
        result = subprocess.CompletedProcess(
            [],
            1,
            '{"type":"error","message":"You have hit your usage limit"}',
            "PRIVATE MAIL",
        )
        with patch("app.personal_inbox.subprocess.run", return_value=result):
            with self.assertRaises(CodexQuotaExhausted) as error:
                codex_structured("", {}, {}, model="m")
        self.assertNotIn("PRIVATE", str(error.exception))

    def test_non_error_model_text_does_not_open_global_quota_gate(self):
        result = subprocess.CompletedProcess(
            [],
            1,
            '{"type":"item.completed","message":"You have hit your usage limit"}',
            "other failure",
        )
        with patch("app.personal_inbox.subprocess.run", return_value=result):
            with self.assertRaises(ValueError):
                codex_structured("", {}, {}, model="m")
