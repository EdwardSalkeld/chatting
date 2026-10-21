import json
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.personal_inbox import TRIAGE_QUESTIONS, classify
from app.worker.personal_inbox import process_one
from app.worker.personal_inbox_rules import RulesSnapshot, load_rules
from tests.test_personal_inbox import jev_answer, mail_task, summary_answer
from app.broker.bbmb_client import PickedMessage


class RulesRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.checkout = self.root / "checkout"
        self.source.mkdir()
        self.git(self.source, "init", "-b", "main")
        self.git(self.source, "config", "user.name", "Test")
        self.git(self.source, "config", "user.email", "test@example.com")
        self.save("Routine Example Co invoices are orders paid by direct debit.")
        self.git(self.root, "clone", str(self.source), str(self.checkout))

    def git(self, repo, *args):
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def save(self, instruction):
        path = self.source / "personal-inbox/rules.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "rules": [{"id": "example-invoices", "instruction": instruction}],
                }
            )
        )
        self.git(self.source, "add", ".")
        self.git(self.source, "commit", "-m", "Update rules")

    def test_fetches_new_commits_without_overwriting_dirty_checkout(self):
        first = load_rules(str(self.checkout))
        head = self.git(self.checkout, "rev-parse", "HEAD")
        path = self.checkout / "personal-inbox/rules.json"
        path.write_text("unsaved owner edits")
        self.save("Failed collections still need action.")
        second = load_rules(str(self.checkout))
        self.assertNotEqual(first.revision, second.revision)
        self.assertIn("Failed collections", second.instructions)
        self.assertNotIn("direct debit", second.instructions)
        self.assertEqual(second.ids, ("example-invoices",))
        self.assertEqual(path.read_text(), "unsaved owner edits")
        self.assertEqual(self.git(self.checkout, "rev-parse", "HEAD"), head)

    def test_invalid_or_missing_rules_and_failed_fetch_do_not_use_old_data(self):
        load_rules(str(self.checkout))
        path = self.source / "personal-inbox/rules.json"
        for data in (
            "not json",
            '{"schema_version": true, "rules": []}',
            '{"schema_version": 1, "rules": [{"id":"x","instruction":""}]}',
            "x" * 8001,
        ):
            with self.subTest(data=data[:40]):
                path.write_text(data)
                self.git(self.source, "commit", "-am", "Invalid rules")
                with self.assertRaises(ValueError):
                    load_rules(str(self.checkout))
        path.unlink()
        self.git(self.source, "commit", "-am", "Remove rules")
        with self.assertRaises(subprocess.CalledProcessError):
            load_rules(str(self.checkout))
        self.git(
            self.checkout, "remote", "set-url", "origin", str(self.root / "absent")
        )
        with self.assertRaises(subprocess.CalledProcessError):
            load_rules(str(self.checkout))

    def test_empty_rules_and_unconfigured_repo(self):
        self.assertEqual(load_rules(""), RulesSnapshot())
        path = self.source / "personal-inbox/rules.json"
        path.write_text('{"schema_version": 1, "rules": []}')
        self.git(self.source, "commit", "-am", "Clear rules")
        rules = load_rules(str(self.checkout))
        self.assertTrue(rules.revision)
        self.assertEqual(rules.instructions, "")
        self.assertEqual(rules.ids, ())


class RulesRuntimeTests(unittest.TestCase):
    def test_same_trusted_context_reaches_both_models_and_trace(self):
        context = "\nTrusted owner rules: Example Co invoices are paid by direct debit."
        original = json.dumps(TRIAGE_QUESTIONS, sort_keys=True)
        with patch("app.personal_inbox.post", return_value=jev_answer()) as post:
            with patch(
                "app.personal_inbox.codex_structured", return_value=summary_answer()
            ) as codex:
                result = classify(
                    mail_task(),
                    jev_key="j",
                    summary_model="m",
                    rules_context=context,
                    rules_revision="a" * 40,
                    rule_ids=("example-invoices",),
                )
        payload = post.call_args.args[2]
        for key in ("category", "requires_action", "has_deadline"):
            self.assertTrue(payload["questions"][key]["instructions"].endswith(context))
        self.assertEqual(
            payload["questions"]["prompt_injection"],
            TRIAGE_QUESTIONS["prompt_injection"],
        )
        self.assertEqual(payload["state"], mail_task()["mail"])
        self.assertTrue(codex.call_args.args[0].endswith(context))
        trace = result["classification"]["trace"]
        self.assertEqual(trace["rules_revision"], "a" * 40)
        self.assertEqual(trace["supplied_rule_ids"], ["example-invoices"])
        self.assertEqual(json.dumps(TRIAGE_QUESTIONS, sort_keys=True), original)

    def test_rules_failure_retries_and_ack_failure_replays_original_snapshot(self):
        cache = sqlite3.connect(":memory:")
        self.addCleanup(cache.close)
        cache.execute(
            "CREATE TABLE classifications (id TEXT PRIMARY KEY, fingerprint TEXT, result TEXT)"
        )

        class Broker:
            def pickup_json(self, *args, **kwargs):
                return PickedMessage(guid="g", payload=mail_task())

            def publish_json(self, *args):
                pass

            def ack(self, *args):
                raise OSError("ack lost")

        broker = Broker()
        with patch(
            "app.worker.personal_inbox.load_rules", side_effect=ValueError("bad rules")
        ):
            with patch.object(broker, "publish_json") as publish:
                with self.assertRaises(ValueError):
                    process_one(
                        broker, cache, jev_key="j", summary_model="m", rules_repo="repo"
                    )
                publish.assert_not_called()
        self.assertEqual(
            cache.execute("SELECT count(*) FROM classifications").fetchone()[0], 0
        )
        with patch(
            "app.worker.personal_inbox.load_rules",
            return_value=RulesSnapshot("rev", "rules", ("id",)),
        ) as load:
            with patch(
                "app.worker.personal_inbox.classify", return_value={"saved": "answer"}
            ) as model:
                for _ in range(2):
                    with self.assertRaises(OSError):
                        process_one(
                            broker,
                            cache,
                            jev_key="j",
                            summary_model="m",
                            rules_repo="repo",
                        )
        self.assertEqual(load.call_count, 1)
        self.assertEqual(model.call_count, 1)
        self.assertEqual(model.call_args.kwargs["rules_revision"], "rev")
