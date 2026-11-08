import json
import logging
import sqlite3
import subprocess
import urllib.error
from unittest.mock import Mock, patch

import pytest

from app.broker.bbmb_client import PickedMessage
from app import personal_inbox_diagnostics as diagnostics
from app.worker.personal_inbox import process_one


@pytest.fixture
def inbox():
    task = {
        "kind": "personal_inbox.digest.v1",
        "digest_id": "private-digest-id",
        "work_item_id": "private-work-id",
        "items": [
            {
                "ref": "private-email-id",
                "from": "private sender",
                "subject": "private subject",
                "body": "private body",
            }
        ],
    }
    broker = Mock()
    broker.pickup_json.return_value = PickedMessage(guid="private-guid", payload=task)
    cache = sqlite3.connect(":memory:")
    cache.execute(
        "CREATE TABLE classifications (id TEXT PRIMARY KEY, fingerprint TEXT, result TEXT)"
    )
    yield broker, cache
    cache.close()


def run(inbox):
    broker, cache = inbox
    return process_one(broker, cache, jev_key="private-key", summary_model="model")


def test_digest_validation_failure_has_safe_reason_and_source(inbox, caplog):
    with patch("app.personal_inbox.codex_structured", return_value={"items": []}):
        with pytest.raises(ValueError):
            run(inbox)
    text = caplog.text
    assert "stage=digest" in text
    assert "reason=digest_did_not_account_for_every_email" in text
    assert "source=app.personal_inbox:digest_lines:" in text
    assert "retry=lease" in text
    assert "task_ref=unknown" not in text
    assert "private" not in text
    assert diagnostics.current.get() is None


def test_ack_failure_then_cached_recovery_is_correlated(inbox, caplog):
    broker, _ = inbox
    broker.ack.side_effect = OSError("private-token and email body")
    caplog.set_level(logging.INFO)
    with patch(
        "app.worker.personal_inbox.digest_lines", return_value={"ok": True}
    ) as model:
        with pytest.raises(OSError):
            run(inbox)
        broker.ack.side_effect = None
        assert run(inbox)
    assert model.call_count == 1
    records = [
        r.getMessage() for r in caplog.records if "personal_inbox_" in r.getMessage()
    ]
    assert len(records) == 2
    assert "stage=result_ack" in records[0]
    assert "personal_inbox_completed" in records[1] and "cached=True" in records[1]
    refs = [r.split("task_ref=")[1].split()[0] for r in records]
    assert refs[0] == refs[1]
    assert "private" not in caplog.text


@pytest.mark.parametrize(
    "error,reason",
    [
        (ValueError("private body"), "unexpected_error"),
        (json.JSONDecodeError("private error", "private output", 0), "invalid_json"),
        (
            subprocess.TimeoutExpired("private command", 10, output="private"),
            "subprocess_timeout",
        ),
        (
            urllib.error.HTTPError(
                "https://private?token=secret", 503, "private", {}, None
            ),
            "http_status_503",
        ),
    ],
)
def test_exception_details_never_include_external_text(error, reason):
    assert diagnostics.failure_details(error)[0] == reason


def test_publish_failure_identifies_stage(inbox, caplog):
    inbox[0].publish_json.side_effect = RuntimeError("private broker response")
    with patch("app.worker.personal_inbox.digest_lines", return_value={}):
        with pytest.raises(RuntimeError):
            run(inbox)
    assert "stage=result_publish" in caplog.text
    assert "private" not in caplog.text


def test_idle_queue_does_not_emit_completion(inbox, caplog):
    inbox[0].pickup_json.return_value = None
    caplog.set_level(logging.INFO)
    assert not run(inbox)
    assert "personal_inbox_completed" not in caplog.text


def test_malformed_task_still_goes_to_invalid_queue(inbox, caplog):
    inbox[0].pickup_json.return_value.payload["kind"] = ["private"]
    assert run(inbox)
    assert (
        inbox[0].publish_json.call_args.args[0] == "chatting.personal-inbox.invalid.v1"
    )
    inbox[0].ack.assert_called_once()
    assert "private" not in caplog.text
