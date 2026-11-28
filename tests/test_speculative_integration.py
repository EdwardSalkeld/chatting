import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.models import ReplyChannel, TaskEnvelope
from app.model_command import MODELS
from app.state import SQLiteStateStore
from app.worker.speculative_quota import tick

NOW = 2_000_000_000
CONFIG = {
    "enabled": True,
    "repositories": ["/repo"],
    "jobs": [
        {"name": "audit", "prompt": "Audit this repo", "model_tier": "speculative_deep"}
    ],
}


def message(task_id, metadata):
    now = datetime.now(timezone.utc)
    return TaskQueueMessage(
        task_id=task_id,
        trace_id=task_id,
        emitted_at=now,
        work_item_id="item_" + task_id,
        envelope=TaskEnvelope(
            id=task_id,
            source="reminder",
            received_at=now,
            actor=None,
            content="test",
            attachments=[],
            context_refs=[],
            reply_channel=ReplyChannel(type="log", target=task_id, metadata=metadata),
            dedupe_key=task_id,
        ),
    )


def test_handler_submission_is_idempotent_and_waits_for_completion(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.record_codex_quota(
        {
            "ordinary_usage_allowed": True,
            "windows": [
                {"duration_minutes": 10080, "used_percent": 80, "resets_at": NOW + 100}
            ],
        },
        NOW,
    )
    result = BytesIO(
        json.dumps({"reminder": {"reminder_id": "rem_1", "revision": 1}}).encode()
    )
    with patch(
        "app.worker.speculative_quota.urllib.request.urlopen", return_value=result
    ) as post:
        assert tick(store, CONFIG, "http://handler", now=NOW)
        body = json.loads(post.call_args.args[0].data)
        assert body["context_refs"] == ["repo:/repo"]
        assert body["idempotency_key"].startswith("speculative:")
        instructions = body["prompt_context"][0]
        assert "Before investigating" in instructions
        assert "same topic, even if nothing worth reporting" in instructions
        assert (
            "audit" in instructions
            and "/repo" in instructions
            and MODELS["speculative_deep"] in instructions
        )
    assert not tick(store, CONFIG, "http://handler", now=NOW)
    assert not tick(store, CONFIG, "http://handler", now=NOW + 181)


def test_retry_after_lost_http_response_reuses_same_request(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.record_codex_quota(
        {
            "ordinary_usage_allowed": True,
            "windows": [
                {"duration_minutes": 10080, "used_percent": 80, "resets_at": NOW + 100}
            ],
        },
        NOW,
    )
    response = BytesIO(
        json.dumps({"reminder": {"reminder_id": "rem_1", "revision": 1}}).encode()
    )
    with patch(
        "app.worker.speculative_quota.urllib.request.urlopen",
        side_effect=[OSError("connection lost"), response],
    ) as post:
        import pytest

        with pytest.raises(OSError):
            tick(store, CONFIG, "http://handler", now=NOW)
        assert tick(store, CONFIG, "http://handler", now=NOW + 1)
        assert (
            post.call_args_list[0].args[0].data == post.call_args_list[1].args[0].data
        )


def test_claim_prioritizes_normal_work_and_rechecks_quota(tmp_path):
    import time

    store = SQLiteStateStore(str(tmp_path / "state.db"))
    store.set_speculative_enabled(True)
    now = time.time()
    store.record_codex_quota(
        {
            "ordinary_usage_allowed": True,
            "windows": [
                {"duration_minutes": 10080, "used_percent": 80, "resets_at": now + 100}
            ],
        },
        now,
    )
    speculative = "task:reminder:rem_1:1"
    with store._transaction() as db:
        db.execute(
            "INSERT INTO speculative_runs VALUES ('id','audit','/repo','speculative','{}',?,?)",
            (speculative, now),
        )
    store.stage_inbox_task(message(speculative, {"speculative_run": "id"}))
    store.stage_inbox_task(message("normal", {}))
    assert store.claim_next_inbox_task().task_message.task_id == "normal"
    store.record_codex_quota(
        {
            "ordinary_usage_allowed": True,
            "windows": [
                {"duration_minutes": 10080, "used_percent": 90, "resets_at": now + 100}
            ],
        },
        now,
    )
    assert store.claim_next_inbox_task() is None
    store.record_codex_quota(
        {
            "ordinary_usage_allowed": True,
            "windows": [
                {"duration_minutes": 10080, "used_percent": 80, "resets_at": now + 100}
            ],
        },
        now,
    )
    store.set_speculative_enabled(False)
    assert store.claim_next_inbox_task() is None
    store.set_speculative_enabled(True)
    assert store.claim_next_inbox_task().task_message.task_id == speculative
