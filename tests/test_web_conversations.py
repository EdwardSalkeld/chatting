import base64
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from app.broker import TaskQueueMessage
from app.models import ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.executor.codex import _task_payload, _web_history
from tests.test_main_reply import _run, _FakeSubmit


def conversation_id(target="-123", topic=7):
    return (
        base64.urlsafe_b64encode(
            json.dumps(
                {"chat": target, "topic": topic}, sort_keys=True, separators=(",", ":")
            ).encode()
        )
        .decode()
        .rstrip("=")
    )


def task(number, *, channel="web", target=None, topic=7):
    key = conversation_id(topic=topic)
    reply = ReplyChannel(
        type=channel,
        target=target or (key if channel == "web" else "-123"),
        metadata={
            "message_id": number,
            "message_thread_id": topic,
            "chat_title": "Test conversation",
        },
    )
    envelope = TaskEnvelope(
        id=f"{channel}:{number}",
        source="im",
        received_at=datetime(2026, 11, 19, 20, tzinfo=timezone.utc)
        + timedelta(seconds=number),
        actor="operator",
        content=f"message {number}",
        attachments=[],
        context_refs=[],
        reply_channel=reply,
        dedupe_key=f"{channel}:{number}",
    )
    return replace(
        TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:{number}"),
        work_item_id="item_test",
    )


def test_history_combines_web_and_telegram_without_crossing_topics(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "worker.db"))
    for message in [
        task(1, channel="telegram"),
        task(2),
        task(3, channel="telegram", topic=8),
    ]:
        store.stage_inbox_task(message)
    snapshot = store.web_conversation_snapshot(conversation_id=conversation_id())
    assert [m["content"] for m in snapshot["messages"]] == ["message 1", "message 2"]
    assert [m["transport"] for m in snapshot["messages"]] == ["telegram", "web"]
    assert snapshot["messages"][1]["status"] == "queued"
    # Existing Telegram history remains independently queryable.
    assert (
        len(store.search_telegram_history(target="-123", topic_id=7, query="message"))
        == 1
    )


def test_history_pagination_has_no_gaps_at_equal_timestamps(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "worker.db"))
    for number in range(1, 8):
        message = task(number)
        store.stage_inbox_task(
            replace(
                message,
                envelope=replace(
                    message.envelope, received_at=task(1).envelope.received_at
                ),
            )
        )
    seen = []
    before = None
    while True:
        snapshot = store.web_conversation_snapshot(
            conversation_id=conversation_id(), limit=2, before=before
        )
        seen.extend(m["id"] for m in snapshot["messages"])
        before = snapshot["next_cursor"]
        if before is None:
            break
    assert len(seen) == len(set(seen)) == 7


def test_main_reply_records_web_answer_only_after_confirmed_delivery(tmp_path):
    path = str(tmp_path / "worker.db")
    store = SQLiteStateStore(path)
    message = task(4)
    store.stage_inbox_task(message)
    spec = {
        "task_id": message.task_id,
        "channel": "web",
        "target": conversation_id(),
        "message": "Answer independent of Telegram",
        "event_id": "web-answer",
    }
    failed, _, _ = _run(
        tmp_path, spec, config={"db_path": path}, submit=_FakeSubmit(503)
    )
    assert failed == 3
    snapshot = store.web_conversation_snapshot(conversation_id=conversation_id())
    assert len(snapshot["messages"]) == 1
    assert snapshot["messages"][0]["status"] == "queued"
    assert _run(tmp_path, spec, config={"db_path": path})[0] == 0
    assert (
        _run(
            tmp_path,
            spec,
            config={"db_path": path},
            submit=_FakeSubmit(200, {"status": "deduped"}),
        )[0]
        == 0
    )
    snapshot = store.web_conversation_snapshot(conversation_id=conversation_id())
    assert len(snapshot["messages"]) == 2
    assert snapshot["messages"][0]["status"] == "answered"
    assert snapshot["messages"][1]["content"] == spec["message"]


def test_web_reply_cannot_record_against_another_conversation(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "worker.db"))
    message = task(5)
    store.stage_inbox_task(message)
    with pytest.raises(ValueError, match="destination"):
        store.record_web_outbound(
            target=conversation_id(topic=8),
            content="wrong place",
            task_id=message.task_id,
            event_id="event",
            occurred_at=message.emitted_at,
        )


def test_web_payload_keeps_context_and_requires_web_answer(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "worker.db"))
    previous = task(1, channel="telegram")
    current = task(5)
    store.stage_inbox_task(previous)
    store.stage_inbox_task(current)
    history = _web_history(store, current.envelope)
    assert [row["content"] for row in history] == ["message 1"]
    payload = _task_payload(
        current.envelope, current_time=datetime.now(timezone.utc), web_history=history
    )
    assert payload["reply_contract"]["reply_spec_example"]["channel"] == "web"
    assert (
        payload["reply_contract"]["reply_spec_example"]["target"] == conversation_id()
    )
    assert payload["recent_history"] == history
    assert "Do not cross-post" in payload["reply_contract"]["destination_choice"]


def test_invalid_history_cursor_is_rejected(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "worker.db"))
    with pytest.raises(ValueError):
        store.web_conversation_snapshot(
            conversation_id=conversation_id(), before="not-a-cursor"
        )


def test_prompt_bounds_history_without_truncating_saved_messages(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "worker.db"))
    previous = task(1)
    previous = replace(
        previous, envelope=replace(previous.envelope, content="x" * 5000)
    )
    store.stage_inbox_task(previous)
    current = task(5)
    store.stage_inbox_task(current)
    history = _web_history(store, current.envelope)
    assert len(history[0]["content"]) < 2200
    assert "truncated in prompt" in history[0]["content"]
    assert (
        store.web_conversation_snapshot(conversation_id=conversation_id())["messages"][
            0
        ]["content"]
        == "x" * 5000
    )


def test_reply_event_ids_are_scoped_to_the_task(tmp_path):
    store = SQLiteStateStore(str(tmp_path / "worker.db"))
    for number in (1, 2):
        message = task(number)
        store.stage_inbox_task(message)
        store.record_web_outbound(
            target=conversation_id(),
            content=f"answer {number}",
            task_id=message.task_id,
            event_id="reply",
            occurred_at=message.emitted_at,
        )
    replies = [
        row["content"]
        for row in store.web_conversation_snapshot(
            conversation_id=conversation_id(),
        )["messages"]
        if row["role"] == "assistant"
    ]
    assert replies == ["answer 1", "answer 2"]
