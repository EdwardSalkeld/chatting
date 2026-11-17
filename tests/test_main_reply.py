import io
from argparse import Namespace
import json
import tempfile
import unittest
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.egress_client import submit_egress
from app.main_reply import (
    DEFAULT_HANDLER_EGRESS_URL,
    EXIT_DROPPED,
    EXIT_FOLLOWUPS,
    EXIT_TRANSIENT,
    _resolve_reply_messages,
    main,
)
from app.models import AttachmentRef, ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.telegram_text import normalize_telegram_outbound_text
from app.worker.main import WORKER_CONFIG_PATH_ENV_VAR
from tests.test_egress_client import READ_TIMEOUT_SECONDS, egress_server

EMAIL_SPEC = {
    "task_id": "task:email:53",
    "channel": "email",
    "target": "alice@example.com",
    "message": "working on it",
}
TELEGRAM_SPEC = {
    "task_id": "task:telegram:53",
    "channel": "telegram",
    "target": "8605042448",
}


class _FakeSubmit:
    """Stand-in for main_reply.submit_egress: records calls, returns a canned
    (status_code, response) so tests don't do real HTTP."""

    def __init__(
        self, status: int = 200, response: dict[str, object] | None = None
    ) -> None:
        self.status = status
        self.response = (
            response if response is not None else {"status": "dispatched", "reason": ""}
        )
        self.calls: list[tuple[str, dict[str, object]]] = []

    def __call__(
        self, url: str, payload: dict[str, object]
    ) -> tuple[int, dict[str, object]]:
        self.calls.append((url, payload))
        return self.status, dict(self.response)


def _run(
    directory: Path,
    spec: dict[str, object],
    submit: object = None,
    *,
    config: dict[str, object] | None = None,
) -> tuple[int, str, str]:
    spec_path = directory / "reply-spec.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    env = {}
    if config is not None:
        config_path = directory / "worker.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        env[WORKER_CONFIG_PATH_ENV_VAR] = str(config_path)
    stdout, stderr = io.StringIO(), io.StringIO()
    with (
        patch("app.main_reply.submit_egress", submit or _FakeSubmit()),
        patch("sys.stdout", stdout),
        patch("sys.stderr", stderr),
        patch("sys.argv", ["main_reply.py", "--spec-file", str(spec_path)]),
        patch.dict("os.environ", env),
    ):
        exit_code = main()
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _store(directory: Path) -> tuple[SQLiteStateStore, dict[str, object]]:
    db_path = str(directory / "worker.db")
    return SQLiteStateStore(db_path), {"db_path": db_path}


def _telegram_task_message(
    number: int,
    *,
    content: str,
    attachments: list[AttachmentRef] | None = None,
) -> TaskQueueMessage:
    envelope = TaskEnvelope(
        id=f"telegram:{number}",
        source="im",
        received_at=datetime(2026, 8, 16, 10, number % 60, tzinfo=timezone.utc),
        actor="8605042448:edsalkeld",
        content=content,
        attachments=attachments or [],
        context_refs=[],
        reply_channel=ReplyChannel(
            type="telegram",
            target="8605042448",
            metadata={"message_id": number},
        ),
        dedupe_key=f"telegram:{number}",
    )
    return TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:telegram:{number}")


class MainReplyCliTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)

    def test_only_telegram_replies_normalize_escape_sequences(self):
        text = r"C:\new\table and regex \d+\.\d+"
        for channel in ("email", "github", "log", "telegram"):
            with self.subTest(channel=channel):
                args = Namespace(
                    channel=channel,
                    target="target",
                    message=text,
                    telegram_reaction=None,
                    attachment_path=None,
                    telegram_topic_id=None,
                )
                self.assertEqual(
                    _resolve_reply_messages(args, {})[0].body,
                    normalize_telegram_outbound_text(text)
                    if channel == "telegram"
                    else text,
                )

    def test_topic_sends_and_history_use_chosen_destination_not_inbound_topic(self):
        for topic in (8, None):
            with self.subTest(topic=topic), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                store, config = _store(directory)
                task = _telegram_task_message(53, content="In topic seven")
                task.envelope.reply_channel.metadata["message_thread_id"] = 7
                store.stage_inbox_task(task)
                for part in (1, 2):
                    metadata = {"message_id": 9000 + part}
                    if topic is not None:
                        metadata["message_thread_id"] = topic
                    submit = _FakeSubmit(
                        response={"status": "dispatched", "metadata": metadata}
                    )
                    spec = {
                        **TELEGRAM_SPEC,
                        "task_id": task.task_id,
                        "telegram_topic_id": topic,
                        "message": f"part {part}",
                        "event_id": f"evt:part:{part}",
                    }
                    self.assertEqual(_run(directory, spec, submit, config=config)[0], 0)
                    self.assertEqual(
                        submit.calls[0][1]["message"].get("metadata", {}),
                        {"message_thread_id": topic} if topic is not None else {},
                    )
                history = store.list_telegram_history_around(
                    target="8605042448",
                    topic_id=topic,
                    message_id=9002,
                    before=1,
                    after=0,
                )
                self.assertEqual([h.content for h in history], ["part 1", "part 2"])
                other = store.list_telegram_history_around(
                    target="8605042448", topic_id=9, message_id=9002, before=2, after=0
                )
                self.assertEqual(other, [])

    def test_invalid_specs_are_rejected_before_submission(self):
        cases = [
            (
                {**TELEGRAM_SPEC, "message": "hi", "telegram_topic_id": topic},
                "positive integer",
            )
            for topic in (True, 0, "oops")
        ] + [
            ({**EMAIL_SPEC, "task_id": "email:53"}, "envelope_id is required"),
            ({**EMAIL_SPEC, "bogus": "nope"}, "unknown fields"),
            (
                {**TELEGRAM_SPEC, "telegram_reaction": "🔎", "telegram_message_id": 1},
                "telegram_reaction '🔎' is not supported by Telegram",
            ),
        ]
        for spec, error in cases:
            with self.subTest(spec=spec):
                submit = _FakeSubmit()
                with self.assertRaisesRegex(ValueError, error):
                    _run(self.dir, spec, submit)
                self.assertEqual(submit.calls, [])

    def test_reaction_only_does_not_hide_task_after_restart(self) -> None:
        store, config = _store(self.dir)
        task = _telegram_task_message(53, content="Question")
        store.stage_inbox_task(task)
        store.claim_next_inbox_task()
        spec = {
            **TELEGRAM_SPEC,
            "telegram_reaction": "👍",
            "telegram_message_id": 53,
        }
        self.assertEqual(_run(self.dir, spec, config=config)[0], 0)

        store.recover_inbox_tasks()
        claimed = store.claim_next_inbox_task()
        assert claimed is not None
        self.assertEqual(claimed.task_message.task_id, task.task_id)
        self.assertEqual(claimed.state, "active")

    def test_telegram_reply_claims_followups_and_withholds_stale_content(self) -> None:
        store, config = _store(self.dir)
        parent = _telegram_task_message(53, content="Initial question")
        followup = _telegram_task_message(
            54,
            content="[document attached: details.pdf]",
            attachments=[
                AttachmentRef(uri="file:///tmp/details.pdf", name="details.pdf")
            ],
        )
        store.stage_inbox_task(parent)
        store.stage_inbox_task(followup)
        store.claim_next_inbox_task()
        spec = {**TELEGRAM_SPEC, "message": "A now-stale answer"}
        submit = _FakeSubmit()
        exit_code, stdout, _ = _run(self.dir, spec, submit, config=config)

        payload = json.loads(stdout)
        self.assertEqual(exit_code, EXIT_FOLLOWUPS)
        self.assertEqual(submit.calls, [])
        self.assertEqual(payload["status"], "follow_ups_claimed")
        self.assertEqual(payload["follow_ups"][0]["task_id"], followup.task_id)
        self.assertEqual(
            payload["follow_ups"][0]["attachments"],
            [{"uri": "file:///tmp/details.pdf", "name": "details.pdf"}],
        )
        self.assertTrue(
            store.has_unresolved_attached_followups(parent_task_id=parent.task_id)
        )

        # A later call has already seen the attached turns, so it may send
        # the current response and atomically close the whole bundle.
        self.assertEqual(_run(self.dir, spec, submit, config=config)[0], 0)
        self.assertEqual(len(submit.calls), 1)
        for task in (parent, followup):
            self.assertEqual(
                store.get_inbox_task(task_id=task.task_id).state, "closing"
            )

    def test_posts_unsequenced_incremental_v2_payload_and_records_activity(self):
        submit = _FakeSubmit()
        db_path = str(self.dir / "state.db")
        spec = {**EMAIL_SPEC, "event_id": "evt:custom:1"}
        exit_code, stdout, _ = _run(self.dir, spec, submit, config={"db_path": db_path})

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(submit.calls), 1)
        url, payload = submit.calls[0]
        self.assertEqual(url, DEFAULT_HANDLER_EGRESS_URL)
        self.assertEqual(payload["task_id"], "task:email:53")
        self.assertEqual(payload["envelope_id"], "email:53")
        self.assertEqual(payload["trace_id"], "trace:email:53")
        self.assertEqual(payload["event_id"], "evt:custom:1")
        self.assertEqual(payload["event_kind"], "incremental")
        self.assertEqual(payload["message_type"], "chatting.egress.v2")
        self.assertEqual(payload["message"]["body"], "working on it")
        self.assertNotIn("sequence", payload)

        printed = json.loads(stdout)
        self.assertEqual(printed["status"], "dispatched")
        self.assertEqual(printed["http_status"], 200)

        activity = SQLiteStateStore(db_path).list_recent_worker_activity(
            limit=10, include_internal=True
        )
        self.assertEqual(len(activity), 1)
        self.assertEqual(activity[0]["task_id"], "task:email:53")
        self.assertEqual(activity[0]["phase"], "egress_incremental")
        self.assertEqual(activity[0]["summary"], "incremental egress to email")
        self.assertEqual(activity[0]["detail"]["event_id"], "evt:custom:1")

    def test_delivered_telegram_reply_closes_unclaimed_task_and_records_history(
        self,
    ) -> None:
        # A direct final reply must prevent a later worker replay.
        store, config = _store(self.dir)
        task = _telegram_task_message(53, content="Question")
        store.stage_inbox_task(task)
        spec = {
            **TELEGRAM_SPEC,
            "message": "Recorded answer",
            "event_id": "evt:answer:1",
        }
        submit = _FakeSubmit(
            response={"status": "dispatched", "metadata": {"message_id": 9001}}
        )
        exit_code, stdout, _ = _run(self.dir, spec, submit, config=config)

        self.assertEqual(exit_code, 0)
        self.assertEqual(store.get_inbox_task(task_id=task.task_id).state, "closing")
        turns = store.list_telegram_history_around(
            target="8605042448", message_id=9001, before=1, after=0
        )
        self.assertEqual(turns[-1].role, "assistant")
        self.assertEqual(turns[-1].content, "Recorded answer")
        self.assertEqual(turns[-1].event_id, "evt:answer:1")
        self.assertEqual(json.loads(stdout)["telegram_message_id"], 9001)

    def test_dropped_exit_has_no_retry_id_and_stays_quiet_in_db(self) -> None:
        db_path = str(self.dir / "state.db")
        submit = _FakeSubmit(
            status=422,
            response={"status": "dropped", "reason": "telegram_attachment_missing"},
        )
        spec = {**TELEGRAM_SPEC, "message": "here is the screenshot"}
        exit_code, _, stderr = _run(self.dir, spec, submit, config={"db_path": db_path})

        self.assertEqual(exit_code, EXIT_DROPPED)
        printed = json.loads(stderr)
        self.assertEqual(printed["status"], "dropped")
        self.assertEqual(printed["reason"], "telegram_attachment_missing")
        self.assertNotIn("retry_event_id", printed)
        # A drop is not a delivery, so it must not be recorded as one.
        activity = SQLiteStateStore(db_path).list_recent_worker_activity(
            limit=10, include_internal=True
        )
        self.assertEqual(activity, [])

    def test_handler_transport_failure_after_send_is_transient(self) -> None:
        with egress_server("close_mid_body") as (url, received):
            exit_code, _, stderr = _run(
                self.dir,
                EMAIL_SPEC,
                partial(submit_egress, timeout_seconds=READ_TIMEOUT_SECONDS),
                config={"handler_egress_url": url},
            )

        self.assertEqual(exit_code, EXIT_TRANSIENT)
        printed = json.loads(stderr)
        self.assertEqual(printed["http_status"], 0)
        self.assertEqual(printed["retry_event_id"], received[0]["event_id"])

    def test_transient_retry_event_id_is_the_base_for_combined_replies(self) -> None:
        submit = _FakeSubmit(status=0, response={"reason": "transport error"})
        spec = {
            **TELEGRAM_SPEC,
            "message": "I found the problem.",
            "telegram_reaction": "👀",
            "telegram_message_id": 123,
            "event_id": "evt:custom:both",
        }
        exit_code, _, stderr = _run(self.dir, spec, submit)

        self.assertEqual(exit_code, EXIT_TRANSIENT)
        printed = json.loads(stderr)
        self.assertEqual(printed["retry_event_id"], "evt:custom:both")
        self.assertEqual(len(printed["results"]), 2)

    def test_spec_file_sends_reaction_and_message(self) -> None:
        submit = _FakeSubmit()
        spec = {
            **TELEGRAM_SPEC,
            "message": "I found the problem.",
            "telegram_reaction": "👀",
            "telegram_message_id": 123,
            "event_id": "evt:custom:both",
        }
        exit_code, stdout, _ = _run(self.dir, spec, submit)

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(submit.calls), 2)
        reaction = submit.calls[0][1]
        message = submit.calls[1][1]
        self.assertEqual(reaction["event_id"], "evt:custom:both:reaction")
        self.assertEqual(reaction["message"]["channel"], "telegram_reaction")
        self.assertEqual(reaction["message"]["body"], "👀")
        self.assertEqual(reaction["message"]["metadata"], {"message_id": 123})
        self.assertEqual(message["event_id"], "evt:custom:both:message")
        self.assertEqual(message["message"]["channel"], "telegram")
        self.assertEqual(message["message"]["body"], "I found the problem.")
        self.assertEqual(len(json.loads(stdout)["results"]), 2)

    def test_combined_reply_attempts_message_when_reaction_is_dropped(self) -> None:
        class _ReactionDropSubmit(_FakeSubmit):
            def __call__(
                self, url: str, payload: dict[str, object]
            ) -> tuple[int, dict[str, object]]:
                self.calls.append((url, payload))
                message = payload["message"]
                if message["channel"] == "telegram_reaction":
                    return 422, {"status": "dropped", "reason": "REACTION_INVALID"}
                return 200, {"status": "dispatched", "reason": ""}

        submit = _ReactionDropSubmit()
        spec = {
            **TELEGRAM_SPEC,
            "message": "The answer still gets sent.",
            "telegram_reaction": "👀",
            "telegram_message_id": 123,
        }
        self.assertEqual(_run(self.dir, spec, submit)[0], EXIT_DROPPED)
        self.assertEqual(len(submit.calls), 2)
        self.assertEqual(submit.calls[1][1]["message"]["channel"], "telegram")

    def test_reaction_uses_inbound_message_id_from_worker_inbox(self) -> None:
        store, config = _store(self.dir)
        task = _telegram_task_message(53, content="hello")
        task.envelope.reply_channel.metadata["message_id"] = 456
        store.stage_inbox_task(task)
        submit = _FakeSubmit()
        spec = {**TELEGRAM_SPEC, "telegram_reaction": "👍"}
        self.assertEqual(_run(self.dir, spec, submit, config=config)[0], 0)
        self.assertEqual(submit.calls[0][1]["message"]["metadata"], {"message_id": 456})

    def test_spec_file_attachment_message(self) -> None:
        submit = _FakeSubmit()
        attachment_path = self.dir / "menu.pdf"
        attachment_path.write_bytes(b"%PDF-1.4\n")
        spec = {
            **TELEGRAM_SPEC,
            "message": "This week's menu",
            "attachment_path": str(attachment_path),
            "attachment_name": "menu.pdf",
        }
        self.assertEqual(_run(self.dir, spec, submit)[0], 0)
        message = submit.calls[0][1]["message"]
        self.assertEqual(message["body"], "This week's menu")
        self.assertEqual(message["attachment"]["name"], "menu.pdf")
        self.assertEqual(message["attachment"]["uri"], attachment_path.as_uri())
