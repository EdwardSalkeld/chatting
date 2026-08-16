import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.main_reply import (
    DEFAULT_HANDLER_EGRESS_URL,
    EXIT_DROPPED,
    EXIT_FOLLOWUPS,
    EXIT_TRANSIENT,
    main,
)
from app.models import AttachmentRef, ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.task_ledger import TaskLedgerStore


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


def _write_spec(directory: Path, spec: dict[str, object]) -> str:
    path = directory / "reply-spec.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    return str(path)


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
    def test_telegram_reply_claims_followups_and_withholds_stale_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            db_path = directory / "state.db"
            config_path = directory / "worker.json"
            config_path.write_text(
                json.dumps({"db_path": str(db_path)}), encoding="utf-8"
            )
            store = SQLiteStateStore(str(db_path))
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
            spec = _write_spec(
                directory,
                {
                    "task_id": parent.task_id,
                    "channel": "telegram",
                    "target": "8605042448",
                    "message": "A now-stale answer",
                },
            )
            submit = _FakeSubmit()
            stdout = io.StringIO()
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stdout", stdout),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "--spec-file",
                        spec,
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

            payload = json.loads(stdout.getvalue())
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
            stdout = io.StringIO()
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stdout", stdout),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "--spec-file",
                        spec,
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                second_exit_code = main()

            self.assertEqual(second_exit_code, 0)
            self.assertEqual(len(submit.calls), 1)
            self.assertEqual(
                store.get_inbox_task(task_id=parent.task_id).state, "closing"
            )
            self.assertEqual(
                store.get_inbox_task(task_id=followup.task_id).state, "closing"
            )

    def test_spec_file_posts_unsequenced_incremental_v2_payload(self) -> None:
        submit = _FakeSubmit()
        stdout = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:email:53",
                    "channel": "email",
                    "target": "alice@example.com",
                    "message": "working on it",
                    "event_id": "evt:custom:1",
                },
            )
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stdout", stdout),
                patch("sys.argv", ["main_reply.py", "--spec-file", spec]),
            ):
                exit_code = main()

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

        printed = json.loads(stdout.getvalue())
        self.assertEqual(printed["status"], "dispatched")
        self.assertEqual(printed["http_status"], 200)

    def test_spec_file_preserves_shell_dangerous_text_verbatim(self) -> None:
        # The whole point of the spec file: prose that would be mangled as a
        # shell argument (backticks, $, quotes, newlines) arrives intact.
        dangerous = 'Use `nix shell` for $HOME, say "hi".\nSecond line.'
        submit = _FakeSubmit()
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:telegram:53",
                    "channel": "telegram",
                    "target": "8605042448",
                    "message": dangerous,
                },
            )
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stdout", io.StringIO()),
                patch("sys.argv", ["main_reply.py", "--spec-file", spec]),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        _, payload = submit.calls[0]
        self.assertEqual(payload["message"]["body"], dangerous)

    def test_inline_message_flag_is_rejected(self) -> None:
        # The inline body path is retired: --message must not exist, so passing
        # it fails hard rather than risking a shell-mangled send.
        with patch(
            "sys.argv",
            [
                "main_reply.py",
                "task:email:53",
                "--channel",
                "email",
                "--target",
                "alice@example.com",
                "--message",
                "inline text",
            ],
        ):
            with self.assertRaises(SystemExit) as raised:
                main()
        self.assertNotEqual(raised.exception.code, 0)

    def test_spec_file_uses_handler_egress_url_from_worker_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"handler_egress_url": "http://10.0.0.5:9999/egress"}),
                encoding="utf-8",
            )
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:email:53",
                    "channel": "email",
                    "target": "alice@example.com",
                    "message": "working on it",
                },
            )
            submit = _FakeSubmit()
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stdout", io.StringIO()),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "--spec-file",
                        spec,
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(submit.calls[0][0], "http://10.0.0.5:9999/egress")

    def test_dropped_exit_and_stays_quiet_in_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"db_path": str(db_path)}), encoding="utf-8"
            )
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:telegram:53",
                    "channel": "telegram",
                    "target": "8605042448",
                    "message": "here is the screenshot",
                },
            )
            submit = _FakeSubmit(
                status=422,
                response={"status": "dropped", "reason": "telegram_attachment_missing"},
            )
            stderr = io.StringIO()
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stderr", stderr),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "--spec-file",
                        spec,
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

            activity = SQLiteStateStore(str(db_path)).list_recent_worker_activity(
                limit=10,
                include_internal=True,
            )

        self.assertEqual(exit_code, EXIT_DROPPED)
        printed = json.loads(stderr.getvalue())
        self.assertEqual(printed["status"], "dropped")
        self.assertEqual(printed["reason"], "telegram_attachment_missing")
        # A drop is not a delivery, so it must not be recorded as one.
        self.assertEqual(activity, [])

    def test_transient_exit_when_handler_unreachable(self) -> None:
        submit = _FakeSubmit(
            status=0, response={"reason": "egress endpoint unreachable"}
        )
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:email:53",
                    "channel": "email",
                    "target": "alice@example.com",
                    "message": "working on it",
                },
            )
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stderr", stderr),
                patch("sys.argv", ["main_reply.py", "--spec-file", spec]),
            ):
                exit_code = main()

        self.assertEqual(exit_code, EXIT_TRANSIENT)

    def test_requires_envelope_id_for_non_prefixed_task_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "email:53",
                    "channel": "email",
                    "target": "alice@example.com",
                    "message": "working on it",
                },
            )
            with patch("sys.argv", ["main_reply.py", "--spec-file", spec]):
                with self.assertRaisesRegex(ValueError, "envelope_id is required"):
                    main()

    def test_spec_file_rejects_unknown_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:email:53",
                    "channel": "email",
                    "target": "alice@example.com",
                    "message": "hi",
                    "bogus": "nope",
                },
            )
            with patch("sys.argv", ["main_reply.py", "--spec-file", spec]):
                with self.assertRaisesRegex(ValueError, "unknown fields"):
                    main()

    def test_reaction_via_flags_using_explicit_message_id(self) -> None:
        submit = _FakeSubmit()
        with (
            patch("app.main_reply.submit_egress", submit),
            patch(
                "sys.argv",
                [
                    "main_reply.py",
                    "task:telegram:53",
                    "--channel",
                    "telegram",
                    "--target",
                    "8605042448",
                    "--telegram-reaction",
                    "👍",
                    "--telegram-message-id",
                    "123",
                ],
            ),
        ):
            exit_code = main()

        self.assertEqual(exit_code, 0)
        _, payload = submit.calls[0]
        self.assertEqual(payload["message"]["channel"], "telegram_reaction")
        self.assertEqual(payload["message"]["body"], "👍")
        self.assertEqual(payload["message"]["metadata"], {"message_id": 123})

    def test_unsupported_reaction_is_rejected_before_handler_submission(self) -> None:
        submit = _FakeSubmit()
        with (
            patch("app.main_reply.submit_egress", submit),
            patch(
                "sys.argv",
                [
                    "main_reply.py",
                    "task:telegram:53",
                    "--channel",
                    "telegram",
                    "--target",
                    "8605042448",
                    "--telegram-reaction",
                    "🔎",
                    "--telegram-message-id",
                    "123",
                ],
            ),
        ):
            with self.assertRaisesRegex(
                ValueError,
                "telegram_reaction '🔎' is not supported by Telegram",
            ):
                main()

        self.assertEqual(submit.calls, [])

    def test_spec_file_sends_reaction_and_message(self) -> None:
        submit = _FakeSubmit()
        stdout = io.StringIO()
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:telegram:53",
                    "channel": "telegram",
                    "target": "8605042448",
                    "message": "I found the problem.",
                    "telegram_reaction": "👀",
                    "telegram_message_id": 123,
                    "event_id": "evt:custom:both",
                },
            )
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stdout", stdout),
                patch("sys.argv", ["main_reply.py", "--spec-file", spec]),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(submit.calls), 2)
        reaction = submit.calls[0][1]
        message = submit.calls[1][1]
        self.assertEqual(reaction["event_id"], "evt:custom:both:reaction")
        self.assertEqual(reaction["message"]["channel"], "telegram_reaction")
        self.assertEqual(reaction["message"]["body"], "👀")
        self.assertEqual(message["event_id"], "evt:custom:both:message")
        self.assertEqual(message["message"]["channel"], "telegram")
        self.assertEqual(message["message"]["body"], "I found the problem.")
        printed = json.loads(stdout.getvalue())
        self.assertEqual(len(printed["results"]), 2)

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
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:telegram:53",
                    "channel": "telegram",
                    "target": "8605042448",
                    "message": "The answer still gets sent.",
                    "telegram_reaction": "👀",
                    "telegram_message_id": 123,
                },
            )
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stderr", io.StringIO()),
                patch("sys.argv", ["main_reply.py", "--spec-file", spec]),
            ):
                exit_code = main()

        self.assertEqual(exit_code, EXIT_DROPPED)
        self.assertEqual(len(submit.calls), 2)
        self.assertEqual(submit.calls[1][1]["message"]["channel"], "telegram")

    def test_reaction_via_spec_file_using_task_ledger_message_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"db_path": str(db_path)}), encoding="utf-8"
            )
            ledger = TaskLedgerStore(str(db_path))
            envelope = TaskEnvelope(
                id="telegram:53",
                source="im",
                received_at=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
                actor="8605042448:edsalkeld",
                content="hello",
                attachments=[],
                context_refs=[],
                reply_channel=ReplyChannel(
                    type="telegram",
                    target="8605042448",
                    metadata={"message_id": 456},
                ),
                dedupe_key="telegram:53",
            )
            ledger.record_task(
                TaskQueueMessage.from_envelope(envelope, trace_id="trace:telegram:53")
            )

            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:telegram:53",
                    "channel": "telegram",
                    "target": "8605042448",
                    "telegram_reaction": "👍",
                },
            )
            submit = _FakeSubmit()
            with (
                patch("app.main_reply.submit_egress", submit),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "--spec-file",
                        spec,
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        _, payload = submit.calls[0]
        self.assertEqual(payload["message"]["metadata"], {"message_id": 456})

    def test_spec_file_attachment_message(self) -> None:
        submit = _FakeSubmit()
        with tempfile.TemporaryDirectory() as tmpdir:
            attachment_path = Path(tmpdir) / "menu.pdf"
            attachment_path.write_bytes(b"%PDF-1.4\n")
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:telegram:53",
                    "channel": "telegram",
                    "target": "8605042448",
                    "message": "This week's menu",
                    "attachment_path": str(attachment_path),
                    "attachment_name": "menu.pdf",
                },
            )
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stdout", io.StringIO()),
                patch("sys.argv", ["main_reply.py", "--spec-file", spec]),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        _, payload = submit.calls[0]
        self.assertEqual(payload["message"]["body"], "This week's menu")
        self.assertEqual(payload["message"]["attachment"]["name"], "menu.pdf")
        self.assertEqual(
            payload["message"]["attachment"]["uri"], attachment_path.as_uri()
        )

    def test_records_worker_activity_on_delivery(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text(
                json.dumps({"db_path": str(db_path)}), encoding="utf-8"
            )
            spec = _write_spec(
                Path(tmpdir),
                {
                    "task_id": "task:email:53",
                    "channel": "email",
                    "target": "alice@example.com",
                    "message": "working on it",
                    "event_id": "evt:custom:1",
                },
            )
            submit = _FakeSubmit()
            with (
                patch("app.main_reply.submit_egress", submit),
                patch("sys.stdout", io.StringIO()),
                patch(
                    "sys.argv",
                    [
                        "main_reply.py",
                        "--spec-file",
                        spec,
                        "--config",
                        str(config_path),
                    ],
                ),
            ):
                exit_code = main()

            activity = SQLiteStateStore(str(db_path)).list_recent_worker_activity(
                limit=10,
                include_internal=True,
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(activity), 1)
        self.assertEqual(activity[0]["task_id"], "task:email:53")
        self.assertEqual(activity[0]["phase"], "egress_incremental")
        self.assertEqual(activity[0]["summary"], "incremental egress to email")
        self.assertEqual(activity[0]["detail"]["event_id"], "evt:custom:1")


if __name__ == "__main__":
    unittest.main()
