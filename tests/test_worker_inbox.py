import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.broker import PickedMessage, TaskQueueMessage
from app.models import AttachmentRef, ReplyChannel, TaskEnvelope
from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor
from app.worker.inbox import InboxCollector


def _telegram_task(
    number: int,
    *,
    target: str = "chat-a",
    content: str | None = None,
    attachment: AttachmentRef | None = None,
    thread_id: int | None = None,
    reply_to_message_id: int | None = None,
    original_content: str | None = None,
) -> TaskQueueMessage:
    metadata: dict[str, object] = {"message_id": 100 + number}
    if thread_id is not None:
        metadata["message_thread_id"] = thread_id
    if reply_to_message_id is not None:
        metadata["reply_to_message_id"] = reply_to_message_id
    if original_content is not None:
        metadata["original_content"] = original_content
    envelope = TaskEnvelope(
        id=f"telegram:{number}",
        source="im",
        received_at=datetime(2026, 8, 16, 10, 0, tzinfo=timezone.utc)
        + timedelta(seconds=number),
        actor="8605042448:edsalkeld",
        content=content or f"message {number}",
        attachments=[] if attachment is None else [attachment],
        context_refs=[],
        reply_channel=ReplyChannel(type="telegram", target=target, metadata=metadata),
        dedupe_key=f"telegram:{number}",
    )
    return replace(
        TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:telegram:{number}"),
        work_item_id="item_"
        + target.replace("-", "_")
        + (f"_topic_{thread_id}" if thread_id is not None else ""),
    )


class WorkerInboxTests(unittest.TestCase):
    def test_parallel_claims_skip_busy_item_and_keep_its_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            first = _telegram_task(1, target="chat-a")
            followup = _telegram_task(2, target="chat-a")
            other = _telegram_task(3, target="chat-b")
            for task in (first, followup, other):
                store.stage_inbox_task(task)

            claimed_first = store.claim_next_inbox_task()
            claimed_other = store.claim_next_inbox_task()
            assert claimed_first is not None and claimed_other is not None
            self.assertEqual(claimed_first.task_message.task_id, first.task_id)
            self.assertEqual(claimed_other.task_message.task_id, other.task_id)
            self.assertIsNone(store.claim_next_inbox_task())
            self.assertEqual(store.inbox_queue_summary(), {"queued": 1, "running": 2})

            store.finish_inbox_task(parent_task_id=first.task_id)
            claimed_followup = store.claim_next_inbox_task()
            assert claimed_followup is not None
            self.assertEqual(claimed_followup.task_message.task_id, followup.task_id)

    def test_delivered_active_task_keeps_lease_until_finished(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            first = _telegram_task(1)
            followup = _telegram_task(2)
            store.stage_inbox_task(first)
            store.stage_inbox_task(followup)
            store.claim_next_inbox_task()
            store.mark_inbox_reply_delivered(parent_task_id=first.task_id)
            self.assertIsNone(store.claim_next_inbox_task())
            store.finish_inbox_task(parent_task_id=first.task_id)
            claimed = store.claim_next_inbox_task()
            assert claimed is not None
            self.assertEqual(claimed.task_message.task_id, followup.task_id)

    def test_simultaneous_claims_do_not_take_same_work_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            first = _telegram_task(1)
            second = _telegram_task(2)
            store.stage_inbox_task(first)
            store.stage_inbox_task(second)
            with ThreadPoolExecutor(max_workers=2) as pool:
                claims = list(
                    pool.map(lambda _: store.claim_next_inbox_task(), range(2))
                )
            self.assertEqual(
                [claim.task_message.task_id for claim in claims if claim is not None],
                [first.task_id],
            )

    def test_staging_records_clean_inbound_turn_and_reply_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            anchor = _telegram_task(1, original_content="The old subject")
            reply = _telegram_task(
                2,
                content="Recent conversation context...\n\nCurrent message: Continue this",
                original_content="Continue this",
                reply_to_message_id=101,
            )
            store.stage_inbox_task(anchor)
            store.stage_inbox_task(reply)

            turns = store.list_telegram_history_around(
                target="chat-a", message_id=101, before=0, after=4
            )

            self.assertEqual([turn.message_id for turn in turns], [101, 102])
            self.assertEqual(turns[0].content, "The old subject")
            self.assertEqual(turns[1].content, "Continue this")
            self.assertEqual(turns[1].reply_to_message_id, 101)
            self.assertEqual(turns[1].sender, "8605042448:edsalkeld")

    def test_records_outbound_turn_using_telegram_message_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            inbound = _telegram_task(1)
            store.stage_inbox_task(inbound)
            store.record_telegram_outbound(
                target="chat-a",
                message_id=102,
                content="A reply",
                attachment=None,
                task_id=inbound.task_id,
                event_id="evt:reply:1",
                occurred_at=datetime(2026, 8, 16, 10, 1, 2, tzinfo=timezone.utc),
            )

            turns = store.list_telegram_history_around(
                target="chat-a", message_id=102, before=2, after=0
            )

            self.assertEqual([turn.role for turn in turns], ["user", "assistant"])
            self.assertEqual(turns[-1].content, "A reply")
            self.assertEqual(turns[-1].event_id, "evt:reply:1")

    def test_collector_persists_before_acknowledging_broker(self) -> None:
        class RecordingBroker:
            def __init__(self, store: SQLiteStateStore, task: TaskQueueMessage) -> None:
                self.store = store
                self.task = task
                self.acks: list[tuple[str, str]] = []

            def pickup_json(self, queue_name, *, timeout_seconds, wait_seconds):
                del queue_name, timeout_seconds, wait_seconds
                return PickedMessage(guid="guid-1", payload=self.task.to_dict())

            def ack(self, queue_name, guid):
                # The durable row must exist before transport ownership ends.
                self.assert_staged()
                self.acks.append((queue_name, guid))

            def assert_staged(self):
                assert self.store.get_inbox_task(task_id=self.task.task_id) is not None

        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            task = _telegram_task(1)
            broker = RecordingBroker(store, task)
            collector = InboxCollector(
                broker=broker,  # type: ignore[arg-type]
                store=store,
                activity_monitor=WorkerActivityMonitor(store=store),
                pickup_timeout_seconds=20,
            )

            self.assertTrue(collector.collect_once())
            self.assertEqual(broker.acks, [("chatting.tasks.v1", "guid-1")])

    def test_claims_only_newer_turns_from_the_same_opaque_conversation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            parent = _telegram_task(1)
            followup_one = _telegram_task(2)
            other_chat = _telegram_task(3, target="chat-b")
            followup_two = _telegram_task(4)
            for task in (parent, followup_one, other_chat, followup_two):
                self.assertTrue(store.stage_inbox_task(task))

            active = store.claim_next_inbox_task()
            assert active is not None
            self.assertTrue(active.conversation_id.startswith("conv_"))
            self.assertNotIn("chat-a", active.conversation_id)

            claimed = store.claim_conversation_followups(parent_task_id=parent.task_id)

            self.assertEqual(
                [item.task_message.task_id for item in claimed],
                [followup_one.task_id, followup_two.task_id],
            )
            self.assertTrue(
                store.has_unresolved_attached_followups(parent_task_id=parent.task_id)
            )
            self.assertEqual(
                store.get_inbox_task(task_id=other_chat.task_id).state,
                "pending",
            )

    def test_topic_routes_are_separate_conversations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            parent = _telegram_task(1, thread_id=10)
            other_topic = _telegram_task(2, thread_id=11)
            store.stage_inbox_task(parent)
            store.stage_inbox_task(other_topic)
            store.claim_next_inbox_task()

            claimed = store.claim_conversation_followups(parent_task_id=parent.task_id)

            self.assertEqual(claimed, [])

    def test_early_reaction_does_not_close_the_peek_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            parent = _telegram_task(1)
            store.stage_inbox_task(parent)
            store.claim_next_inbox_task()

            store.mark_inbox_reply_delivered(parent_task_id=parent.task_id)

            self.assertEqual(
                store.get_inbox_task(task_id=parent.task_id).state, "active"
            )
            followup = _telegram_task(2)
            store.stage_inbox_task(followup)
            claimed = store.claim_conversation_followups(parent_task_id=parent.task_id)
            self.assertEqual(
                [item.task_message.task_id for item in claimed], [followup.task_id]
            )

    def test_pending_task_with_a_delivered_reply_closes_without_execution(self) -> None:
        """A direct reply must not leave an unclaimed task available to replay."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            task = _telegram_task(1)
            store.stage_inbox_task(task)

            self.assertTrue(
                store.mark_inbox_reply_delivered(parent_task_id=task.task_id)
            )

            pending = store.get_inbox_task(task_id=task.task_id)
            assert pending is not None
            self.assertEqual(pending.state, "closing")

            recovered = store.claim_next_inbox_task()
            assert recovered is not None
            self.assertEqual(recovered.task_message.task_id, task.task_id)
            self.assertEqual(recovered.state, "closing")

    def test_standalone_document_survives_claim_and_bundle_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            parent = _telegram_task(1)
            document = _telegram_task(
                2,
                content="[document attached: rota.pdf]",
                attachment=AttachmentRef(
                    uri="file:///var/lib/chatting/attachments/rota.pdf",
                    name="rota.pdf",
                ),
            )
            store.stage_inbox_task(parent)
            store.stage_inbox_task(document)
            store.claim_next_inbox_task()
            claimed = store.claim_conversation_followups(parent_task_id=parent.task_id)

            self.assertEqual(
                claimed[0].task_message.envelope.attachments[0].name, "rota.pdf"
            )
            self.assertTrue(
                store.mark_inbox_reply_delivered(parent_task_id=parent.task_id)
            )
            closing = store.list_closing_inbox_followups(parent_task_id=parent.task_id)
            self.assertEqual(
                closing[0].task_message.envelope.content,
                "[document attached: rota.pdf]",
            )
            completed = store.finish_inbox_task(parent_task_id=parent.task_id)
            self.assertEqual(
                [item.task_message.task_id for item in completed], [document.task_id]
            )
            self.assertEqual(
                store.get_inbox_task(task_id=document.task_id).state, "completed"
            )

    def test_reply_for_attached_followup_closes_entire_bundle(self) -> None:
        """A recovery reply scoped to a follow-up must not replay that turn."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            parent = _telegram_task(1)
            followup = _telegram_task(2, content="Known. Migrating to new host")
            store.stage_inbox_task(parent)
            store.stage_inbox_task(followup)
            store.claim_next_inbox_task()
            store.claim_conversation_followups(parent_task_id=parent.task_id)

            self.assertTrue(
                store.mark_inbox_reply_delivered(parent_task_id=followup.task_id)
            )
            self.assertEqual(
                store.get_inbox_task(task_id=parent.task_id).state, "closing"
            )
            self.assertEqual(
                store.get_inbox_task(task_id=followup.task_id).state, "closing"
            )

            store.finish_inbox_task(parent_task_id=parent.task_id)

            self.assertEqual(
                store.get_inbox_task(task_id=parent.task_id).state, "completed"
            )
            self.assertEqual(
                store.get_inbox_task(task_id=followup.task_id).state, "completed"
            )
            self.assertIsNone(store.claim_next_inbox_task())

    def test_restart_requeues_unfinished_work_but_preserves_delivered_bundle(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteStateStore(str(Path(tmpdir) / "worker.db"))
            unfinished = _telegram_task(1, target="unfinished")
            delivered = _telegram_task(2, target="delivered")
            delivered_followup = _telegram_task(3, target="delivered")
            for task in (unfinished, delivered, delivered_followup):
                store.stage_inbox_task(task)
            store.claim_next_inbox_task()
            store.claim_next_inbox_task()
            store.claim_conversation_followups(parent_task_id=delivered.task_id)
            store.mark_inbox_reply_delivered(parent_task_id=delivered.task_id)

            store.recover_inbox_tasks()

            self.assertEqual(
                store.get_inbox_task(task_id=unfinished.task_id).state, "pending"
            )
            recovered = store.claim_next_inbox_task()
            assert recovered is not None
            self.assertEqual(recovered.task_message.task_id, delivered.task_id)
            self.assertEqual(recovered.state, "closing")


if __name__ == "__main__":
    unittest.main()
