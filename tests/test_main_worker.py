import threading
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.broker import TaskQueueMessage
from app.main_worker import DEFAULT_MAX_PARALLEL_LANES, LaneSerialExecutor, PickedTask, _load_config, _parse_args, _task_lane_key
from app.models import ReplyChannel, TaskEnvelope


def _build_picked_task(*, envelope_id: str, source: str = "im", reply_channel_type: str) -> PickedTask:
    envelope = TaskEnvelope(
        id=envelope_id,
        source=source,
        received_at=datetime(2026, 3, 16, 13, 0, tzinfo=timezone.utc),
        actor="tester",
        content=f"task for {reply_channel_type}",
        attachments=[],
        context_refs=[],
        policy_profile="default",
        reply_channel=ReplyChannel(type=reply_channel_type, target="target"),
        dedupe_key=envelope_id,
    )
    return PickedTask(
        guid=f"guid:{envelope_id}",
        task_message=TaskQueueMessage.from_envelope(envelope, trace_id=f"trace:{envelope_id}"),
    )


class MainWorkerLaneTests(unittest.TestCase):
    def test_task_lane_key_uses_reply_channel_type(self) -> None:
        picked_task = _build_picked_task(envelope_id="telegram:1", source="im", reply_channel_type="telegram")

        self.assertEqual(_task_lane_key(picked_task.task_message), "telegram")

    def test_lane_executor_serializes_tasks_with_same_lane(self) -> None:
        first_task = _build_picked_task(envelope_id="email:1", source="email", reply_channel_type="email")
        second_task = _build_picked_task(envelope_id="email:2", source="email", reply_channel_type="email")
        first_started = threading.Event()
        second_started = threading.Event()
        release_first = threading.Event()
        started_task_ids: list[str] = []
        started_lock = threading.Lock()

        def handler(picked_task: PickedTask, lane_key: str) -> None:
            del lane_key
            with started_lock:
                started_task_ids.append(picked_task.task_message.task_id)
            if picked_task.task_message.task_id.endswith("email:1"):
                first_started.set()
                release_first.wait(timeout=1.0)
                return
            second_started.set()

        executor = LaneSerialExecutor(max_workers=2, handler=handler)
        try:
            executor.submit(first_task)
            self.assertTrue(first_started.wait(timeout=1.0))

            executor.submit(second_task)
            self.assertFalse(second_started.wait(timeout=0.2))

            release_first.set()
            executor.wait_for_idle()
        finally:
            executor.shutdown()

        self.assertEqual(
            started_task_ids,
            [first_task.task_message.task_id, second_task.task_message.task_id],
        )

    def test_lane_executor_runs_different_lanes_in_parallel(self) -> None:
        telegram_task = _build_picked_task(envelope_id="telegram:1", reply_channel_type="telegram")
        github_task = _build_picked_task(envelope_id="webhook:1", source="webhook", reply_channel_type="github")
        release_tasks = threading.Event()
        overlap_detected = threading.Event()
        started_count = 0
        started_lock = threading.Lock()
        active_count = 0
        active_lock = threading.Lock()

        def handler(picked_task: PickedTask, lane_key: str) -> None:
            del picked_task, lane_key
            nonlocal started_count, active_count
            with started_lock:
                started_count += 1
            with active_lock:
                active_count += 1
                if active_count == 2:
                    overlap_detected.set()
            release_tasks.wait(timeout=1.0)
            with active_lock:
                active_count -= 1

        executor = LaneSerialExecutor(max_workers=2, handler=handler)
        try:
            executor.submit(telegram_task)
            executor.submit(github_task)

            self.assertTrue(overlap_detected.wait(timeout=1.0))
            release_tasks.set()
            executor.wait_for_idle()
        finally:
            executor.shutdown()

        self.assertEqual(started_count, 2)


class MainWorkerConfigTests(unittest.TestCase):
    def test_parse_args_accepts_max_parallel_lanes(self) -> None:
        with patch("sys.argv", ["app.main_worker", "--max-parallel-lanes", "3"]):
            args = _parse_args()

        self.assertEqual(args.max_parallel_lanes, 3)

    def test_load_config_allows_max_parallel_lanes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "worker.json"
            config_path.write_text('{"max_parallel_lanes": 2}', encoding="utf-8")

            config = _load_config(str(config_path))

        self.assertEqual(config["max_parallel_lanes"], 2)

    def test_default_max_parallel_lanes_matches_expected_lane_count_budget(self) -> None:
        self.assertEqual(DEFAULT_MAX_PARALLEL_LANES, 4)


if __name__ == "__main__":
    unittest.main()
