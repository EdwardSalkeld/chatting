import json
import os
import socket
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.models import ReplyChannel, TaskEnvelope
from app.worker.executor.codex import _task_payload
from app.worker.executor.lane_isolation import reply_relay


def _envelope() -> TaskEnvelope:
    return TaskEnvelope(
        id="telegram:12", source="im",
        received_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
        actor="test", content="hello", attachments=[], context_refs=[],
        reply_channel=ReplyChannel(type="telegram", target="123"),
        dedupe_key="telegram:12",
    )


class LaneReplyRelayTests(unittest.TestCase):
    def test_relay_allows_only_active_task_and_destination(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            allowed = {
                "task_id": "task:telegram:12", "channel": "telegram",
                "target": "123", "message": "hello",
            }
            with patch(
                "app.worker.executor.lane_isolation._run_reply",
                return_value={"exit_code": 0, "stdout": "sent", "stderr": ""},
            ) as run_reply:
                with reply_relay(
                    directory, uid=os.getuid(), gid=os.getgid(),
                    envelope=_envelope(), worker_env={},
                ):
                    self.assertEqual(self._request(directory, allowed)["exit_code"], 0)
                    for change in (
                        {"task_id": "task:telegram:other"},
                        {"target": "another-chat"},
                        {"attachment_path": "/etc/passwd"},
                        {"envelope_id": "other"},
                    ):
                        bad = dict(allowed, **change)
                        self.assertEqual(self._request(directory, bad)["exit_code"], 2)
                self.assertEqual(run_reply.call_count, 1)

    @staticmethod
    def _request(directory: Path, spec: dict[str, object]) -> dict[str, object]:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(directory / ".reply.sock"))
            client.sendall(json.dumps(spec).encode())
            client.shutdown(socket.SHUT_WR)
            result = bytearray()
            while chunk := client.recv(65536):
                result.extend(chunk)
        return json.loads(result)

    def test_isolated_payload_omits_shared_checkout_path(self) -> None:
        envelope = _envelope()
        envelope.context_refs.append("repo:/srv/chatting/workspace/chatting")
        with patch(
            "app.worker.executor.codex._repository_sources",
            return_value=["https://github.com/EdwardSalkeld/chatting.git"],
        ):
            payload = _task_payload(
                envelope, current_time=datetime.now(timezone.utc),
                executor_working_dir="/tmp/lane", isolated=True,
            )
        self.assertEqual(payload["task"]["context"], [])
        self.assertEqual(
            payload["task"]["repository_sources"],
            ["https://github.com/EdwardSalkeld/chatting.git"],
        )
