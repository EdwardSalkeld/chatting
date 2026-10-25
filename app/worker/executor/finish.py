"""Per-execution Unix socket owned by the supervisor, with durable finish records."""

from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import socket
import tempfile


class FinishSession:
    def __init__(self, task_id: str, cwd: str | None):
        self.task_id = task_id
        self.execution_id = secrets.token_hex(16)
        self.temporary = tempfile.TemporaryDirectory(prefix="chatting-finish-")
        self.path = str(Path(self.temporary.name) / "finish.sock")
        self.record_dir = Path(cwd or Path.cwd()) / ".chatting-finish"
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.bind(self.path)
        self.socket.listen(4)
        self.socket.setblocking(False)
        self.requested = False

    def contract(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "execution_id": self.execution_id,
            "socket_path": self.path,
            "command": "python3 -P -m app.main_finish",
            "instructions": (
                "Completion requires an explicit termination request, independently of messaging. "
                "When all work and any replies are complete, run python3 -P -m app.main_finish "
                "and WAIT. The supervisor records completion and kills this executor and its "
                "children. Never exit yourself: any exit without this request restarts your work. "
                "The command uses this execution's credentials from the environment."
            ),
        }

    def environment(self) -> dict[str, str]:
        return {
            "CHATTING_FINISH_SOCKET": self.path,
            "CHATTING_FINISH_TASK_ID": self.task_id,
            "CHATTING_FINISH_EXECUTION_ID": self.execution_id,
        }

    def poll(self) -> bool:
        try:
            connection, _ = self.socket.accept()
        except BlockingIOError:
            return False
        with connection:
            connection.settimeout(0.2)
            try:
                data = bytearray()
                while b"\n" not in data and len(data) <= 4096:
                    chunk = connection.recv(4096)
                    if not chunk:
                        return False
                    data.extend(chunk)
                request = json.loads(data)
                if request != {
                    "task_id": self.task_id,
                    "execution_id": self.execution_id,
                }:
                    connection.sendall(b'{"error":"invalid execution"}\n')
                    return False
            except OSError, ValueError:
                return False
            self.record_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            with (self.record_dir / f"{self.execution_id}.json").open("x") as record:
                json.dump(request, record)
                record.flush()
                os.fsync(record.fileno())
            # Sync the parent as well, including the directory entry if this
            # execution created the record directory for the first time.
            for directory in (self.record_dir, self.record_dir.parent):
                directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            self.requested = True
            return True

    def close(self) -> None:
        self.socket.close()
        self.temporary.cleanup()
