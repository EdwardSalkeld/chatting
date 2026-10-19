"""Private per-conversation owner for a Codex stdio app-server.

The socket is only a transport. A write-ahead request journal prevents an
uncertain disconnect or process death from silently replaying model actions.
"""

from __future__ import annotations

import fcntl
import ctypes
import json
import os
from pathlib import Path
from queue import Empty, Queue
import signal
import socket
import subprocess
import sys
from threading import Thread
import time
from typing import Callable

from app.model_command import MODELS
from app.models import ExecutionResult


def _parent_death_signal() -> None:
    # The worker runs on Linux. An abruptly killed session owner must not leave
    # a second live app-server writing to the same thread after a restart.
    ctypes.CDLL(None).prctl(1, signal.SIGTERM)
    if os.getppid() == 1:
        os.kill(os.getpid(), signal.SIGTERM)


def _write(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class AppServer:
    def __init__(self, command: list[str]):
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
            preexec_fn=_parent_death_signal if sys.platform == "linux" else None,
        )
        self.events: Queue[tuple[str, str | None]] = Queue()
        self.pending: list[dict] = []
        self.sequence = 0
        for stream, name in (
            (self.process.stdout, "stdout"),
            (self.process.stderr, "stderr"),
        ):
            assert stream is not None
            Thread(target=self._reader, args=(stream, name), daemon=True).start()

    def _reader(self, stream, name: str) -> None:
        try:
            for line in stream:
                self.events.put((name, line))
        finally:
            self.events.put((name, None))

    def send(self, message: dict) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def read(self, deadline: float, emit: Callable[[str, str], None]) -> dict:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("turn timed out")
            try:
                name, line = self.events.get(timeout=remaining)
            except Empty:
                raise TimeoutError("turn timed out") from None
            if line is None:
                if name == "stdout":
                    raise RuntimeError("app-server exited")
                continue
            emit(name, line)
            if name != "stdout":
                continue
            message = json.loads(line)
            if "method" in message and "id" in message:
                self.send(
                    {
                        "id": message["id"],
                        "error": {
                            "code": -32601,
                            "message": "Unattended executor cannot answer client requests",
                        },
                    }
                )
                raise RuntimeError(f"unsupported server request: {message['method']}")
            return message

    def rpc(
        self,
        method: str,
        params: dict,
        deadline: float,
        emit: Callable[[str, str], None],
    ) -> dict:
        self.sequence += 1
        self.send({"id": self.sequence, "method": method, "params": params})
        while True:
            event = self.read(deadline, emit)
            if event.get("id") == self.sequence:
                if "error" in event:
                    raise RuntimeError(f"{method}: {event['error']}")
                return event["result"]
            self.pending.append(event)

    def turn(
        self,
        request: dict,
        thread_id: str,
        deadline: float,
        emit: Callable[[str, str], None],
    ) -> None:
        payload = request["payload"]
        started = self.rpc(
            "turn/start",
            {
                "threadId": thread_id,
                "input": [{"type": "text", "text": json.dumps(payload)}],
                "cwd": request["cwd"],
                "model": MODELS[request["model_tier"] or "high"],
                "approvalPolicy": "never",
            },
            deadline,
            emit,
        )
        turn_id = started["turn"]["id"]
        while True:
            event = self.pending.pop(0) if self.pending else self.read(deadline, emit)
            params = event.get("params", {})
            if (
                event.get("method") == "turn/completed"
                and params.get("threadId") == thread_id
                and params.get("turn", {}).get("id") == turn_id
            ):
                turn = params["turn"]
                if turn["status"] != "completed":
                    raise RuntimeError(f"turn {turn['status']}: {turn.get('error')}")
                return

    def close(self) -> None:
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            # EOF lets Codex flush its rollout and stop tools before exit.
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(self.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.process.wait()
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream:
                stream.close()


class Session:
    def __init__(self, directory: Path, settings: dict):
        self.directory = directory
        self.settings = settings
        self.server: AppServer | None = None
        self.loaded = False

    def execute(
        self, request: dict, emit: Callable[[str, str], None]
    ) -> ExecutionResult:
        key = request["request_id"]
        if len(key) != 64 or any(char not in "0123456789abcdef" for char in key):
            raise ValueError("invalid request id")
        journal = self.directory / f"request-{key}.json"
        if journal.exists():
            saved = json.loads(journal.read_text())
            if "result" in saved:
                result = ExecutionResult(**saved["result"])
                for name in ("stdout", "stderr"):
                    content = getattr(result, name)
                    if content:
                        emit(name, content)
                return result
            return ExecutionResult(
                errors=[
                    "app_server_uncertain_request:inspect_session_journal_before_retry"
                ]
            )
        pending = self.directory / "pending.json"
        if pending.exists():
            previous_key = json.loads(pending.read_text())["request_id"]
            previous = self.directory / f"request-{previous_key}.json"
            if not previous.exists() or "result" not in json.loads(
                previous.read_text()
            ):
                return ExecutionResult(
                    errors=[
                        "app_server_uncertain_request:inspect_session_journal_before_retry"
                    ]
                )
        state_file = self.directory / "thread.json"
        existing_thread = state_file.exists()
        thread_id = (
            json.loads(state_file.read_text())["thread_id"] if existing_thread else None
        )
        captured: dict[str, list[str]] = {"stdout": [], "stderr": []}

        def capture(name: str, content: str) -> None:
            captured[name].append(content)
            emit(name, content)

        deadline = time.monotonic() + request["timeout"]
        try:
            if self.server is None:
                self.server = AppServer(self.settings["command"])
                self.server.rpc(
                    "initialize",
                    {
                        "clientInfo": {
                            "name": "chatting",
                            "title": "Chatting",
                            "version": "0.1.0",
                        }
                    },
                    deadline,
                    capture,
                )
                self.server.send({"method": "initialized"})
            if not self.loaded:
                params = {"cwd": request["cwd"], "approvalPolicy": "never"}
                if request.get("sandbox_mode") is not None:
                    # Resume otherwise retains the sandbox saved with the thread,
                    # even when the process command sets a different default.
                    params["sandbox"] = request["sandbox_mode"]
                if thread_id:
                    params["threadId"] = thread_id
                    params["excludeTurns"] = True
                result = self.server.rpc(
                    "thread/resume" if thread_id else "thread/start",
                    params,
                    deadline,
                    capture,
                )
                thread_id = result["thread"]["id"]
                _write(state_file, {"thread_id": thread_id})
                self.loaded = True
            else:
                assert thread_id is not None
            # Rebuild history only when creating the conversation, never on resume.
            if existing_thread:
                request["payload"].pop("recent_history", None)
            _write(
                journal,
                {
                    "state": "submitted",
                    "task_id": request["payload"]["task"]["task_id"],
                    "thread_id": thread_id,
                },
            )
            _write(pending, {"request_id": key})
            self.server.turn(request, thread_id, deadline, capture)
            escalation_reason = None
            escalation_path = request.get("escalation_path")
            if escalation_path and Path(escalation_path).exists():
                escalation = json.loads(Path(escalation_path).read_text())
                if escalation.get("task_id") == request["payload"]["task"]["task_id"]:
                    reason = escalation.get("reason")
                    if isinstance(reason, str) and reason.strip():
                        escalation_reason = reason.strip()[:4000]
            result = ExecutionResult(
                errors=[],
                stdout="".join(captured["stdout"]),
                stderr="".join(captured["stderr"]),
                escalation_reason=escalation_reason,
            )
        except (OSError, ValueError, RuntimeError, TimeoutError, KeyError) as exc:
            if self.server:
                self.server.close()
                self.server = None
                self.loaded = False
            result = ExecutionResult(
                errors=[f"app_server_error:{exc}"],
                stdout="".join(captured["stdout"]),
                stderr="".join(captured["stderr"]),
            )
        if journal.exists():
            _write(journal, {"result": result.to_dict()})
            # A leftover marker is harmless: the next request checks the result
            # journal before deciding whether an earlier turn is uncertain.
            pending.unlink(missing_ok=True)
        return result

    def close(self) -> None:
        if self.server:
            self.server.close()


def serve(directory: Path) -> None:
    settings = json.loads((directory / "settings.json").read_text())
    with (directory / "owner.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock.seek(0)
        lock.truncate()
        lock.write(str(os.getpid()))
        lock.flush()
        path = directory / "rpc.sock"
        path.unlink(missing_ok=True)
        session = Session(directory, settings)
        owns_path = True
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(path))
                path.chmod(0o600)
                listener.listen(8)
                listener.settimeout(settings["idle_seconds"])
                while True:
                    try:
                        connection, _ = listener.accept()
                    except TimeoutError:
                        return
                    with connection:
                        connection.settimeout(10)
                        with connection.makefile("rb") as stream:
                            request = json.loads(stream.readline(16 * 1024 * 1024))
                        connected = True

                        def send(value: dict) -> None:
                            nonlocal connected
                            if not connected:
                                return
                            try:
                                connection.sendall(json.dumps(value).encode() + b"\n")
                            except OSError:
                                # Finish and journal even if the supervisor disconnects.
                                connected = False

                        if request["generation"] != settings["generation"]:
                            session.close()
                            session.server = None
                            path.unlink(missing_ok=True)
                            owns_path = False
                            fcntl.flock(lock, fcntl.LOCK_UN)
                            send(
                                {
                                    "result": ExecutionResult(
                                        errors=[
                                            "app_server_config_changed:retry_with_new_owner"
                                        ]
                                    ).to_dict()
                                }
                            )
                            return
                        result = session.execute(
                            request,
                            lambda name, content: send(
                                {"stream": name, "content": content}
                            ),
                        )
                        send({"result": result.to_dict()})
        finally:
            session.close()
            if owns_path:
                path.unlink(missing_ok=True)


if __name__ == "__main__":

    def terminate(signum, frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    serve(Path(sys.argv[1]))
