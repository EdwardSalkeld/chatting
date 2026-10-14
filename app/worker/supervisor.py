"""One long-lived executor slot, controlled through a private Unix socket.

The manager can disappear at any point. Claiming and processing a task both
belong to the supervisor, so reconnecting never needs to repeat a handoff.
Supervisor crashes are deliberately fatal; there is no supervisor recovery.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
from threading import Lock, Thread
import time
from dataclasses import replace

from app.state import SQLiteStateStore
from app.worker.activity import WorkerActivityMonitor
from app.worker.executor import CodexExecutor

LOGGER = logging.getLogger(__name__)
PROTOCOL_VERSION = 1
MAX_REQUEST_BYTES = 65536
# These change between service invocations without changing executor settings.
_SERVICE_ENVIRONMENT = {
    "INVOCATION_ID",
    "JOURNAL_STREAM",
    "NOTIFY_SOCKET",
    "SYSTEMD_EXEC_PID",
    "WATCHDOG_PID",
    "MAINPID",
    "MANAGERPID",
    "SHLVL",
    "_",
    "PWD",
    "OLDPWD",
}


class SupervisorError(RuntimeError):
    pass


def request(path: Path, command: str, **fields: object) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        # SQLite's busy timeout is five seconds. Wait long enough to receive
        # the result of a contended claim rather than abandoning the manager.
        client.settimeout(10)
        client.connect(str(path))
        client.sendall(
            json.dumps(
                {"version": PROTOCOL_VERSION, "command": command, **fields}
            ).encode()
            + b"\n"
        )
        with client.makefile("rb") as stream:
            response = json.loads(stream.readline())
    if response.get("version") != PROTOCOL_VERSION or response.get("error") is not None:
        raise SupervisorError(f"{path}: {response}")
    return response


class Supervisor:
    def __init__(self, *, slot: int, settings: dict) -> None:
        # Import here to keep the manager/supervisor entrypoints independent.
        from app.worker.main import _build_executor

        self.slot = slot
        self.settings = settings
        self.store = SQLiteStateStore(settings["db_path"])
        self.monitor = WorkerActivityMonitor(store=self.store)
        self.executor = _build_executor(
            argparse.Namespace(**settings["args"]), settings["config"]
        )
        if isinstance(self.executor, CodexExecutor):
            self.executor = replace(self.executor, history_store=self.store)
        self.lock = Lock()
        self.state = "idle"
        self.task_id: str | None = None
        self.error: str | None = None

    def handle(self, payload: dict) -> dict:
        if payload.get("version") != PROTOCOL_VERSION:
            raise ValueError("unsupported supervisor protocol")
        command = payload.get("command")
        with self.lock:
            if command == "start":
                if self.state != "idle":
                    raise ValueError("slot is not idle")
                try:
                    inbox_task = self.store.claim_next_inbox_task()
                except sqlite3.OperationalError as exc:
                    if getattr(exc, "sqlite_errorcode", 0) & 0xFF not in (
                        sqlite3.SQLITE_BUSY,
                        sqlite3.SQLITE_LOCKED,
                    ):
                        raise
                    # The connection closes and rolls back the failed claim.
                    # Leave the slot idle so the next manager tick retries.
                    LOGGER.warning("supervisor_claim_busy slot=%s", self.slot)
                    inbox_task = None
                if inbox_task is not None:
                    self.task_id = inbox_task.task_message.task_id
                    self.state = "running"
                    Thread(target=self._run, args=(inbox_task,), daemon=True).start()
            elif command == "ack":
                if self.state != "finished" or payload.get("task_id") != self.task_id:
                    raise ValueError("completion acknowledgement does not match")
                self.task_id = None
                self.state = "idle"
            elif command == "shutdown":
                if self.state != "idle":
                    raise ValueError("cannot shut down an occupied slot")
                self.state = "stopped"
            elif command != "status":
                raise ValueError("unknown supervisor command")
            return {
                "version": PROTOCOL_VERSION,
                "slot": self.slot,
                "pid": os.getpid(),
                "db_path": self.settings["db_path"],
                "generation": self.settings["generation"],
                "state": self.state,
                "task_id": self.task_id,
                "error": self.error,
                "active_executors": self.monitor._current_executors(),
            }

    def _run(self, inbox_task) -> None:
        from app.worker.main import _process_claimed_task

        try:
            _process_claimed_task(
                store=self.store,
                inbox_task=inbox_task,
                executor=self.executor,
                max_attempts=self.settings["max_attempts"],
                activity_monitor=self.monitor,
                handler_egress_url=None,
            )
        except Exception as exc:
            LOGGER.exception("supervisor_task_failed slot=%s", self.slot)
            with self.lock:
                # Do not release uncertain work for another slot to repeat.
                self.error = str(exc)
                self.state = "failed"
        else:
            with self.lock:
                self.state = "finished"


class SupervisorPool:
    """Discover all existing slots before recovering any inbox leases."""

    def __init__(self, *, directory: Path, size: int, settings: dict) -> None:
        self.directory = directory.resolve()
        self.size = size
        app_root = Path(__file__).resolve().parents[1]
        code_digest = hashlib.sha256()
        for source in sorted(app_root.rglob("*.py")):
            code_digest.update(str(source.relative_to(app_root)).encode())
            code_digest.update(source.read_bytes())
        generation = hashlib.sha256(
            json.dumps(
                {
                    "settings": settings,
                    "code": str(Path(__file__).resolve()),
                    "code_digest": code_digest.hexdigest(),
                    "python": sys.executable,
                    "environment": {
                        key: value
                        for key, value in os.environ.items()
                        if key not in _SERVICE_ENVIRONMENT
                    },
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        self.settings = {**settings, "generation": generation}
        self.children: list[subprocess.Popen] = []
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        if (
            self.directory.stat().st_uid != os.getuid()
            or self.directory.stat().st_mode & 0o077
        ):
            raise SupervisorError(
                "supervisor directory must be private to the worker user"
            )
        if len(os.fsencode(self.directory / "supervisor-9999.sock")) > 103:
            raise SupervisorError("supervisor_dir is too long for Unix sockets")
        existing = {
            int(path.stem.split("-")[1])
            for path in self.directory.glob("supervisor-*.lock")
        }
        self.slots = sorted(existing | set(range(1, size + 1)))
        for slot in self.slots:
            self._connect_or_start(slot)

    def path(self, slot: int) -> Path:
        return self.directory / f"supervisor-{slot}.sock"

    def _connect_or_start(self, slot: int) -> None:
        try:
            self.status(slot)
            return
        except (OSError, ValueError):
            pass
        marker = self.directory / f"supervisor-{slot}.lock"
        # A lock file remains after a crash. Never silently replace a dead or
        # unresponsive supervisor; resolving that failure is an operator action.
        if marker.exists():
            raise SupervisorError(f"supervisor {slot} is unavailable; inspect {marker}")
        settings_path = self.directory / f"supervisor-{slot}.json"
        settings_path.write_text(json.dumps(self.settings), encoding="utf-8")
        settings_path.chmod(0o600)
        log_path = self.directory / f"supervisor-{slot}.log"
        with log_path.open("ab") as log:
            child = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "app.worker.supervisor",
                    "--slot",
                    str(slot),
                    "--directory",
                    str(self.directory),
                ],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
            )
        self.children.append(child)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise SupervisorError(f"supervisor {slot} exited; inspect {log_path}")
            try:
                self.status(slot)
                return
            except (OSError, ValueError):
                time.sleep(0.05)
        raise SupervisorError(f"supervisor {slot} did not open its socket")

    def status(self, slot: int) -> dict:
        status = request(self.path(slot), "status")
        if status["slot"] != slot or status["db_path"] != self.settings["db_path"]:
            raise SupervisorError("supervisor identity does not match manager")
        if status["state"] == "failed":
            raise SupervisorError(f"supervisor {slot} failed: {status['error']}")
        return status

    def statuses(self) -> list[dict]:
        for child in self.children:
            child.poll()  # Reap children we started that have since stopped.
        return [self.status(slot) for slot in self.slots]

    def tick(self) -> None:
        for status in self.statuses():
            slot = status["slot"]
            if status["state"] == "finished":
                request(self.path(slot), "ack", task_id=status["task_id"])
                status = self.status(slot)
            if status["state"] == "idle":
                if slot > self.size:
                    request(self.path(slot), "shutdown")
                    self.slots.remove(slot)
                else:
                    if status["generation"] != self.settings["generation"]:
                        request(self.path(slot), "shutdown")
                        deadline = time.monotonic() + 5
                        while (self.directory / f"supervisor-{slot}.lock").exists():
                            if time.monotonic() >= deadline:
                                raise SupervisorError(f"supervisor {slot} did not stop")
                            time.sleep(0.025)
                        self._connect_or_start(slot)
                    request(self.path(slot), "start")

    def drain(self) -> None:
        """Used by finite smoke runs; normal manager exit leaves slots alone."""
        while any(s["state"] == "running" for s in self.statuses()):
            time.sleep(0.05)
        for status in self.statuses():
            if status["state"] == "finished":
                request(self.path(status["slot"]), "ack", task_id=status["task_id"])
            request(self.path(status["slot"]), "shutdown")
        for child in self.children:
            child.wait(timeout=5)


def main() -> int:
    from app.worker.main import _configure_logging

    _configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slot", type=int)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument(
        "--shutdown",
        action="store_true",
        help="Drain and stop slots after stopping the manager.",
    )
    args = parser.parse_args()
    if args.shutdown:
        paths = sorted(args.directory.glob("supervisor-*.sock"))
        if not paths:
            return 0
        db_path = request(paths[0], "status")["db_path"]
        with Path(f"{db_path}.worker.lock").open("a+") as manager_lock:
            try:
                fcntl.flock(manager_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise SupervisorError(
                    "stop the manager before shutting down supervisors"
                ) from exc
            for path in paths:
                while (status := request(path, "status"))["state"] == "running":
                    time.sleep(0.1)
                if status["state"] == "finished":
                    request(path, "ack", task_id=status["task_id"])
                request(path, "shutdown")
        return 0
    if args.slot is None or args.slot <= 0:
        parser.error("a positive --slot is required unless using --shutdown")
    stem = args.directory / f"supervisor-{args.slot}"
    marker = stem.with_suffix(".lock")
    # Never unlink a socket until this process owns the slot lock.
    with marker.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock.seek(0)
        previous = lock.read()
        if previous:
            raise SupervisorError(
                "slot already has an owner; supervisor recovery is disabled"
            )
        lock.write(str(os.getpid()))
        lock.flush()
        settings = json.loads(stem.with_suffix(".json").read_text())
        supervisor = Supervisor(slot=args.slot, settings=settings)
        path = stem.with_suffix(".sock")
        path.unlink(missing_ok=True)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(path))
            path.chmod(0o600)
            server.listen(8)
            while supervisor.state != "stopped":
                connection, _ = server.accept()
                with connection:
                    connection.settimeout(3)
                    try:
                        with connection.makefile("rb") as stream:
                            raw = stream.readline(MAX_REQUEST_BYTES + 1)
                        if len(raw) > MAX_REQUEST_BYTES or not raw.endswith(b"\n"):
                            raise ValueError("invalid supervisor request")
                        payload = json.loads(raw)
                        if not isinstance(payload, dict):
                            raise ValueError("supervisor request must be an object")
                        response = supervisor.handle(payload)
                    except (ValueError, OSError) as exc:
                        response = {"version": PROTOCOL_VERSION, "error": str(exc)}
                    try:
                        connection.sendall(json.dumps(response).encode() + b"\n")
                    except OSError:
                        # A vanished manager does not cancel its request.
                        pass
        path.unlink(missing_ok=True)
        marker.unlink()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
