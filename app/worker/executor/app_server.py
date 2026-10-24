"""Opt-in persistent Codex conversations, independent of the exec backend."""

from __future__ import annotations

from dataclasses import dataclass, replace
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from threading import Thread
from typing import Callable, cast

from app.models import ExecutionResult, TaskEnvelope
from app.worker.executor.codex import CodexExecutor, _executor_env, _task_payload

LOGGER = logging.getLogger(__name__)


def _write_migration_file(path: Path, content: bytes) -> None:
    temporary = path.with_name(path.name + ".migrating")
    with temporary.open("wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _has_unfinished_request(session: Path) -> bool:
    for journal in session.glob("request-*.json"):
        try:
            if "result" not in json.loads(journal.read_text()):
                return True
        except (OSError, ValueError):
            return True
    return False


@dataclass(frozen=True)
class AppServerExecutor(CodexExecutor):
    """Send turns to one lazy, idle-expiring app-server per conversation."""

    command: tuple[str, ...] = ("codex", "app-server")
    session_root: str | None = None
    conversation_id: str | None = None
    idle_seconds: float = 300

    def for_workspace(self, *, work_item_id: str) -> AppServerExecutor:
        return cast(AppServerExecutor, super().for_workspace(work_item_id=work_item_id))

    def for_model(self, tier: str) -> AppServerExecutor:
        return cast(AppServerExecutor, super().for_model(tier))

    def with_output_callback(
        self, callback: Callable[[str, str], None]
    ) -> AppServerExecutor:
        return replace(self, output_callback=callback)

    def for_conversation(self, conversation_id: str) -> AppServerExecutor:
        return replace(self, conversation_id=conversation_id)

    def execute(self, envelope: TaskEnvelope) -> ExecutionResult:
        identity = self.work_item_id or self.conversation_id
        if identity is None or self.session_root is None:
            return ExecutionResult(errors=["app_server_conversation_missing"])
        directory = (
            Path(self.session_root) / hashlib.sha256(identity.encode()).hexdigest()[:24]
        )
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        if directory.is_symlink() or directory.stat().st_mode & 0o077:
            raise ValueError("app-server session directory must be private")
        path = directory / "rpc.sock"
        if len(os.fsencode(path)) > 103:
            raise ValueError(
                "app-server session directory is too long for Unix sockets"
            )
        environment = _executor_env(self.env)
        # A persistent process must never inherit credentials for an outer turn.
        for key in tuple(environment):
            if key.startswith("CHATTING_FINISH_"):
                environment.pop(key)
        generation = hashlib.sha256(
            json.dumps(
                [
                    self.command,
                    "danger-full-access/never",
                    environment,
                    str(Path(__file__).resolve()),
                ],
                sort_keys=True,
            ).encode()
        ).hexdigest()
        payload = _task_payload(
            envelope,
            current_time=self.now_provider(),
            executor_working_dir=self.cwd,
            work_item_id=self.work_item_id,
            model_tier=self.model_tier,
        )
        metadata = envelope.reply_channel.metadata
        message_id = metadata.get("message_id")
        if (
            self.history_store is not None
            and envelope.reply_channel.type == "telegram"
            and isinstance(message_id, int)
            and not isinstance(message_id, bool)
            and message_id > 0
        ):
            topic = metadata.get("message_thread_id")
            history = self.history_store.list_recent_telegram_history(
                target=envelope.reply_channel.target,
                topic_id=topic if isinstance(topic, int) and topic > 0 else None,
                before_message_id=message_id,
            )
            # Reuse the exec backend's history serialization, only on bootstrap.
            payload["recent_history"] = _task_payload(
                envelope, current_time=self.now_provider(), recent_history=history
            ).get("recent_history", [])
        payload["reply_contract"]["executor_exit_status_drives_completion"] = False
        payload["session_contract"] = {
            "instructions": (
                "You are handling one turn in a persistent conversation. Use this task's "
                "task_id, reply channel, workspace and contracts for all actions. Earlier "
                "turns are context, not requests to repeat completed work. After publishing "
                "the reply and resolving any follow-ups, end your turn normally. Do not "
                "call main_finish or wait for process termination. Codex turn completion "
                "finishes this task and leaves the conversation available."
            )
        }
        # Stable across transport retries, distinct for recovery continuations.
        request_id = hashlib.sha256(
            json.dumps([envelope.to_dict(), self.model_tier], sort_keys=True).encode()
        ).hexdigest()
        escalation_path = (
            Path(self.cwd or Path.cwd()) / f".chatting-escalate-{request_id}.json"
        )
        if self.model_tier == "low":
            payload["escalation_contract"] = _task_payload(
                envelope,
                current_time=self.now_provider(),
                escalation_path=escalation_path,
            )["escalation_contract"]
        request = {
            "generation": generation,
            "request_id": request_id,
            "payload": payload,
            "cwd": self.cwd,
            "model_tier": self.model_tier,
            "timeout": self.timeout_seconds,
            "escalation_path": str(escalation_path)
            if self.model_tier == "low"
            else None,
        }
        captured: dict[str, list[str]] = {"stdout": [], "stderr": []}
        try:
            with (directory / "startup.lock").open("a+") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                self._migrate_session(directory)
                connection = self._connect(path, directory, environment, generation)
            with connection:
                connection.settimeout(self.timeout_seconds + 15)
                connection.sendall(json.dumps(request).encode() + b"\n")
                with connection.makefile("rb") as stream:
                    for raw in stream:
                        event = json.loads(raw)
                        if "result" in event:
                            return ExecutionResult(**event["result"])
                        name = event["stream"]
                        content = event["content"]
                        captured[name].append(content)
                        if self.output_callback:
                            self.output_callback(name, content)
            raise RuntimeError("session disconnected; outcome may be uncertain")
        except (OSError, ValueError, RuntimeError) as exc:
            return ExecutionResult(
                errors=[f"app_server_transport:{exc}"],
                stdout="".join(captured["stdout"]),
                stderr="".join(captured["stderr"]),
            )

    def _migrate_session(self, directory: Path) -> None:
        if not self.work_item_id or not self.history_store or not self.session_root:
            return
        # Once the item has its own thread, old sessions are history.
        if (directory / "thread.json").exists():
            return
        candidates = [
            Path(self.session_root) / hashlib.sha256(legacy.encode()).hexdigest()[:24]
            for legacy in self.history_store.legacy_work_sessions(
                work_item_id=self.work_item_id
            )
        ]
        candidates = [
            old
            for old in candidates
            if (old / "thread.json").exists()
            or (old / "pending.json").exists()
            or any(old.glob("request-*.json"))
        ]
        if not candidates:
            return
        if len(candidates) > 1:
            # Adopting one thread would hide the others' context, so none is
            # adopted. A fresh thread is safe unless an old request may have
            # run without a recorded outcome: retrying it could repeat effects.
            if any(_has_unfinished_request(old) for old in candidates):
                raise ValueError(
                    "legacy app-server sessions for this work item have unfinished "
                    "requests; reconcile before resuming"
                )
            LOGGER.warning(
                "app_server_ambiguous_legacy_sessions work_item_id=%s count=%s",
                self.work_item_id,
                len(candidates),
            )
            return
        old = candidates[0]
        if old.is_symlink() or old.stat().st_mode & 0o077:
            raise ValueError("legacy app-server session directory must be private")
        # The old owner handles requests sequentially. A generation change
        # retires it after any current turn finishes, before copying its state.
        with (old / "startup.lock").open("a+") as startup:
            fcntl.flock(startup, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with (old / "owner.lock").open("a+") as owner:
                try:
                    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                        client.settimeout(self.timeout_seconds + 15)
                        client.connect(str(old / "rpc.sock"))
                        client.sendall(
                            b'{"generation":"work-item-identity-migration"}\n'
                        )
                        with client.makefile("rb") as stream:
                            result = json.loads(stream.readline())
                        if result.get("result", {}).get("errors") != [
                            "app_server_config_changed:retry_with_new_owner"
                        ]:
                            raise RuntimeError("legacy session did not retire")
                    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                marker = old / "migrated-to"
                if marker.exists() and marker.read_text() != self.work_item_id:
                    raise ValueError("legacy session already belongs to another item")
                if (directory / "thread.json").exists():
                    return
                _write_migration_file(marker, self.work_item_id.encode())
                # Preserve crash and retry journals as well as the thread ID.
                # Write thread.json last, so interrupted copies can be retried.
                for source in [
                    *old.glob("request-*.json"),
                    old / "pending.json",
                    old / "thread.json",
                ]:
                    if source.exists():
                        _write_migration_file(
                            directory / source.name, source.read_bytes()
                        )

    def _connect(
        self, path: Path, directory: Path, environment: dict[str, str], generation: str
    ) -> socket.socket:
        def connect() -> socket.socket:
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                client.settimeout(10)
                client.connect(str(path))
                return client
            except BaseException:
                client.close()
                raise

        try:
            return connect()
        except (FileNotFoundError, ConnectionRefusedError):
            pass
        settings = directory / "settings.json"
        settings.write_text(
            json.dumps(
                {
                    "command": self.command,
                    "generation": generation,
                    "idle_seconds": self.idle_seconds,
                }
            )
        )
        settings.chmod(0o600)
        with (directory / "session.log").open("ab") as log:
            child = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "app.worker.executor.app_server_session",
                    str(directory),
                ],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                env=environment,
                start_new_session=True,
            )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise RuntimeError("session failed to start; inspect session.log")
            try:
                connection = connect()
                Thread(target=child.wait, daemon=True).start()
                return connection
            except (FileNotFoundError, ConnectionRefusedError):
                time.sleep(0.025)
        raise RuntimeError("session did not open its socket")
