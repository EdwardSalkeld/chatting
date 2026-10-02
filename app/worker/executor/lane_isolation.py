"""OS identities and task-scoped reply relay for isolated lane executors."""

from __future__ import annotations

import hashlib
import json
import os
import pwd
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Mapping

from app.models import TaskEnvelope

REPLY_SOCKET_ENV = "CHATTING_REPLY_SOCKET"
_MAX_REQUEST = 1024 * 1024


def prepare_lane(
    directory: Path, *, work_item_id: str, source_env: Mapping[str, str]
) -> tuple[int, int, dict[str, str]]:
    """Provision a persistent Unix identity and private Codex home for one lane."""
    if os.geteuid() != 0:
        raise RuntimeError("isolated executors require a root worker")
    name = "chatlane_" + hashlib.sha256(work_item_id.encode()).hexdigest()[:12]
    try:
        account = pwd.getpwnam(name)
    except KeyError:
        subprocess.run(
            ["useradd", "--system", "--no-create-home", "--shell", "/usr/sbin/nologin", name],
            check=True,
        )
        account = pwd.getpwnam(name)
    uid, gid = account.pw_uid, account.pw_gid
    if uid == 0 or gid == 0:
        raise RuntimeError("lane account must be unprivileged")
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("lane directory must be a real directory")
    # Adopt files left by the earlier root-run prototype without crossing a
    # symlink into another lane or an unrelated host path.
    for parent, subdirs, files in os.walk(directory, followlinks=False):
        for name in subdirs + files:
            os.lchown(Path(parent) / name, uid, gid)
    os.chown(directory, uid, gid)
    directory.chmod(0o700)
    home = directory / ".home"
    temp = directory / ".tmp"
    codex_home = home / ".codex"
    for path in (home, temp, codex_home):
        path.mkdir(mode=0o700, exist_ok=True)
        if path.is_symlink():
            raise ValueError("lane private directory must not be a symlink")
        os.chown(path, uid, gid)
        path.chmod(0o700)

    source_home = Path(
        source_env.get("CODEX_HOME") or Path(source_env.get("HOME", "/root")) / ".codex"
    )
    source_auth = source_home / "auth.json"
    if not source_auth.is_file():
        raise RuntimeError(f"Codex auth file is missing: {source_auth}")
    destination = codex_home / "auth.json"
    if destination.is_symlink():
        raise ValueError("lane Codex auth file must not be a symlink")
    # Refresh on each run so credential rotation reaches existing lanes.
    with source_auth.open("rb") as source, destination.open("wb") as target:
        shutil.copyfileobj(source, target)
    os.chown(destination, uid, gid)
    destination.chmod(0o600)

    env = {
        key: source_env[key]
        for key in ("PATH", "LANG", "LC_ALL", "TERM", "PYTHONPATH", "SSL_CERT_FILE")
        if key in source_env
    }
    env.update(
        HOME=str(home),
        CODEX_HOME=str(codex_home),
        TMPDIR=str(temp),
        USER=name,
        LOGNAME=name,
        **{REPLY_SOCKET_ENV: str(directory / ".reply.sock")},
    )
    return uid, gid, env


def drop_privileges(uid: int, gid: int) -> None:
    os.setgroups([])
    os.setgid(gid)
    os.setuid(uid)


@contextmanager
def reply_relay(
    directory: Path, *, uid: int, gid: int, envelope: TaskEnvelope,
    worker_env: Mapping[str, str],
) -> Iterator[None]:
    """Serve replies from one lane UID for one active task only."""
    path = directory / ".reply.sock"
    if path.exists() or path.is_symlink():
        path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(path))
    os.chown(path, uid, gid)
    path.chmod(0o600)
    server.listen(4)
    server.settimeout(0.2)
    stop = threading.Event()

    def serve() -> None:
        while not stop.is_set():
            try:
                connection, _ = server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with connection:
                try:
                    credentials = connection.getsockopt(
                        socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
                    )
                    _, peer_uid, _ = struct.unpack("3i", credentials)
                    if peer_uid != uid:
                        raise ValueError("reply peer has wrong lane identity")
                    request = bytearray()
                    while len(request) <= _MAX_REQUEST:
                        chunk = connection.recv(min(65536, _MAX_REQUEST + 1 - len(request)))
                        if not chunk:
                            break
                        request.extend(chunk)
                    if len(request) > _MAX_REQUEST:
                        raise ValueError("reply is too large")
                    spec = json.loads(request)
                    _validate_reply(spec, directory=directory, envelope=envelope)
                    response = _run_reply(spec, worker_env)
                except (ValueError, TypeError, OSError, json.JSONDecodeError) as error:
                    response = {"exit_code": 2, "stdout": "", "stderr": str(error)}
                connection.sendall(json.dumps(response).encode())

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        server.close()
        thread.join(timeout=5)
        path.unlink(missing_ok=True)


def _validate_reply(spec: object, *, directory: Path, envelope: TaskEnvelope) -> None:
    if not isinstance(spec, dict):
        raise ValueError("reply must be a JSON object")
    if spec.get("task_id") != f"task:{envelope.id}":
        raise ValueError("reply task does not match active task")
    if (spec.get("channel"), spec.get("target")) != (
        envelope.reply_channel.type, envelope.reply_channel.target
    ):
        raise ValueError("reply destination does not match active task")
    if any(field in spec for field in ("envelope_id", "trace_id")):
        raise ValueError("reply cannot override envelope identity")
    attachment = spec.get("attachment_path")
    if attachment is not None:
        path = Path(attachment).resolve(strict=True)
        if not path.is_file() or not path.is_relative_to(directory.resolve()):
            raise ValueError("attachment must be inside the active lane workspace")


def _run_reply(spec: dict[str, object], worker_env: Mapping[str, str]) -> dict[str, object]:
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json") as file:
        json.dump(spec, file)
        file.flush()
        env = dict(worker_env)
        env.pop(REPLY_SOCKET_ENV, None)
        try:
            completed = subprocess.run(
                [sys.executable, "-P", "-m", "app.main_reply", "--spec-file", file.name],
                env=env, text=True, capture_output=True, timeout=30, check=False,
            )
        except subprocess.TimeoutExpired:
            return {"exit_code": 3, "stdout": "", "stderr": "reply handler timed out"}
    return {
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
