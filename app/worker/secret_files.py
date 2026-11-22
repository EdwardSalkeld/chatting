"""Write secret files as the workspace-owning worker user."""

from __future__ import annotations

import ctypes
import os
import re
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ID = re.compile(r"[A-Za-z0-9_-]+\Z")
_libc = ctypes.CDLL(None, use_errno=True)
_linkat = _libc.linkat
_linkat.argtypes = (
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.c_char_p,
    ctypes.c_int,
)
_linkat.restype = ctypes.c_int
_AT_EMPTY_PATH = 0x1000


class Unavailable(Exception):
    pass


def save_secret(root: Path, payload: dict[str, Any]) -> dict[str, object]:
    if payload.get("workspace_root") != str(root):
        raise OSError("workspace root mismatch")
    item, request = payload.get("work_item_id"), payload.get("request_id")
    secret, expires = payload.get("secret"), payload.get("expires_at")
    if (
        not isinstance(item, str)
        or not _ID.fullmatch(item)
        or not isinstance(request, str)
        or not _ID.fullmatch(request)
    ):
        raise Unavailable
    if not isinstance(expires, str) or not (secret is None or isinstance(secret, str)):
        raise Unavailable
    try:
        expiry = datetime.fromisoformat(expires.replace("Z", "+00:00"))
        if expiry.tzinfo is None:
            raise ValueError("timezone required")
    except ValueError as exc:
        raise Unavailable from exc
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    root_fd = os.open(root, flags)
    try:
        work_fd = os.open(item, flags, dir_fd=root_fd)
        try:
            try:
                os.mkdir(".chatting-secrets", 0o700, dir_fd=work_fd)
            except FileExistsError:
                pass
            os.fsync(work_fd)
            directory_fd = os.open(".chatting-secrets", flags, dir_fd=work_fd)
            try:
                st = os.fstat(directory_fd)
                if st.st_uid != os.geteuid() or st.st_mode & 0o077:
                    raise Unavailable
                try:
                    existing = os.stat(
                        request, dir_fd=directory_fd, follow_symlinks=False
                    )
                except FileNotFoundError:
                    existing = None
                if existing is not None:
                    if not stat.S_ISREG(existing.st_mode):
                        raise Unavailable
                    os.fsync(directory_fd)
                    return {
                        "path": str(root / item / ".chatting-secrets" / request),
                        "existing": True,
                    }
                if secret is None or not secret or datetime.now(timezone.utc) >= expiry:
                    raise Unavailable
                data = secret.encode("utf-8")
                if len(data) > 65536:
                    raise Unavailable
                fd = os.open(
                    ".",
                    os.O_WRONLY | os.O_TMPFILE | os.O_CLOEXEC,
                    0o600,
                    dir_fd=directory_fd,
                )
                try:
                    with os.fdopen(fd, "wb", closefd=False) as stream:
                        stream.write(data)
                        stream.flush()
                    os.fsync(fd)
                    if (
                        _linkat(
                            fd,
                            b"",
                            directory_fd,
                            request.encode("ascii"),
                            _AT_EMPTY_PATH,
                        )
                        != 0
                    ):
                        error = ctypes.get_errno()
                        raise OSError(error, os.strerror(error))
                    os.fsync(directory_fd)
                finally:
                    os.close(fd)
                return {
                    "path": str(root / item / ".chatting-secrets" / request),
                    "existing": False,
                }
            finally:
                os.close(directory_fd)
        finally:
            os.close(work_fd)
    finally:
        os.close(root_fd)
