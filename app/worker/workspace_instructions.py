"""Install the approved AGENTS.md from a configured Git repository."""

from __future__ import annotations

import fcntl
import logging
import os
from pathlib import Path
import subprocess
import tempfile
import json
import re
import time
import urllib.error
import urllib.request

LOGGER = logging.getLogger(__name__)
MAX_BYTES = 32768


def fetch(repo: str) -> tuple[str, bytes]:
    """Fetch an immutable approved main snapshot."""
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}

    def git(*args: str) -> bytes:
        return subprocess.run(
            ["git", "-C", repo, *args],
            check=True,
            capture_output=True,
            timeout=30,
            env=env,
        ).stdout

    # Serialize refreshes so another workspace cannot change the ref between
    # fetch and reading the immutable commit.
    lock_path = Path(repo) / ".chatting-instructions.lock"
    with lock_path.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        git(
            "fetch",
            "--no-tags",
            "origin",
            "+refs/heads/main:refs/chatting/workspace-instructions",
        )
        revision = (
            git(
                "rev-parse", "--verify", "refs/chatting/workspace-instructions^{commit}"
            )
            .decode()
            .strip()
        )
        size = int(git("cat-file", "-s", f"{revision}:AGENTS.md"))
        if size > MAX_BYTES or size == 0:
            raise ValueError("workspace AGENTS.md must be between 1 and 32768 bytes")
        content = git("show", f"{revision}:AGENTS.md")
    content.decode("utf-8")
    return revision, content


def write(workspace: Path, revision: str, content: bytes) -> None:
    # Stage on the destination filesystem so replace is atomic, including
    # when the existing workspace file is a symlink.
    fd, name = tempfile.mkstemp(prefix=".agents-", dir=workspace)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(content)
        os.replace(name, workspace / "AGENTS.md")
    finally:
        if os.path.exists(name):
            os.unlink(name)
    LOGGER.info(
        "workspace_instructions_installed revision=%s workspace=%s", revision, workspace
    )


def install(repo: str, workspace: Path) -> str:
    """Fetch main and atomically install its file; fail closed on errors."""
    revision, content = fetch(repo)
    write(workspace, revision, content)
    return revision


class UpdatePoller:
    """Reconcile existing workspaces after durable handler notifications."""

    def __init__(self, repo: str, root: Path, handler_url: str):
        self.repo = repo
        self.root = root
        self.url = handler_url.rsplit("/", 1)[0] + "/instructions/generation"
        self.generation: int | None = None
        self.next_poll = 0.0

    def poll(self, *, idle: bool) -> None:
        if not idle or time.monotonic() < self.next_poll:
            return
        self.next_poll = time.monotonic() + 10
        try:
            with urllib.request.urlopen(self.url, timeout=3) as response:
                data = json.load(response)
            if not isinstance(data, dict):
                raise ValueError("invalid instructions generation response")
            generation = data["generation"]
            if type(generation) is not int or generation < 0:
                raise ValueError("invalid instructions generation")
            if generation == self.generation:
                return
            revision, content = fetch(self.repo)
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
            for workspace in self.root.iterdir():
                if (
                    re.fullmatch(r"item_[A-Za-z0-9_-]+", workspace.name)
                    and not workspace.is_symlink()
                    and workspace.is_dir()
                ):
                    write(workspace, revision, content)
            self.generation = generation
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
            LOGGER.warning(
                "workspace_instructions_refresh_failed type=%s", type(exc).__name__
            )
