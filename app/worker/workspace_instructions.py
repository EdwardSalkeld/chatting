"""Install the approved AGENTS.md from a configured Git repository."""

from __future__ import annotations

import fcntl
import logging
import os
from pathlib import Path
import subprocess
import tempfile

LOGGER = logging.getLogger(__name__)
MAX_BYTES = 32768


def install(repo: str, workspace: Path) -> str:
    """Fetch main and atomically install its file; fail closed on errors."""
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
    return revision
