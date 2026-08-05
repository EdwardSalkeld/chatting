#!/usr/bin/env python3
"""Fake codex executor that captures the prompt and sends a reply via main_reply."""

import json
import os
import subprocess
import sys
import tempfile


def main():
    payload = sys.stdin.read()
    prompt_dir = os.environ.get("FAKE_CODEX_PROMPT_DIR", "")
    if prompt_dir:
        os.makedirs(prompt_dir, exist_ok=True)
        with open(os.path.join(prompt_dir, "prompt.json"), "w") as f:
            f.write(payload)

    # Send a visible reply via main_reply, as the reply contract requires
    parsed = json.loads(payload)
    task = parsed.get("task", {})
    task_id = task.get("task_id", "")
    reply_channel = task.get("reply_channel", {})
    channel = reply_channel.get("type", "log")
    target = reply_channel.get("target", "test")
    # Some ingress sources (e.g. webhook/auxiliary-ingress) declare a reply
    # channel that the handler has no dispatcher for, so a reply there is
    # undeliverable by design. Send the reply on "log" in that case rather than
    # emitting an undeliverable one: main_reply is now synchronous and would
    # exit non-zero on a dropped send, failing this run.
    _DELIVERABLE = {"email", "telegram", "telegram_reaction", "github", "log"}
    if channel not in _DELIVERABLE:
        channel = "log"

    # Send via a spec file, like the real reply contract now requires, so the
    # reply body never passes through the shell.
    spec = {
        "task_id": task_id,
        "channel": channel,
        "target": target,
        "message": "E2E fake codex reply",
    }
    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", prefix="fake-codex-reply-", delete=False
    ) as spec_file:
        json.dump(spec, spec_file)
        spec_path = spec_file.name

    subprocess.run(
        [sys.executable, "-m", "app.main_reply", "--spec-file", spec_path],
        check=True,
    )

    print("fake codex transcript", file=sys.stdout)


if __name__ == "__main__":
    main()
