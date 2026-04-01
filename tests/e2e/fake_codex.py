#!/usr/bin/env python3
"""Fake codex executor that captures the prompt and sends a reply via main_reply."""

import json
import os
import subprocess
import sys


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

    subprocess.run(
        [
            sys.executable,
            "-m",
            "app.main_reply",
            task_id,
            "--message",
            "E2E fake codex reply",
            "--channel",
            channel,
            "--target",
            target,
        ],
        check=True,
    )

    print("fake codex transcript", file=sys.stdout)


if __name__ == "__main__":
    main()
