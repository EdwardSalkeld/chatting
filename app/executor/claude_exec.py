#!/usr/bin/env python3
"""Claude executor subprocess: reads task JSON, calls the Anthropic API, sends reply via main_reply."""
import json
import os
import subprocess
import sys

import anthropic


def main() -> int:
    payload = json.loads(sys.stdin.read())
    task = payload.get("task", {})
    task_id = task.get("task_id", "")
    reply_channel = task.get("reply_channel", {})
    channel = reply_channel.get("type", "log")
    target = reply_channel.get("target", "unknown")
    content = task.get("content", "")

    api_key_env = os.environ.get("CLAUDE_API_KEY_ENV", "ANTHROPIC_API_KEY")
    api_key = os.environ.get(api_key_env, "")
    if not api_key:
        print(f"missing API key from env var {api_key_env}", file=sys.stderr)
        return 1

    model = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")
    max_tokens = task.get("execution_constraints", {}).get("max_tokens", 4096) or 4096

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": content}],
    )
    reply_text = response.content[0].text

    subprocess.run(
        [
            sys.executable, "-m", "app.main_reply",
            task_id,
            "--message", reply_text,
            "--channel", channel,
            "--target", target,
        ],
        check=True,
    )

    result = {
        "schema_version": "1.0",
        "actions": [],
        "config_updates": [],
        "requires_human_review": False,
        "errors": [],
    }
    json.dump(result, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
