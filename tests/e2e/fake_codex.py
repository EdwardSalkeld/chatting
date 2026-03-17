#!/usr/bin/env python3
"""Fake codex executor that captures the prompt and returns success."""
import json
import os
import sys


def main():
    payload = sys.stdin.read()
    prompt_dir = os.environ.get("FAKE_CODEX_PROMPT_DIR", "")
    if prompt_dir:
        os.makedirs(prompt_dir, exist_ok=True)
        with open(os.path.join(prompt_dir, "prompt.json"), "w") as f:
            f.write(payload)

    result = {
        "schema_version": "1.0",
        "actions": [],
        "config_updates": [],
        "requires_human_review": False,
        "errors": [],
    }
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()
