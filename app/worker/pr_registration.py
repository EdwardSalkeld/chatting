"""Link pull requests an executor opens to the work item that opened them."""

from __future__ import annotations

import json
import re
import urllib.request

_PR_URL = re.compile(r"https://github\.com/[\w.-]+/[\w.-]+/pull/\d+")


def created_pr_url(line: str) -> str | None:
    """Return the PR URL printed by a successful `gh pr create`, if this is one.

    Reads one line of executor output: an app-server `item/completed`
    notification or an exec `item.completed` event.
    """
    try:
        event = json.loads(line)
    except ValueError:
        return None
    if not isinstance(event, dict):
        return None
    if event.get("method") == "item/completed":
        item = event.get("params", {}).get("item", {})
        kind, output, exit_code = "commandExecution", "aggregatedOutput", "exitCode"
    elif event.get("type") == "item.completed":
        item = event.get("item", {})
        kind, output, exit_code = "command_execution", "aggregated_output", "exit_code"
    else:
        return None
    if (
        not isinstance(item, dict)
        or item.get("type") != kind
        or item.get(exit_code) != 0
        or "gh pr create" not in str(item.get("command", ""))
    ):
        return None
    # gh prints the new PR's URL last; `git push` in the same command can also
    # print a .../pull/new/<branch> link, which the number excludes.
    urls = _PR_URL.findall(str(item.get(output, "")))
    return urls[-1] if urls else None


def register_pr(handler_url: str, *, task_id: str, pr_url: str) -> None:
    """Record pr_url against task_id's work item on the handler."""
    register_github_artifact(handler_url, task_id=task_id, kind="pr", value=pr_url)


def register_github_artifact(
    handler_url: str, *, task_id: str, kind: str, value: str
) -> None:
    field = {"pr": "pr_url", "subject": "subject_url", "repository": "repository"}[kind]
    request = urllib.request.Request(
        handler_url.rstrip("/") + f"/work-items/register-{kind}",
        data=json.dumps({"task_id": task_id, field: value}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10):
        pass
