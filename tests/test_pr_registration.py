import json
import unittest

from app.worker.pr_registration import created_pr_url

# Shaped like events captured from the deployed worker.
APP_SERVER_PR_CREATED = json.dumps(
    {
        "method": "item/completed",
        "params": {
            "item": {
                "type": "commandExecution",
                "command": "/bin/bash -lc \"gh pr create --repo EdwardSalkeld/chatting "
                "--base main --head fix/x --title 'Fix x' --body-file pr.md\"",
                "status": "completed",
                "aggregatedOutput": "https://github.com/EdwardSalkeld/chatting/pull/308\n",
                "exitCode": 0,
            },
            "threadId": "thread-1",
            "turnId": "turn-1",
        },
    }
)
EXEC_PR_CREATED = json.dumps(
    {
        "type": "item.completed",
        "item": {
            "type": "command_execution",
            "command": "/bin/bash -lc \"git push -u origin HEAD; gh pr create --base main\"",
            "aggregated_output": (
                "remote: Create a pull request for 'fix/x' on GitHub by visiting:\n"
                "remote:      https://github.com/brokensbone/media-collection-manager"
                "/pull/new/fix/x\n"
                "https://github.com/brokensbone/media-collection-manager/pull/67\n"
            ),
            "exit_code": 0,
            "status": "completed",
        },
    }
)


class CreatedPRURLTests(unittest.TestCase):
    def test_reads_app_server_command_completion(self) -> None:
        self.assertEqual(
            created_pr_url(APP_SERVER_PR_CREATED),
            "https://github.com/EdwardSalkeld/chatting/pull/308",
        )

    def test_reads_exec_event_and_ignores_new_pull_request_links(self) -> None:
        self.assertEqual(
            created_pr_url(EXEC_PR_CREATED),
            "https://github.com/brokensbone/media-collection-manager/pull/67",
        )

    def test_ignores_failed_and_unrelated_commands_and_non_events(self) -> None:
        failed = json.loads(APP_SERVER_PR_CREATED)
        failed["params"]["item"]["exitCode"] = 1
        listing = json.loads(APP_SERVER_PR_CREATED)
        listing["params"]["item"]["command"] = "gh pr list"
        for line in (
            json.dumps(failed),
            json.dumps(listing),
            "not json",
            "[]",
            json.dumps({"method": "item/started", "params": {}}),
        ):
            with self.subTest(line=line[:40]):
                self.assertIsNone(created_pr_url(line))


if __name__ == "__main__":
    unittest.main()
