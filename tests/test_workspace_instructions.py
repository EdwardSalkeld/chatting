import subprocess
import tempfile
import unittest
from unittest.mock import patch
from io import BytesIO
from pathlib import Path

from app.worker.executor.codex import CodexExecutor
from app.worker.workspace_instructions import UpdatePoller


class WorkspaceInstructionsTests(unittest.TestCase):
    def test_fetches_main_not_local_changes_and_overwrites_workspace(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def git(*args):
                return subprocess.run(["git", *args], check=True, capture_output=True)

            remote = root / "remote.git"
            git("init", "--bare", str(remote))
            source = root / "source"
            git("clone", str(remote), str(source))
            git("-C", str(source), "switch", "-c", "main")
            git("-C", str(source), "config", "user.name", "Test")
            git("-C", str(source), "config", "user.email", "test@example.invalid")
            (source / "AGENTS.md").write_text("approved\n")
            git("-C", str(source), "add", "AGENTS.md")
            git("-C", str(source), "commit", "-m", "instructions")
            git("-C", str(source), "push", "origin", "main")
            (source / "AGENTS.md").write_text("unapproved\n")
            executor = CodexExecutor(
                workspace_root=str(root / "work"), instructions_repo=str(source)
            )
            workspace = Path(executor.for_workspace(work_item_id="one").cwd)
            self.assertEqual((workspace / "AGENTS.md").read_text(), "approved\n")
            (workspace / "AGENTS.md").write_text("local\n")
            executor.for_workspace(work_item_id="one")
            self.assertEqual((workspace / "AGENTS.md").read_text(), "approved\n")
            (source / "AGENTS.md").write_text("updated\n")
            git("-C", str(source), "add", "AGENTS.md")
            git("-C", str(source), "commit", "-m", "update")
            git("-C", str(source), "push", "origin", "main")
            executor.for_workspace(work_item_id="one")
            self.assertEqual((workspace / "AGENTS.md").read_text(), "updated\n")
            git("-C", str(source), "remote", "set-url", "origin", str(root / "missing"))
            with self.assertRaises(subprocess.CalledProcessError):
                executor.for_workspace(work_item_id="one")
            self.assertEqual((workspace / "AGENTS.md").read_text(), "updated\n")

    def test_unconfigured_leaves_existing_file_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            executor = CodexExecutor(workspace_root=tmp)
            workspace = Path(executor.for_workspace(work_item_id="one").cwd)
            (workspace / "AGENTS.md").write_text("local\n")
            executor.for_workspace(work_item_id="one")
            self.assertEqual((workspace / "AGENTS.md").read_text(), "local\n")

    def test_notification_refresh_waits_until_idle_and_retries_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "item_one"
            second = root / "item_two"
            first.mkdir()
            second.mkdir()
            outside = root / "other"
            outside.mkdir()
            (root / "item_link").symlink_to(outside, target_is_directory=True)
            poller = UpdatePoller("repo", root, "http://127.0.0.1:9467/egress")
            with (
                patch(
                    "app.worker.workspace_instructions.time.monotonic", return_value=11
                ),
                patch(
                    "app.worker.workspace_instructions.urllib.request.urlopen",
                    return_value=BytesIO(b'{"generation": 1}'),
                ) as get,
                patch(
                    "app.worker.workspace_instructions.fetch",
                    return_value=("abc", b"approved\n"),
                ) as fetch,
            ):
                poller.poll(idle=False)
                get.assert_not_called()
                poller.poll(idle=True)
                self.assertEqual((first / "AGENTS.md").read_text(), "approved\n")
                self.assertEqual((second / "AGENTS.md").read_text(), "approved\n")
                self.assertFalse((outside / "AGENTS.md").exists())
                self.assertEqual(poller.generation, 1)
                self.assertEqual(fetch.call_count, 1)
                poller.next_poll = 0
                get.return_value = BytesIO(b'{"generation": 1}')
                poller.poll(idle=True)
                self.assertEqual(fetch.call_count, 1)
            poller.next_poll = 0
            with (
                patch(
                    "app.worker.workspace_instructions.time.monotonic", return_value=22
                ),
                patch(
                    "app.worker.workspace_instructions.urllib.request.urlopen",
                    return_value=BytesIO(b'{"generation": 2}'),
                ),
                patch(
                    "app.worker.workspace_instructions.fetch",
                    side_effect=OSError("unavailable"),
                ),
            ):
                poller.poll(idle=True)
                self.assertEqual(poller.generation, 1)
