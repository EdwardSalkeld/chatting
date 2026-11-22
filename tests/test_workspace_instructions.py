import subprocess
import tempfile
import unittest
from pathlib import Path

from app.worker.executor.codex import CodexExecutor


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
