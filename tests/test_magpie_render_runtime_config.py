import json
import tempfile
import unittest
from pathlib import Path

from deploy.magpie.render_runtime_config import render_runtime_config


class RenderRuntimeConfigTests(unittest.TestCase):
    def test_render_runtime_config_rewrites_blink_paths_for_magpie(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_root = root / "source"
            output_root = root / "output"
            handler_dir = source_root / "handler"
            worker_dir = source_root / "worker"
            handler_dir.mkdir(parents=True)
            worker_dir.mkdir(parents=True)

            (handler_dir / "handler.json").write_text(
                json.dumps(
                    {
                        "db_path": "/data/chatting-message-handler.db",
                        "bbmb_address": "bbmb:9876",
                        "metrics_host": "0.0.0.0",
                        "schedule_file": "/config/live-schedule.local.json",
                        "context_refs": ["repo:/workspace", "repo:/workspace/chatting"],
                        "telegram_context_refs": ["repo:/workspace"],
                    }
                ),
                encoding="utf-8",
            )
            (worker_dir / "worker.json").write_text(
                json.dumps(
                    {
                        "db_path": "/data/chatting-worker.db",
                        "bbmb_address": "bbmb:9876",
                        "codex_working_dir": "/workspace",
                    }
                ),
                encoding="utf-8",
            )
            (handler_dir / "handler.env").write_text("ONE=1\n", encoding="utf-8")
            (worker_dir / "worker.env").write_text("TWO=2\n", encoding="utf-8")
            (handler_dir / "live-schedule.local.json").write_text(
                "[]\n", encoding="utf-8"
            )

            render_runtime_config(source_root=source_root, output_root=output_root)

            handler_payload = json.loads(
                (output_root / "handler.json").read_text(encoding="utf-8")
            )
            worker_payload = json.loads(
                (output_root / "worker.json").read_text(encoding="utf-8")
            )

            self.assertEqual(
                handler_payload["db_path"], "/var/lib/handler/chatting-message-handler.db"
            )
            self.assertEqual(handler_payload["bbmb_address"], "127.0.0.1:9876")
            self.assertEqual(handler_payload["metrics_host"], "127.0.0.1")
            self.assertEqual(
                handler_payload["telegram_attachment_dir"],
                "/var/lib/handler/telegram-attachments",
            )
            self.assertEqual(
                handler_payload["schedule_file"], "/etc/chatting/live-schedule.local.json"
            )
            self.assertEqual(
                handler_payload["context_refs"],
                ["repo:/srv/chatting/workspace", "repo:/srv/chatting/workspace/chatting"],
            )
            self.assertEqual(
                handler_payload["telegram_context_refs"],
                ["repo:/srv/chatting/workspace"],
            )
            self.assertEqual(
                worker_payload["db_path"], "/var/lib/worker/chatting-worker.db"
            )
            self.assertEqual(worker_payload["bbmb_address"], "127.0.0.1:9876")
            self.assertEqual(
                worker_payload["codex_working_dir"], "/srv/chatting/workspace"
            )
            self.assertEqual((output_root / "handler.env").read_text(encoding="utf-8"), "ONE=1\n")
            self.assertEqual((output_root / "worker.env").read_text(encoding="utf-8"), "TWO=2\n")
            self.assertEqual(
                (output_root / "live-schedule.local.json").read_text(encoding="utf-8"),
                "[]\n",
            )


if __name__ == "__main__":
    unittest.main()
