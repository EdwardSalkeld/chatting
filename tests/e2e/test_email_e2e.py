"""End-to-end email roundtrip test using Mailpit and a fake codex executor."""

import json
import os
import smtplib
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from email.message import EmailMessage
from pathlib import Path
from urllib.request import Request, urlopen


def _is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        return probe.connect_ex((host, port)) == 0


def _wait_for_port(host: str, port: int, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _is_port_open(host, port):
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {host}:{port}")


def _mailpit_messages() -> list[dict]:
    """Fetch messages from mailpit REST API."""
    req = Request("http://127.0.0.1:8025/api/v1/messages")
    with urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read())
    return data.get("messages", [])


def _send_test_email(from_addr: str, to_addr: str, subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)
    with smtplib.SMTP("127.0.0.1", 1025) as client:
        client.send_message(msg)


class EmailE2ETests(unittest.TestCase):
    def test_email_roundtrip_with_real_codex_executor_and_mailpit(self) -> None:
        server_bin_raw = os.environ.get("CHATTING_BBMB_SERVER_BIN", "").strip()
        if not server_bin_raw:
            self.skipTest("CHATTING_BBMB_SERVER_BIN is not set")
        server_bin = Path(server_bin_raw)
        if not server_bin.exists():
            self.skipTest(f"bbmb server binary not found: {server_bin}")

        if _is_port_open("127.0.0.1", 9876):
            self.skipTest("127.0.0.1:9876 already in use")

        if not _is_port_open("127.0.0.1", 8025):
            self.skipTest("mailpit not available on 127.0.0.1:8025")

        repo_root = Path(__file__).resolve().parent.parent.parent
        fake_codex = str(repo_root / "tests" / "e2e" / "fake_codex.py")

        server_proc = None
        worker_proc = None
        handler_proc = None

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            handler_db = temp_root / "handler.db"
            worker_db = temp_root / "worker.db"
            prompt_dir = temp_root / "prompts"
            prompt_dir.mkdir()

            handler_config_path = temp_root / "handler.json"
            worker_config_path = temp_root / "worker.json"

            handler_config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(handler_db),
                        "bbmb_address": "127.0.0.1:9876",
                        "poll_interval_seconds": 0.5,
                        "poll_timeout_seconds": 2,
                        "max_loops": 200,
                        "allowed_egress_channels": ["email", "log"],
                        "imap_host": "127.0.0.1",
                        "imap_port": 1143,
                        "imap_username": "bot@chatting.local",
                        "imap_password_env": "CHATTING_IMAP_PASSWORD",
                        "imap_use_ssl": False,
                        "smtp_host": "127.0.0.1",
                        "smtp_port": 1025,
                        "smtp_from": "bot@chatting.local",
                        "smtp_starttls": False,
                        "smtp_use_ssl": False,
                    }
                ),
                encoding="utf-8",
            )

            worker_config_path.write_text(
                json.dumps(
                    {
                        "db_path": str(worker_db),
                        "bbmb_address": "127.0.0.1:9876",
                        "max_attempts": 2,
                        "poll_timeout_seconds": 2,
                        "sleep_seconds": 0.2,
                        "max_loops": 200,
                        "use_stub_executor": False,
                        "codex_command": f"{sys.executable} {fake_codex}",
                    }
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["CHATTING_IMAP_PASSWORD"] = "dummy"
            env["CHATTING_SMTP_PASSWORD"] = "dummy"
            env["FAKE_CODEX_PROMPT_DIR"] = str(prompt_dir)

            try:
                server_proc = subprocess.Popen(
                    [str(server_bin)],
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                _wait_for_port("127.0.0.1", 9876, timeout_seconds=5.0)

                worker_proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "app.main_worker",
                        "--config",
                        str(worker_config_path),
                    ],
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                )
                handler_proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "app.main_message_handler",
                        "--config",
                        str(handler_config_path),
                    ],
                    cwd=repo_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                )

                # Give processes a moment to start
                time.sleep(1)

                _send_test_email(
                    from_addr="sender@test.local",
                    to_addr="bot@chatting.local",
                    subject="E2E test task",
                    body="Please do the e2e test thing",
                )

                # Poll mailpit for a reply email (from bot to sender)
                deadline = time.monotonic() + 60
                reply_found = False
                while time.monotonic() < deadline:
                    messages = _mailpit_messages()
                    for msg in messages:
                        to_addrs = [
                            addr.get("Address", "") for addr in msg.get("To", [])
                        ]
                        from_addr = msg.get("From", {}).get("Address", "")
                        if (
                            from_addr == "bot@chatting.local"
                            and "sender@test.local" in to_addrs
                        ):
                            reply_found = True
                            break
                    if reply_found:
                        break
                    time.sleep(1)

                if not reply_found:
                    # Dump diagnostics before failing
                    diag = ["reply email not found in mailpit within 60s"]
                    for name, proc in [("handler", handler_proc), ("worker", worker_proc)]:
                        if proc is not None:
                            rc = proc.poll()
                            diag.append(f"\n--- {name} (rc={rc}) ---")
                            if rc is not None:
                                out, err = proc.communicate(timeout=5)
                                diag.append(f"stdout:\n{out[-2000:]}")
                                diag.append(f"stderr:\n{err[-2000:]}")
                    try:
                        diag.append(f"\nmailpit messages: {json.dumps(_mailpit_messages(), indent=2)[:3000]}")
                    except Exception:
                        pass
                    prompt_file_diag = prompt_dir / "prompt.json"
                    diag.append(f"\nprompt file exists: {prompt_file_diag.exists()}")
                    self.fail("\n".join(diag))

                prompt_file = prompt_dir / "prompt.json"
                self.assertTrue(
                    prompt_file.exists(), f"prompt dump not found at {prompt_file}"
                )
                prompt = json.loads(prompt_file.read_text(encoding="utf-8"))

                self.assertEqual(prompt["schema_version"], "1.0")
                self.assertIn("task", prompt)
                task = prompt["task"]
                self.assertIn("E2E test task", task["content"])
                self.assertIn("Please do the e2e test thing", task["content"])
                self.assertEqual(task["source"], "email")
                self.assertEqual(task["reply_channel"]["type"], "email")
                self.assertEqual(
                    task["reply_channel"]["target"], "sender@test.local"
                )
                self.assertIn("current_time", prompt)
                self.assertIn("reply_contract", prompt)

            finally:
                for proc in (handler_proc, worker_proc, server_proc):
                    if proc is None:
                        continue
                    if proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
