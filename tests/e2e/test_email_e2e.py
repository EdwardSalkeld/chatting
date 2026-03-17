"""End-to-end email roundtrip test using GreenMail and a fake codex executor."""

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


def _send_test_email(from_addr: str, to_addr: str, subject: str, body: str, *, port: int = 3025) -> None:
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)
    with smtplib.SMTP("127.0.0.1", port) as client:
        client.send_message(msg)


class EmailE2ETests(unittest.TestCase):
    def test_email_roundtrip_with_real_codex_executor_and_greenmail(self) -> None:
        server_bin_raw = os.environ.get("CHATTING_BBMB_SERVER_BIN", "").strip()
        if not server_bin_raw:
            self.skipTest("CHATTING_BBMB_SERVER_BIN is not set")
        server_bin = Path(server_bin_raw)
        if not server_bin.exists():
            self.skipTest(f"bbmb server binary not found: {server_bin}")

        if _is_port_open("127.0.0.1", 9876):
            self.skipTest("127.0.0.1:9876 already in use")

        # GreenMail IMAP must be running on 3143
        if not _is_port_open("127.0.0.1", 3143):
            self.skipTest("GreenMail IMAP not available on 127.0.0.1:3143")

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

            handler_log = temp_root / "handler.log"
            worker_log = temp_root / "worker.log"

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
                        "imap_port": 3143,
                        "imap_username": "bot@chatting.local",
                        "imap_password_env": "CHATTING_IMAP_PASSWORD",
                        "imap_use_ssl": False,
                        "smtp_host": "127.0.0.1",
                        "smtp_port": 3025,
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

            handler_log_fh = open(handler_log, "w")
            worker_log_fh = open(worker_log, "w")

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
                    stdout=worker_log_fh,
                    stderr=subprocess.STDOUT,
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
                    stdout=handler_log_fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                )

                time.sleep(1)

                # Send test email to GreenMail SMTP
                _send_test_email(
                    from_addr="sender@test.local",
                    to_addr="bot@chatting.local",
                    subject="E2E test task",
                    body="Please do the e2e test thing",
                    port=3025,
                )

                # Poll for prompt file - proves the full ingress path worked
                # (email -> handler -> bbmb -> worker -> codex executor)
                deadline = time.monotonic() + 60
                prompt_file = prompt_dir / "prompt.json"
                while time.monotonic() < deadline:
                    if prompt_file.exists():
                        break
                    time.sleep(1)

                if not prompt_file.exists():
                    self._dump_diagnostics(handler_proc, worker_proc, handler_log, worker_log, handler_log_fh, worker_log_fh, prompt_dir)
                    self.fail("prompt file never appeared - task never reached the executor")

                # Now wait for the reply email to arrive back in GreenMail
                # Check via IMAP directly
                import imaplib

                reply_found = False
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    try:
                        imap = imaplib.IMAP4("127.0.0.1", 3143)
                        imap.login("sender@test.local", "dummy")
                        imap.select("INBOX")
                        status, data = imap.search(None, "ALL")
                        if status == "OK" and data[0]:
                            reply_found = True
                        imap.logout()
                        if reply_found:
                            break
                    except Exception:
                        pass
                    time.sleep(1)

                if not reply_found:
                    self._dump_diagnostics(handler_proc, worker_proc, handler_log, worker_log, handler_log_fh, worker_log_fh, prompt_dir)
                    self.fail("reply email not found in GreenMail within 30s")

                # Validate the captured prompt
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
                if not handler_log_fh.closed:
                    handler_log_fh.close()
                if not worker_log_fh.closed:
                    worker_log_fh.close()

    def _dump_diagnostics(self, handler_proc, worker_proc, handler_log, worker_log, handler_log_fh, worker_log_fh):
        for proc in (handler_proc, worker_proc):
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        if not handler_log_fh.closed:
            handler_log_fh.close()
        if not worker_log_fh.closed:
            worker_log_fh.close()

        diag = []
        for name, log_path in [("handler", handler_log), ("worker", worker_log)]:
            log_content = log_path.read_text(encoding="utf-8")
            diag.append(f"\n--- {name} log (last 3000 chars) ---")
            diag.append(log_content[-3000:] if log_content else "(empty)")
        print("\n".join(diag), file=sys.stderr)


if __name__ == "__main__":
    unittest.main()
