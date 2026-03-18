"""End-to-end test for claude_exec using GreenMail and a mocked Anthropic client."""

import json
import imaplib
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
    last_error = None
    for _ in range(10):
        try:
            with smtplib.SMTP("127.0.0.1", port, timeout=5) as client:
                client.send_message(msg)
            return
        except (smtplib.SMTPServerDisconnected, ConnectionError, OSError) as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"failed to send email after retries: {last_error}")


class ClaudeExecE2ETests(unittest.TestCase):
    def test_claude_exec_email_roundtrip_with_mocked_api(self) -> None:
        """Full e2e: email in -> claude_exec (with mocked API) -> reply email out."""
        server_bin_raw = os.environ.get("CHATTING_BBMB_SERVER_BIN", "").strip()
        if not server_bin_raw:
            self.skipTest("CHATTING_BBMB_SERVER_BIN is not set")
        server_bin = Path(server_bin_raw)
        if not server_bin.exists():
            self.skipTest(f"bbmb server binary not found: {server_bin}")
        if _is_port_open("127.0.0.1", 9876):
            self.skipTest("127.0.0.1:9876 already in use")
        if not _is_port_open("127.0.0.1", 3143):
            self.skipTest("GreenMail IMAP not available on 127.0.0.1:3143")
        if not _is_port_open("127.0.0.1", 3025):
            self.skipTest("GreenMail SMTP not available on 127.0.0.1:3025")

        repo_root = Path(__file__).resolve().parent.parent.parent
        # Use a wrapper that patches anthropic before importing claude_exec
        mock_wrapper = repo_root / "tests" / "e2e" / "mock_claude_exec.py"

        server_proc = None
        worker_proc = None
        handler_proc = None

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir)
            handler_db = temp_root / "handler.db"
            worker_db = temp_root / "worker.db"
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
                        "imap_username": "bot",
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
                        "codex_command": f"{sys.executable} {mock_wrapper}",
                    }
                ),
                encoding="utf-8",
            )

            env = os.environ.copy()
            env["CHATTING_IMAP_PASSWORD"] = "dummy"
            env["CHATTING_SMTP_PASSWORD"] = "dummy"
            env["ANTHROPIC_API_KEY"] = "test-key-not-real"

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
                    [sys.executable, "-m", "app.main_worker", "--config", str(worker_config_path)],
                    cwd=repo_root,
                    stdout=worker_log_fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                )
                handler_proc = subprocess.Popen(
                    [sys.executable, "-m", "app.main_message_handler", "--config", str(handler_config_path)],
                    cwd=repo_root,
                    stdout=handler_log_fh,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                )

                time.sleep(1)

                _send_test_email(
                    from_addr="sender@test.local",
                    to_addr="bot@chatting.local",
                    subject="What is 2+2?",
                    body="Please answer briefly.",
                    port=3025,
                )

                # Wait for reply in sender's inbox
                reply_found = False
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    try:
                        imap = imaplib.IMAP4("127.0.0.1", 3143)
                        imap.login("sender", "dummy")
                        imap.select("INBOX")
                        status, data = imap.search(None, "ALL")
                        if status == "OK" and data[0]:
                            # Check for reply content from mock
                            for uid in data[0].split():
                                _, msg_data = imap.fetch(uid, "(RFC822)")
                                raw = msg_data[0][1] if msg_data[0] and len(msg_data[0]) > 1 else b""
                                if b"mock claude reply" in raw.lower():
                                    reply_found = True
                                    break
                        imap.logout()
                        if reply_found:
                            break
                    except Exception:
                        pass
                    time.sleep(1)

                if not reply_found:
                    for proc in (handler_proc, worker_proc):
                        if proc is not None and proc.poll() is None:
                            proc.terminate()
                            try:
                                proc.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait(timeout=5)
                    handler_log_fh.close()
                    worker_log_fh.close()
                    diag = ["reply email not found in GreenMail within 60s"]
                    for name, log_path in [("handler", handler_log), ("worker", worker_log)]:
                        log_content = log_path.read_text(encoding="utf-8")
                        diag.append(f"\n--- {name} log (last 3000 chars) ---")
                        diag.append(log_content[-3000:] if log_content else "(empty)")
                    print("\n".join(diag), file=sys.stderr)
                    self.fail("reply email with mock claude content not found")

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


if __name__ == "__main__":
    unittest.main()
