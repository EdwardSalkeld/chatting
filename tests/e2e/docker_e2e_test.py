#!/usr/bin/env python3
"""Docker E2E test driver: sends email to the compose stack via GreenMail and waits for a reply.

Expects the compose stack from docker-compose.e2e.yml to be running with
GreenMail SMTP on localhost:3025 and IMAP on localhost:3143.
"""
import imaplib
import smtplib
import sys
import time
from email.message import EmailMessage


def send_email(subject: str, body: str) -> None:
    msg = EmailMessage()
    msg["From"] = "sender@test.local"
    msg["To"] = "bot@chatting.local"
    msg["Subject"] = subject
    msg.set_content(body)
    for attempt in range(10):
        try:
            with smtplib.SMTP("127.0.0.1", 3025, timeout=5) as client:
                client.send_message(msg)
            return
        except (smtplib.SMTPServerDisconnected, ConnectionError, OSError):
            time.sleep(1)
    print("FAIL: could not send email after 10 attempts", file=sys.stderr)
    sys.exit(1)


def wait_for_reply(timeout_seconds: int = 60) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            imap = imaplib.IMAP4("127.0.0.1", 3143)
            imap.login("sender", "dummy")
            imap.select("INBOX")
            status, data = imap.search(None, "ALL")
            if status == "OK" and data[0]:
                imap.logout()
                return True
            imap.logout()
        except Exception:
            pass
        time.sleep(1)
    return False


def main() -> int:
    print("Sending test email...")
    send_email("Docker E2E test", "Please reply to this.")

    print("Waiting for reply in sender's inbox...")
    if wait_for_reply():
        print("PASS: reply email received")
        return 0
    else:
        print("FAIL: no reply within 60 seconds", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
