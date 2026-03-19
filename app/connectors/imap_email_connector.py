"""IMAP-backed email connector."""

from __future__ import annotations

import imaplib
from datetime import datetime, timezone
from email import message_from_bytes, policy
from email.message import EmailMessage
from email.utils import parseaddr, parsedate_to_datetime
from typing import Callable, Sequence, cast

from app.models import PromptContext, ReplyChannel, TaskEnvelope


class ImapEmailConnector:
    """Poll an IMAP inbox and normalize unseen emails to task envelopes."""

    source = "email"

    def __init__(
        self,
        *,
        host: str,
        username: str,
        password: str,
        port: int = 993,
        mailbox: str = "INBOX",
        search_criterion: str = "UNSEEN",
        context_refs: list[str] | None = None,
        prompt_context: PromptContext | None = None,
        use_ssl: bool = True,
        imap_client_factory: Callable[[str, int], imaplib.IMAP4] | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        if not host:
            raise ValueError("host is required")
        if not username:
            raise ValueError("username is required")
        if not password:
            raise ValueError("password is required")
        if port <= 0:
            raise ValueError("port must be positive")
        if not mailbox:
            raise ValueError("mailbox is required")
        if not search_criterion:
            raise ValueError("search_criterion is required")

        self._host = host
        self._username = username
        self._password = password
        self._port = port
        self._mailbox = mailbox
        self._search_criterion = search_criterion
        self._context_refs = context_refs or []
        self._prompt_context = prompt_context or PromptContext()
        self._imap_client_factory = imap_client_factory or _default_imap_factory(use_ssl)
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def poll(self) -> list[TaskEnvelope]:
        envelopes: list[TaskEnvelope] = []
        client = self._imap_client_factory(self._host, self._port)
        try:
            status, _ = client.login(self._username, self._password)
            if status != "OK":
                raise RuntimeError("imap_login_failed")

            status, _ = client.select(self._mailbox)
            if status != "OK":
                raise RuntimeError(f"imap_select_failed:{self._mailbox}")

            status, search_data = client.search(None, self._search_criterion)
            if status != "OK":
                raise RuntimeError("imap_search_failed")

            uids = _parse_uid_list(search_data)
            for uid in uids:
                status, fetch_data = client.fetch(uid.decode("ascii"), "(RFC822)")
                if status != "OK":
                    continue
                raw_message = _extract_message_bytes(fetch_data)
                if raw_message is None:
                    continue
                envelopes.append(self._to_envelope(uid, raw_message))
        finally:
            try:
                client.logout()
            except Exception:  # noqa: BLE001
                pass

        return envelopes

    def _to_envelope(self, uid: bytes, raw_message: bytes) -> TaskEnvelope:
        parsed = cast(EmailMessage, message_from_bytes(raw_message, policy=policy.default))
        from_header = parsed.get("From", "")
        _, from_address = parseaddr(from_header)
        target = from_address or self._username
        subject = (parsed.get("Subject") or "(no subject)").strip() or "(no subject)"
        body = _extract_body_text(parsed)
        content = f"Subject: {subject}\n\n{body}"
        received_at = _parse_received_at(parsed.get("Date"), self._now_provider())
        uid_value = uid.decode("utf-8")
        event_id = f"email:{uid_value}"
        return TaskEnvelope(
            id=event_id,
            source="email",
            received_at=received_at,
            actor=from_address or None,
            content=content,
            attachments=[],
            context_refs=self._context_refs,
            reply_channel=ReplyChannel(type="email", target=target),
            dedupe_key=event_id,
            prompt_context=self._prompt_context,
        )


def _default_imap_factory(use_ssl: bool) -> Callable[[str, int], imaplib.IMAP4]:
    if use_ssl:
        return lambda host, port: imaplib.IMAP4_SSL(host, port)
    return lambda host, port: imaplib.IMAP4(host, port)


def _parse_uid_list(search_data: list[bytes]) -> list[bytes]:
    if not search_data:
        return []
    first = search_data[0]
    if not isinstance(first, bytes):
        return []
    return [token for token in first.split() if token]


def _extract_message_bytes(fetch_data: Sequence[object]) -> bytes | None:
    for part in fetch_data:
        if not isinstance(part, tuple) or len(part) < 2:
            continue
        payload = part[1]
        if isinstance(payload, bytes):
            return payload
    return None


def _extract_body_text(parsed: EmailMessage) -> str:
    if parsed.is_multipart():
        for part in parsed.walk():
            disposition = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disposition:
                continue
            if part.get_content_type() != "text/plain":
                continue
            body = part.get_content()
            if isinstance(body, str) and body.strip():
                return body.strip()
    else:
        body = parsed.get_content()
        if isinstance(body, str) and body.strip():
            return body.strip()
    return "(empty body)"


def _parse_received_at(date_header: str | None, fallback: datetime) -> datetime:
    if not date_header:
        return _ensure_utc(fallback)
    try:
        parsed = parsedate_to_datetime(date_header)
    except (TypeError, ValueError):
        return _ensure_utc(fallback)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("fallback datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


__all__ = ["ImapEmailConnector"]
