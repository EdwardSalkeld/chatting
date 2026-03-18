"""Integrated applier that performs writes and dispatches outbound messages."""

from __future__ import annotations

import json
import logging
import mimetypes
import smtplib
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Callable, Protocol
from urllib.parse import urlparse

from app.models import ActionProposal, ApplyResult, OutboundMessage, PolicyDecision, TaskEnvelope

LOGGER = logging.getLogger(__name__)
_MAX_HTTP_ERROR_LOG_BODY_CHARS = 500


class EmailSender(Protocol):
    """Dispatch outbound email messages."""

    def send(self, target: str, body: str, *, subject: str | None = None) -> None:
        """Send one outbound email message."""


class TelegramSender(Protocol):
    """Dispatch outbound Telegram messages."""

    def send(self, target: str, message: OutboundMessage) -> None:
        """Send one outbound Telegram message."""

    def react(self, target: str, message_id: int, emoji: str) -> None:
        """React to one existing Telegram message."""


class GitHubSender(Protocol):
    """Dispatch outbound GitHub issue comments."""

    def send(self, target: str, body: str) -> None:
        """Send one outbound GitHub issue comment."""


@dataclass(frozen=True)
class MessageDispatchError(RuntimeError):
    """Raised when message dispatch fails after one or more successful sends."""

    reason_code: str
    dispatched_messages: list[OutboundMessage]

    def __str__(self) -> str:
        return self.reason_code


@dataclass(frozen=True)
class SmtpEmailSender:
    """SMTP email sender used by the integrated applier."""

    host: str
    port: int
    from_address: str
    username: str | None = None
    password: str | None = None
    subject: str = "Automation response"
    use_ssl: bool = True
    starttls: bool = False
    timeout_seconds: float = 10.0
    smtp_client_factory: Callable[[str, int], object] | None = None

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host is required")
        if self.port <= 0:
            raise ValueError("port must be positive")
        if not self.from_address:
            raise ValueError("from_address is required")

    def send(self, target: str, body: str, *, subject: str | None = None) -> None:
        if not target:
            raise ValueError("target is required")
        if not body.strip():
            raise ValueError("body is required")

        message = EmailMessage()
        message["From"] = self.from_address
        message["To"] = target
        message["Subject"] = (subject or self.subject).strip() or self.subject
        message.set_content(body)

        client_factory = self.smtp_client_factory or self._default_smtp_client_factory()
        client = client_factory(self.host, self.port)
        try:
            if self.starttls and hasattr(client, "starttls"):
                client.starttls()
            if self.username and self.password and hasattr(client, "login"):
                client.login(self.username, self.password)
            client.send_message(message)
        finally:
            if hasattr(client, "quit"):
                client.quit()

    def _default_smtp_client_factory(self) -> Callable[[str, int], object]:
        if self.use_ssl:
            return lambda host, port: smtplib.SMTP_SSL(
                host,
                port,
                timeout=self.timeout_seconds,
            )
        return lambda host, port: smtplib.SMTP(
            host,
            port,
            timeout=self.timeout_seconds,
            )


@dataclass(frozen=True)
class TelegramMessageSender:
    """Telegram Bot API sender used by the integrated applier."""

    bot_token: str
    api_base_url: str = "https://api.telegram.org"
    parse_mode: str | None = "Markdown"
    timeout_seconds: float = 10.0
    http_post_json: Callable[[str, dict[str, object], float], dict[str, object]] | None = None
    http_post_multipart: (
        Callable[[str, dict[str, object], str, Path, float], dict[str, object]] | None
    ) = None

    def __post_init__(self) -> None:
        if not self.bot_token:
            raise ValueError("bot_token is required")
        if not self.api_base_url:
            raise ValueError("api_base_url is required")
        if self.parse_mode is not None and self.parse_mode not in {"Markdown", "MarkdownV2", "HTML"}:
            raise ValueError("parse_mode must be one of Markdown, MarkdownV2, HTML, or None")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

    def send(self, target: str, message: OutboundMessage) -> None:
        if not target:
            raise ValueError("target is required")
        if message.attachment is None:
            if message.body is None or not message.body.strip():
                raise ValueError("body is required")
            self._send_text_message(target, message.body)
            return

        file_path = _resolve_telegram_attachment_path(message.attachment.uri)
        api_method = _telegram_api_method_for_attachment(file_path)
        field_name = "photo" if api_method == "sendPhoto" else "document"
        url = f"{self.api_base_url.rstrip('/')}/bot{self.bot_token}/{api_method}"
        payload: dict[str, object] = {"chat_id": target}
        if message.body is not None:
            payload["caption"] = message.body
        if self.parse_mode is not None and message.body is not None:
            payload["parse_mode"] = self.parse_mode

        client = self.http_post_multipart or _default_http_post_multipart
        response = client(url, payload, field_name, file_path, self.timeout_seconds)
        if response.get("ok") is True:
            return

        if self.parse_mode is not None and message.body is not None and _is_telegram_parse_mode_error(response):
            fallback_payload = dict(payload)
            fallback_payload.pop("parse_mode", None)
            fallback_response = client(url, fallback_payload, field_name, file_path, self.timeout_seconds)
            if fallback_response.get("ok") is True:
                return

        raise RuntimeError("telegram_attachment_send_failed")

    def _send_text_message(self, target: str, body: str) -> None:
        if not body.strip():
            raise ValueError("body is required")

        client = self.http_post_json or _default_http_post_json
        url = f"{self.api_base_url.rstrip('/')}/bot{self.bot_token}/sendMessage"
        payload: dict[str, object] = {"chat_id": target, "text": body}
        if self.parse_mode is not None:
            payload["parse_mode"] = self.parse_mode
        response = client(url, payload, self.timeout_seconds)
        if response.get("ok") is True:
            return

        # Gracefully degrade to plain text when Telegram rejects formatting entities.
        if self.parse_mode is not None and _is_telegram_parse_mode_error(response):
            plain_text_payload = {"chat_id": target, "text": body}
            fallback_response = client(url, plain_text_payload, self.timeout_seconds)
            if fallback_response.get("ok") is True:
                return

        raise RuntimeError(_describe_telegram_response_error("telegram_send_failed", response))

    def react(self, target: str, message_id: int, emoji: str) -> None:
        if not target:
            raise ValueError("target is required")
        if message_id <= 0:
            raise ValueError("message_id must be positive")
        if not emoji.strip():
            raise ValueError("emoji is required")

        client = self.http_post_json or _default_http_post_json
        url = f"{self.api_base_url.rstrip('/')}/bot{self.bot_token}/setMessageReaction"
        payload: dict[str, object] = {
            "chat_id": target,
            "message_id": message_id,
            "reaction": [{"type": "emoji", "emoji": emoji.strip()}],
        }
        response = client(url, payload, self.timeout_seconds)
        if response.get("ok") is True:
            return
        raise RuntimeError(_describe_telegram_response_error("telegram_reaction_failed", response))


@dataclass(frozen=True)
class GitHubIssueCommentSender:
    """GitHub CLI sender used by the integrated applier."""

    command_runner: Callable[[list[str]], object] | None = None

    def send(self, target: str, body: str) -> None:
        if not target:
            raise ValueError("target is required")
        if not body.strip():
            raise ValueError("body is required")
        repository, issue_number = _parse_github_issue_target(target)
        command = [
            "gh",
            "issue",
            "comment",
            str(issue_number),
            "--repo",
            repository,
            "--body",
            body,
        ]
        runner = self.command_runner or _default_gh_command_runner
        result = runner(command)
        returncode = getattr(result, "returncode", 0)
        if returncode != 0:
            raise RuntimeError("github_issue_comment_failed")


@dataclass(frozen=True)
class IntegratedApplier:
    """Apply approved actions and dispatch approved outbound messages."""

    base_dir: str
    email_sender: EmailSender | None = None
    telegram_sender: TelegramSender | None = None
    github_sender: GitHubSender | None = None

    def apply(self, decision: PolicyDecision, envelope: TaskEnvelope | None = None) -> ApplyResult:
        applied_actions: list[ActionProposal] = []
        skipped_actions: list[ActionProposal] = []
        dispatched_messages: list[OutboundMessage] = []
        reason_codes: list[str] = []

        for action in decision.approved_actions:
            if action.type != "write_file":
                skipped_actions.append(action)
                reason_codes.append("unsupported_action_type")
                continue

            if action.path is None or action.content is None:
                skipped_actions.append(action)
                reason_codes.append("write_file_payload_invalid")
                continue

            try:
                destination = _resolve_relative_path(self.base_dir, action.path)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(action.content, encoding="utf-8")
                applied_actions.append(action)
            except ValueError as error:
                skipped_actions.append(action)
                reason_codes.append(str(error))

        for message in decision.approved_messages:
            dispatch_channel, dispatch_target = _resolve_dispatch_channel_and_target(
                message=message,
                envelope=envelope,
            )
            normalized_message = OutboundMessage(
                channel=dispatch_channel,
                target=dispatch_target,
                body=message.body,
                metadata=dict(message.metadata),
                attachment=message.attachment,
            )

            if dispatch_channel == "log":
                LOGGER.info("log_dispatch target=%s body=%s", dispatch_target, message.body)
                dispatched_messages.append(normalized_message)
                continue
            if dispatch_channel == "drop":
                LOGGER.info("drop_marker target=%s body=%s", dispatch_target, message.body)
                dispatched_messages.append(normalized_message)
                continue
            if dispatch_channel == "email":
                if self.email_sender is None:
                    LOGGER.warning(
                        "drop_dispatch reason=email_dispatch_not_configured channel=%s target=%s",
                        dispatch_channel,
                        dispatch_target,
                    )
                    reason_codes.append("email_dispatch_not_configured")
                    continue
                try:
                    reply_subject, reply_body = _format_email_reply(message, envelope)
                    self.email_sender.send(
                        dispatch_target,
                        reply_body,
                        subject=reply_subject,
                    )
                    dispatched_messages.append(normalized_message)
                except Exception:  # noqa: BLE001
                    LOGGER.exception(
                        "drop_dispatch reason=email_dispatch_failed channel=%s target=%s",
                        dispatch_channel,
                        dispatch_target,
                    )
                    raise MessageDispatchError(
                        reason_code="email_dispatch_failed",
                        dispatched_messages=list(dispatched_messages),
                    ) from None
                continue
            if dispatch_channel == "telegram":
                if self.telegram_sender is None:
                    LOGGER.warning(
                        "drop_dispatch reason=telegram_dispatch_not_configured channel=%s target=%s",
                        dispatch_channel,
                        dispatch_target,
                    )
                    reason_codes.append("telegram_dispatch_not_configured")
                    continue
                try:
                    self.telegram_sender.send(dispatch_target, message)
                    dispatched_messages.append(normalized_message)
                except Exception as error:  # noqa: BLE001
                    error_reason = _telegram_dispatch_reason_code(message, exception=error)
                    LOGGER.exception(
                        "drop_dispatch reason=%s channel=%s target=%s",
                        error_reason,
                        dispatch_channel,
                        dispatch_target,
                    )
                    raise MessageDispatchError(
                        reason_code=error_reason,
                        dispatched_messages=list(dispatched_messages),
                    ) from None
                continue
            if dispatch_channel == "github":
                if self.github_sender is None:
                    LOGGER.warning(
                        "drop_dispatch reason=github_dispatch_not_configured channel=%s target=%s",
                        dispatch_channel,
                        dispatch_target,
                    )
                    reason_codes.append("github_dispatch_not_configured")
                    continue
                try:
                    self.github_sender.send(dispatch_target, message.body)
                    dispatched_messages.append(normalized_message)
                except Exception:  # noqa: BLE001
                    LOGGER.exception(
                        "drop_dispatch reason=github_dispatch_failed channel=%s target=%s",
                        dispatch_channel,
                        dispatch_target,
                    )
                    raise MessageDispatchError(
                        reason_code="github_dispatch_failed",
                        dispatched_messages=list(dispatched_messages),
                    ) from None
                continue
            if dispatch_channel == "telegram_reaction":
                if self.telegram_sender is None:
                    LOGGER.warning(
                        "drop_dispatch reason=telegram_dispatch_not_configured channel=%s target=%s",
                        dispatch_channel,
                        dispatch_target,
                    )
                    reason_codes.append("telegram_dispatch_not_configured")
                    continue
                try:
                    self.telegram_sender.react(
                        dispatch_target,
                        _resolve_telegram_reaction_message_id(message=message, envelope=envelope),
                        message.body,
                    )
                    dispatched_messages.append(normalized_message)
                except Exception:  # noqa: BLE001
                    LOGGER.exception(
                        "drop_dispatch reason=telegram_dispatch_failed channel=%s target=%s",
                        dispatch_channel,
                        dispatch_target,
                    )
                    raise MessageDispatchError(
                        reason_code="telegram_dispatch_failed",
                        dispatched_messages=list(dispatched_messages),
                    ) from None
                continue
            LOGGER.warning(
                "drop_dispatch reason=unsupported_message_channel channel=%s target=%s",
                dispatch_channel,
                dispatch_target,
            )
            reason_codes.append("unsupported_message_channel")

        if decision.blocked_actions:
            reason_codes.append("policy_blocked_actions_present")

        return ApplyResult(
            applied_actions=applied_actions,
            skipped_actions=skipped_actions,
            dispatched_messages=dispatched_messages,
            reason_codes=_dedupe_preserving_order(reason_codes),
        )


def _resolve_relative_path(base_dir: str, relative_path: str) -> Path:
    base_path = Path(base_dir).resolve()
    destination = (base_path / relative_path).resolve()
    try:
        destination.relative_to(base_path)
    except ValueError as error:
        raise ValueError("write_file_outside_base_dir") from error
    return destination


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _format_email_reply(
    message: OutboundMessage,
    envelope: TaskEnvelope | None,
) -> tuple[str | None, str]:
    if message.body is None:
        raise ValueError("email body is required")
    cleaned_body = _strip_leading_subject_line(message.body)

    if envelope is None or envelope.source != "email":
        return None, cleaned_body

    original_subject, original_body = _parse_email_envelope_content(envelope.content)
    reply_subject = _to_reply_subject(original_subject)
    if not original_body:
        return reply_subject, cleaned_body

    quoted_original = "\n".join(f"> {line}" for line in original_body.splitlines())
    if not quoted_original:
        return reply_subject, cleaned_body

    reply_body = (
        f"{cleaned_body.rstrip()}\n\n"
        "Original message:\n"
        f"{quoted_original}\n"
    )
    return reply_subject, reply_body


def _parse_email_envelope_content(content: str) -> tuple[str, str]:
    if not content.startswith("Subject: "):
        return "(no subject)", content.strip()

    subject, _, body = content.partition("\n\n")
    subject_value = subject.removeprefix("Subject: ").strip() or "(no subject)"
    return subject_value, body.strip()


def _to_reply_subject(subject: str) -> str:
    if subject.lower().startswith("re:"):
        return subject
    return f"Re: {subject}"


def _strip_leading_subject_line(body: str) -> str:
    stripped = body.lstrip()
    if not stripped.lower().startswith("subject:"):
        return body

    first_line, _, remainder = stripped.partition("\n")
    subject_candidate = first_line.removeprefix("Subject:").strip()
    if not subject_candidate:
        return body

    return remainder.lstrip() or body


def _resolve_dispatch_channel_and_target(
    *,
    message: OutboundMessage,
    envelope: TaskEnvelope | None,
) -> tuple[str, str]:
    if message.channel != "final":
        return message.channel, message.target
    if envelope is None:
        return message.channel, message.target
    return envelope.reply_channel.type, envelope.reply_channel.target


def _resolve_telegram_reaction_message_id(
    *,
    message: OutboundMessage,
    envelope: TaskEnvelope | None,
) -> int:
    message_id = message.metadata.get("message_id")
    if isinstance(message_id, int) and message_id > 0:
        return message_id
    if envelope is not None:
        reply_message_id = envelope.reply_channel.metadata.get("message_id")
        if isinstance(reply_message_id, int) and reply_message_id > 0:
            return reply_message_id
    raise ValueError("telegram reaction message_id is required")


def _default_http_post_json(
    url: str,
    payload: dict[str, object],
    timeout_seconds: float,
) -> dict[str, object]:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        error_body = _read_http_error_body(error)
        LOGGER.error(
            "telegram_http_error status=%s reason=%s response_body=%s",
            error.code,
            error.reason,
            _truncate_http_error_body_for_log(error_body),
        )
        if error_body:
            try:
                parsed_error = json.loads(error_body)
            except json.JSONDecodeError:
                parsed_error = None
            if isinstance(parsed_error, dict):
                # Preserve Telegram API JSON error payload so caller can inspect
                # parse-mode failures and retry without formatting.
                return parsed_error
        raise RuntimeError(
            _describe_telegram_http_error(
                status_code=error.code,
                reason=error.reason,
                response_body=error_body,
            )
        ) from error
    except urllib.error.URLError as error:
        LOGGER.error("telegram_http_error reason=%s", getattr(error, "reason", error))
        raise RuntimeError(f"telegram_http_error reason={getattr(error, 'reason', error)}") from error
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as error:
        raise RuntimeError("telegram_invalid_json") from error
    if not isinstance(parsed, dict):
        raise RuntimeError("telegram_invalid_response_shape")
    return parsed


def _default_http_post_multipart(
    url: str,
    payload: dict[str, object],
    file_field_name: str,
    file_path: Path,
    timeout_seconds: float,
) -> dict[str, object]:
    boundary = f"----chatting-{uuid.uuid4().hex}"
    encoded_payload = _encode_multipart_payload(
        payload=payload,
        file_field_name=file_field_name,
        file_path=file_path,
        boundary=boundary,
    )
    request = urllib.request.Request(
        url=url,
        data=encoded_payload,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        error_body = _read_http_error_body(error)
        if error_body:
            try:
                parsed_error = json.loads(error_body)
            except json.JSONDecodeError:
                parsed_error = None
            if isinstance(parsed_error, dict):
                return parsed_error
        raise RuntimeError(
            _describe_telegram_http_error(
                status_code=error.code,
                reason=error.reason,
                response_body=error_body,
            )
        ) from error
    except urllib.error.URLError as error:
        raise RuntimeError("telegram_http_error") from error
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as error:
        raise RuntimeError("telegram_invalid_json") from error
    if not isinstance(parsed, dict):
        raise RuntimeError("telegram_invalid_response_shape")
    return parsed


def _encode_multipart_payload(
    *,
    payload: dict[str, object],
    file_field_name: str,
    file_path: Path,
    boundary: str,
) -> bytes:
    lines: list[bytes] = []
    for key, value in payload.items():
        lines.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                f"{value}\r\n".encode("utf-8"),
            ]
        )
    mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    lines.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="{file_field_name}"; '
                f'filename="{file_path.name}"\r\n'
            ).encode("utf-8"),
            f"Content-Type: {mime_type}\r\n\r\n".encode("utf-8"),
            file_path.read_bytes(),
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    return b"".join(lines)


def _resolve_telegram_attachment_path(uri: str) -> Path:
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme and parsed.scheme != "file":
        raise RuntimeError("telegram_attachment_unsupported_uri")
    if parsed.scheme == "file":
        path = urllib.request.url2pathname(parsed.path)
        if parsed.netloc:
            path = f"//{parsed.netloc}{path}"
    else:
        path = uri
    attachment_path = Path(path)
    if not attachment_path.is_absolute():
        raise RuntimeError("telegram_attachment_path_not_absolute")
    if not attachment_path.is_file():
        raise RuntimeError("telegram_attachment_missing")
    return attachment_path


def _telegram_api_method_for_attachment(file_path: Path) -> str:
    mime_type, _ = mimetypes.guess_type(file_path.name)
    if mime_type is not None and mime_type.startswith("image/"):
        return "sendPhoto"
    return "sendDocument"


def _telegram_dispatch_reason_code(message: OutboundMessage, exception: Exception | None) -> str:
    if exception is not None:
        reason = str(exception).strip()
        if reason.startswith("telegram_"):
            return reason
    if message.attachment is not None:
        return "telegram_attachment_dispatch_failed"
    return "telegram_dispatch_failed"


def _is_telegram_parse_mode_error(response: dict[str, object]) -> bool:
    description = response.get("description")
    if not isinstance(description, str):
        return False
    normalized = description.lower()
    return "parse entities" in normalized


def _read_http_error_body(error: urllib.error.HTTPError) -> str | None:
    if error.fp is None:
        return None
    try:
        raw_error_body = error.read()
    except Exception:  # noqa: BLE001
        return None
    if not raw_error_body:
        return None
    return raw_error_body.decode("utf-8", errors="replace")


def _truncate_http_error_body_for_log(body: str | None) -> str:
    if body is None:
        return "<empty>"
    if len(body) <= _MAX_HTTP_ERROR_LOG_BODY_CHARS:
        return body
    return f"{body[:_MAX_HTTP_ERROR_LOG_BODY_CHARS]}...(truncated)"


def _describe_telegram_http_error(
    *,
    status_code: int | None,
    reason: str | None,
    response_body: str | None,
) -> str:
    detail_parts = ["telegram_http_error"]
    if status_code is not None:
        detail_parts.append(f"status={status_code}")
    if reason:
        detail_parts.append(f"reason={reason}")
    detail_parts.append(f"response_body={_truncate_http_error_body_for_log(response_body)}")
    return " ".join(detail_parts)


def _describe_telegram_response_error(prefix: str, response: dict[str, object]) -> str:
    description = response.get("description")
    if isinstance(description, str) and description.strip():
        return f"{prefix} description={description}"
    return prefix


def _default_gh_command_runner(command: list[str]) -> object:
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _parse_github_issue_target(target: str) -> tuple[str, int]:
    if not target.strip():
        raise ValueError("github_issue_target_invalid")

    stripped = target.strip()
    if stripped.startswith("http://") or stripped.startswith("https://"):
        parsed = urlparse(stripped)
        if parsed.netloc.casefold() != "github.com":
            raise ValueError("github_issue_target_invalid")
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) < 4 or parts[2] not in {"issues", "pull"}:
            raise ValueError("github_issue_target_invalid")
        repository = f"{parts[0]}/{parts[1]}"
        number_text = parts[3]
    else:
        if "#" not in stripped:
            raise ValueError("github_issue_target_invalid")
        repository, _, number_text = stripped.partition("#")
        repository = repository.strip()
        number_text = number_text.strip()
        if not repository or "/" not in repository:
            raise ValueError("github_issue_target_invalid")

    if not number_text.isdigit():
        raise ValueError("github_issue_target_invalid")
    issue_number = int(number_text)
    if issue_number <= 0:
        raise ValueError("github_issue_target_invalid")
    return repository, issue_number


__all__ = [
    "EmailSender",
    "GitHubIssueCommentSender",
    "GitHubSender",
    "IntegratedApplier",
    "MessageDispatchError",
    "SmtpEmailSender",
    "TelegramMessageSender",
    "TelegramSender",
]
