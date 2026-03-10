"""Integrated applier that performs writes and dispatches outbound messages."""

from __future__ import annotations

import json
import logging
import smtplib
import subprocess
import urllib.error
import urllib.request
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Callable, Protocol
from urllib.parse import urlparse

from app.models import ActionProposal, ApplyResult, OutboundMessage, PolicyDecision, TaskEnvelope

LOGGER = logging.getLogger(__name__)


class EmailSender(Protocol):
    """Dispatch outbound email messages."""

    def send(self, target: str, body: str, *, subject: str | None = None) -> None:
        """Send one outbound email message."""


class TelegramSender(Protocol):
    """Dispatch outbound Telegram messages."""

    def send(self, target: str, body: str) -> None:
        """Send one outbound Telegram message."""


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

    def __post_init__(self) -> None:
        if not self.bot_token:
            raise ValueError("bot_token is required")
        if not self.api_base_url:
            raise ValueError("api_base_url is required")
        if self.parse_mode is not None and self.parse_mode not in {"Markdown", "MarkdownV2", "HTML"}:
            raise ValueError("parse_mode must be one of Markdown, MarkdownV2, HTML, or None")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

    def send(self, target: str, body: str) -> None:
        if not target:
            raise ValueError("target is required")
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

        raise RuntimeError("telegram_send_failed")


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
                    self.telegram_sender.send(dispatch_target, message.body)
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
    except urllib.error.URLError as error:
        raise RuntimeError("telegram_http_error") from error
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as error:
        raise RuntimeError("telegram_invalid_json") from error
    if not isinstance(parsed, dict):
        raise RuntimeError("telegram_invalid_response_shape")
    return parsed


def _is_telegram_parse_mode_error(response: dict[str, object]) -> bool:
    description = response.get("description")
    if not isinstance(description, str):
        return False
    normalized = description.lower()
    return "parse entities" in normalized


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
        if len(parts) < 4 or parts[2] != "issues":
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
