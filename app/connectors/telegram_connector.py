"""Telegram Bot API long-polling connector."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from app.models import ReplyChannel, TaskEnvelope


@dataclass(frozen=True)
class TelegramGetUpdatesResponse:
    """Parsed Telegram getUpdates response payload."""

    ok: bool
    result: list[dict[str, object]]


class TelegramConnector:
    """Poll Telegram updates and normalize supported messages into envelopes."""

    source = "im"

    def __init__(
        self,
        *,
        bot_token: str,
        api_base_url: str = "https://api.telegram.org",
        poll_timeout_seconds: int = 20,
        allowed_chat_ids: list[str] | None = None,
        allowed_channel_ids: list[str] | None = None,
        context_refs: list[str] | None = None,
        policy_profile: str = "default",
        request_timeout_seconds: float = 30.0,
        http_get_json: Callable[[str, float], TelegramGetUpdatesResponse] | None = None,
    ) -> None:
        if not bot_token:
            raise ValueError("bot_token is required")
        if not api_base_url:
            raise ValueError("api_base_url is required")
        if poll_timeout_seconds <= 0:
            raise ValueError("poll_timeout_seconds must be positive")
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if allowed_chat_ids is not None:
            if not isinstance(allowed_chat_ids, list):
                raise ValueError("allowed_chat_ids must be a list of non-empty strings")
            if not all(isinstance(chat_id, str) and chat_id.strip() for chat_id in allowed_chat_ids):
                raise ValueError("allowed_chat_ids must be a list of non-empty strings")
        if allowed_channel_ids is not None:
            if not isinstance(allowed_channel_ids, list):
                raise ValueError("allowed_channel_ids must be a list of non-empty strings")
            if not all(
                isinstance(channel_id, str) and channel_id.strip()
                for channel_id in allowed_channel_ids
            ):
                raise ValueError("allowed_channel_ids must be a list of non-empty strings")

        self._bot_token = bot_token
        self._api_base_url = api_base_url.rstrip("/")
        self._poll_timeout_seconds = poll_timeout_seconds
        self._allowed_chat_ids = set(allowed_chat_ids or [])
        self._allowed_channel_ids = set(allowed_channel_ids or [])
        self._context_refs = list(context_refs or [])
        self._policy_profile = policy_profile
        self._request_timeout_seconds = request_timeout_seconds
        self._http_get_json = http_get_json or _default_http_get_json
        self._next_offset: int | None = None

    def poll(self) -> list[TaskEnvelope]:
        url = self._build_get_updates_url()
        response = self._http_get_json(url, self._request_timeout_seconds)
        if not response.ok:
            raise RuntimeError("telegram_get_updates_failed")

        envelopes: list[TaskEnvelope] = []
        highest_update_id: int | None = None
        for update in response.result:
            if not isinstance(update, dict):
                continue
            update_id = update.get("update_id")
            if not isinstance(update_id, int):
                continue
            if highest_update_id is None or update_id > highest_update_id:
                highest_update_id = update_id
            envelope = self._normalize_update(update_id, update)
            if envelope is not None:
                envelopes.append(envelope)

        if highest_update_id is not None:
            self._next_offset = highest_update_id + 1
        return envelopes

    def _build_get_updates_url(self) -> str:
        query: dict[str, str] = {"timeout": str(self._poll_timeout_seconds)}
        if self._next_offset is not None:
            query["offset"] = str(self._next_offset)
        encoded_query = urllib.parse.urlencode(query)
        return f"{self._api_base_url}/bot{self._bot_token}/getUpdates?{encoded_query}"

    def _normalize_update(
        self,
        update_id: int,
        update: dict[str, object],
    ) -> TaskEnvelope | None:
        message_payload = update.get("message")
        if isinstance(message_payload, dict):
            return self._normalize_message(update_id=update_id, payload=message_payload)

        channel_post_payload = update.get("channel_post")
        if isinstance(channel_post_payload, dict):
            return self._normalize_channel_post(update_id=update_id, payload=channel_post_payload)
        return None

    def _normalize_message(self, *, update_id: int, payload: dict[str, object]) -> TaskEnvelope | None:
        message_id = payload.get("message_id")
        chat = payload.get("chat")
        if not isinstance(message_id, int) or not isinstance(chat, dict):
            return None
        chat_id_value = _extract_chat_id(chat)
        if chat_id_value is None:
            return None
        if self._allowed_chat_ids and chat_id_value not in self._allowed_chat_ids:
            return None
        return self._build_envelope(
            update_id=update_id,
            payload=payload,
            chat_id_value=chat_id_value,
            actor=_extract_actor(payload.get("from")),
        )

    def _normalize_channel_post(
        self,
        *,
        update_id: int,
        payload: dict[str, object],
    ) -> TaskEnvelope | None:
        message_id = payload.get("message_id")
        chat = payload.get("chat")
        if not isinstance(message_id, int) or not isinstance(chat, dict):
            return None
        chat_id_value = _extract_chat_id(chat)
        if chat_id_value is None:
            return None
        if chat.get("type") != "channel":
            return None
        if not self._allowed_channel_ids or chat_id_value not in self._allowed_channel_ids:
            return None
        return self._build_envelope(
            update_id=update_id,
            payload=payload,
            chat_id_value=chat_id_value,
            actor=_extract_sender_chat_actor(payload.get("sender_chat")),
        )

    def _build_envelope(
        self,
        *,
        update_id: int,
        payload: dict[str, object],
        chat_id_value: str,
        actor: str | None,
    ) -> TaskEnvelope | None:
        text = payload.get("text")
        if not isinstance(text, str) or not text.strip():
            return None

        event_id = f"telegram:{update_id}"
        received_at = _parse_message_timestamp(payload.get("date"))
        thread_id = payload.get("message_thread_id")
        content = text.strip()
        if isinstance(thread_id, int):
            content = f"[thread_id={thread_id}] {content}"

        return TaskEnvelope(
            id=event_id,
            source="im",
            received_at=received_at,
            actor=actor,
            content=content,
            attachments=[],
            context_refs=self._context_refs,
            policy_profile=self._policy_profile,
            reply_channel=ReplyChannel(type="telegram", target=chat_id_value),
            dedupe_key=event_id,
        )


def _extract_chat_id(chat: dict[str, object]) -> str | None:
    chat_id = chat.get("id")
    if not isinstance(chat_id, int):
        return None
    return str(chat_id)


def _extract_actor(raw_sender: object) -> str | None:
    if not isinstance(raw_sender, dict):
        return None
    sender_id = raw_sender.get("id")
    username = raw_sender.get("username")
    if isinstance(username, str) and username.strip():
        if isinstance(sender_id, int):
            return f"{sender_id}:{username}"
        return username
    if isinstance(sender_id, int):
        return str(sender_id)
    return None


def _extract_sender_chat_actor(raw_sender_chat: object) -> str | None:
    if not isinstance(raw_sender_chat, dict):
        return None
    sender_id = raw_sender_chat.get("id")
    username = raw_sender_chat.get("username")
    title = raw_sender_chat.get("title")
    if isinstance(username, str) and username.strip():
        if isinstance(sender_id, int):
            return f"{sender_id}:{username}"
        return username
    if isinstance(title, str) and title.strip():
        if isinstance(sender_id, int):
            return f"{sender_id}:{title}"
        return title
    if isinstance(sender_id, int):
        return str(sender_id)
    return None


def _parse_message_timestamp(value: object) -> datetime:
    if isinstance(value, int):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return datetime.now(timezone.utc)


def _default_http_get_json(url: str, timeout_seconds: float) -> TelegramGetUpdatesResponse:
    request = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.URLError as error:
        raise RuntimeError("telegram_http_error") from error
    except json.JSONDecodeError as error:
        raise RuntimeError("telegram_invalid_json") from error

    if not isinstance(payload, dict):
        raise RuntimeError("telegram_invalid_response_shape")
    ok = payload.get("ok")
    result = payload.get("result")
    if not isinstance(ok, bool) or not isinstance(result, list):
        raise RuntimeError("telegram_invalid_response_shape")
    return TelegramGetUpdatesResponse(ok=ok, result=result)


__all__ = ["TelegramConnector", "TelegramGetUpdatesResponse"]
