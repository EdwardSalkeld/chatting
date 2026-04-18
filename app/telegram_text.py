"""Shared Telegram/outbound text normalization helpers."""

from __future__ import annotations

import re


def normalize_telegram_outbound_text(text: str) -> str:
    """Collapse literal escape sequences into displayable text before send-time escaping."""
    normalized = text.replace("\r\n", "\n")
    for _ in range(3):
        previous = normalized
        normalized = re.sub(r"(?:\\)+r(?:\\)+n", "\n", normalized)
        normalized = re.sub(r"(?:\\)+n", "\n", normalized)
        normalized = re.sub(r"(?:\\)+r", "\n", normalized)
        normalized = normalized.replace("\\t", "\t")
        normalized = re.sub(r"(?:\\)+([\\_*`\[\]\(\)~>#\+\-=|{}.!])", r"\1", normalized)
        if normalized == previous:
            break
    return normalized


__all__ = ["normalize_telegram_outbound_text"]
